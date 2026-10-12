"""Store tests using inline fixtures and an explicit clock (no wall clock reads)."""

from __future__ import annotations

import json
import sqlite3
import stat
import tempfile
import threading
import unittest
from datetime import datetime, timezone
from pathlib import Path

from pullshit.contracts import DAY, ChatMessage, ContentBundle, MediaRef, MessageRef
from pullshit.storage.store import Store, StoreError

LIMITS = {'min_interval_seconds': 0, 'daily_batches': 100, 'daily_nodes': 1000,
          'daily_publish_bytes': 10 ** 9}


class Clock:
    def __init__(self, start: float = 1000.0):
        self._now = start

    def __call__(self) -> float:
        return self._now

    def advance(self, seconds: float) -> None:
        self._now += seconds


def utc_day(timestamp: float) -> str:
    return datetime.fromtimestamp(timestamp, tz=timezone.utc).strftime('%Y-%m-%d')


def message(account='100', group='200', message_id='1', occurred_at=1000.0, segments=None, **kwargs):
    return ChatMessage(
        ref=MessageRef(account, group, message_id),
        user_id=kwargs.get('user_id', '300'),
        nickname=kwargs.get('nickname', 'alice'),
        occurred_at=occurred_at,
        segments=[{'type': 'text', 'text': 'hello'}] if segments is None else segments,
        own=kwargs.get('own', False),
        recalled=kwargs.get('recalled', False),
        expires_at=kwargs.get('expires_at', 0.0),
    )


def bundle(bundle_id='b1', fingerprint='f1', messages=None, created_at=1000.0, expires_at=100000.0, media=None):
    return ContentBundle(bundle_id=bundle_id, fingerprint=fingerprint,
                         messages=messages if messages is not None else [message()],
                         created_at=created_at, expires_at=expires_at, media=media or [])


def delivery(delivery_id='d1', account='100', group='200', bundle_id='b1', fingerprint='f1',
             part_index=0, part_total=1, nodes=None, estimated_bytes=100, node_count=2,
             expires_at=100000.0, created_at=1000.0, tags=None):
    return {'delivery_id': delivery_id, 'account_id': account, 'group_id': group, 'bundle_id': bundle_id,
            'fingerprint': fingerprint, 'part_index': part_index, 'part_total': part_total,
            'nodes': nodes or [{'type': 'node', 'data': {'id': 'x'}}], 'estimated_bytes': estimated_bytes,
            'node_count': node_count, 'expires_at': expires_at, 'created_at': created_at, 'tags': tags or ['a']}


def feedback_signal(signal_id='s1', account='100', group='200', occurred_at=1000.0, expires_at=10000.0, **kwargs):
    return {'signal_id': signal_id, 'account_id': account, 'group_id': group,
            'bundle_id': kwargs.get('bundle_id', 'b1'), 'publication_id': kwargs.get('publication_id', 'p1'),
            'user_id': kwargs.get('user_id', '300'), 'kind': kwargs.get('kind', 'emoji'),
            'direction': kwargs.get('direction', 'positive'), 'tags': kwargs.get('tags', ['t']),
            'occurred_at': occurred_at, 'expires_at': expires_at,
            'payload': kwargs.get('payload', {'e': 1}), 'active': kwargs.get('active', True)}


class StoreTestCase(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.path = Path(self.directory.name) / 'test.db'
        self.store = Store(self.path)
        self.addCleanup(self.store.close)

    # ------------------------------------------------------------ schema

    def test_schema_shape(self):
        names = {row['name'] for row in self.store.db.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'")}
        self.assertEqual(names, {'messages', 'candidates', 'deliveries', 'feedback', 'group_state',
                                 'daily_budgets'})
        self.assertEqual(self.store.db.execute('PRAGMA user_version').fetchone()[0], 1)
        self.assertEqual(self.store.db.execute('PRAGMA journal_mode').fetchone()[0], 'wal')
        self.assertEqual(self.store.db.execute('PRAGMA synchronous').fetchone()[0], 2)
        self.assertEqual(self.store.db.execute('PRAGMA foreign_keys').fetchone()[0], 1)
        self.assertEqual(self.store.db.execute('PRAGMA auto_vacuum').fetchone()[0], 2)

    def test_private_file_permissions(self):
        self.assertEqual(stat.S_IMODE(self.path.stat().st_mode), 0o600)

    def test_rejects_old_schema(self):
        path = Path(self.directory.name) / 'old.db'
        connection = sqlite3.connect(path)
        connection.execute('CREATE TABLE raw_events (id TEXT)')
        connection.close()
        with self.assertRaises(StoreError):
            Store(path)

    def test_rejects_other_version(self):
        path = Path(self.directory.name) / 'version.db'
        connection = sqlite3.connect(path)
        connection.execute('PRAGMA user_version = 2')
        connection.close()
        with self.assertRaises(StoreError):
            Store(path)

    def test_rejects_extra_tables(self):
        path = Path(self.directory.name) / 'extra.db'
        store = Store(path)
        store.db.execute('CREATE TABLE logging (id TEXT)')
        store.close()
        with self.assertRaises(StoreError):
            Store(path)

    def test_empty_file_treated_as_new(self):
        path = Path(self.directory.name) / 'empty.db'
        path.touch()
        store = Store(path)
        try:
            self.assertEqual(store.db.execute('PRAGMA user_version').fetchone()[0], 1)
        finally:
            store.close()

    # ------------------------------------------------------------ messages

    def test_put_message_idempotent(self):
        original = message()
        self.assertTrue(self.store.put_message(original))
        self.assertFalse(self.store.put_message(original))
        self.assertEqual(self.store.summary()['messages'], 1)
        self.assertEqual(self.store.get_message(original.ref, now=2000.0), original)

    def test_message_scoped_ids(self):
        a = message(account='100', group='200', message_id='7', segments=[{'type': 'text', 'text': 'A'}])
        b = message(account='100', group='201', message_id='7', segments=[{'type': 'text', 'text': 'B'}])
        c = message(account='101', group='200', message_id='7', segments=[{'type': 'text', 'text': 'C'}])
        for item in (a, b, c):
            self.store.put_message(item)
        self.assertEqual(self.store.summary()['messages'], 3)
        self.assertEqual(self.store.get_message(a.ref, now=2000.0), a)
        self.assertEqual(self.store.get_message(MessageRef('100', '201', '7'), now=2000.0), b)
        self.assertEqual(self.store.get_message(MessageRef('101', '200', '7'), now=2000.0), c)

    def test_get_message_expired(self):
        self.store.put_message(message(expires_at=3000.0))
        self.assertIsNotNone(self.store.get_message(MessageRef('100', '200', '1'), now=2000.0))
        self.assertIsNone(self.store.get_message(MessageRef('100', '200', '1'), now=3000.0))

    def test_expiry_cannot_renew(self):
        self.store.put_message(message(expires_at=2000.0))
        self.assertFalse(self.store.put_message(message(expires_at=5000.0)))
        row = self.store.db.execute('SELECT expires_at FROM messages').fetchone()
        self.assertEqual(row['expires_at'], 2000.0)
        self.assertIsNone(self.store.get_message(MessageRef('100', '200', '1'), now=2500.0))

    def test_recall_before_message(self):
        ref = MessageRef('100', '200', '42')
        self.store.recall_message(ref, now=1000.0)
        tombstone = self.store.get_message(ref, now=1001.0)
        self.assertTrue(tombstone.recalled)
        self.assertEqual(tombstone.segments, [])
        body = ChatMessage(ref=ref, user_id='300', nickname='alice', occurred_at=900.0,
                           segments=[{'type': 'text', 'text': 'late body'}], expires_at=900.0 + 3 * DAY)
        self.assertFalse(self.store.put_message(body))
        stored = self.store.get_message(ref, now=1001.0)
        self.assertTrue(stored.recalled)
        self.assertEqual(stored.segments, body.segments)
        self.assertEqual(stored.expires_at, 900.0 + 3 * DAY)

    def test_recall_existing(self):
        original = message()
        self.store.put_message(original)
        self.store.recall_message(original.ref, now=1500.0)
        stored = self.store.get_message(original.ref, now=1501.0)
        self.assertTrue(stored.recalled)
        self.assertEqual(stored.segments, original.segments)
        self.assertEqual(stored.expires_at, min(original.expires_at, 1500.0 + 3 * DAY))

    def test_nearby_window_and_limit(self):
        anchor = message(message_id='500', occurred_at=1500.0)
        self.store.put_message(anchor)
        for occurred, message_id in ((1300.0, '1'), (1400.0, '2'), (1500.0, '3'), (1600.0, '4'), (1700.0, '5')):
            self.store.put_message(message(message_id=message_id, occurred_at=occurred))
        self.store.put_message(message(account='999', message_id='7', occurred_at=1500.0))
        got = self.store.nearby(anchor.ref, 1500.0, window_seconds=120, limit=10, now=2000.0)
        self.assertEqual([item.ref.message_id for item in got], ['3', '2', '4'])
        got = self.store.nearby(anchor.ref, 1500.0, window_seconds=120, limit=1, now=2000.0)
        self.assertEqual([item.ref.message_id for item in got], ['3'])
        self.store.put_message(message(message_id='6', occurred_at=1490.0, expires_at=1500.0))
        got = self.store.nearby(anchor.ref, 1500.0, window_seconds=120, limit=10, now=2000.0)
        self.assertNotIn('6', [item.ref.message_id for item in got])

    def test_invalid_message_ids(self):
        with self.assertRaises(ValueError):
            self.store.put_message(ChatMessage(ref=MessageRef('abc', '200', '1'), user_id='', nickname='',
                                               occurred_at=1.0, segments=[], expires_at=100.0))
        with self.assertRaises(ValueError):
            self.store.get_message(MessageRef('100', 'group!', '1'), now=0.0)

    # ------------------------------------------------------------ candidates

    def test_candidate_roundtrip(self):
        original = bundle()
        self.assertTrue(self.store.put_candidate(original, analysis={'score': 0.5}))
        got = self.store.get_candidate('b1', now=2000.0)
        self.assertEqual(got['bundle'], original)
        self.assertEqual(got['status'], 'bundled')
        self.assertEqual(got['analysis'], {'score': 0.5})
        self.assertFalse(self.store.put_candidate(original))
        with self.assertRaises(StoreError):
            self.store.put_candidate(bundle(fingerprint='other'))
        row = self.store.db.execute('SELECT expires_at FROM candidates').fetchone()
        self.assertEqual(row['expires_at'], original.expires_at)

    def test_candidate_status(self):
        self.store.put_candidate(bundle())
        self.store.set_candidate_status('b1', 'eligible', analysis={'ok': True})
        got = self.store.get_candidate('b1', now=2000.0)
        self.assertEqual(got['status'], 'eligible')
        self.assertEqual(got['analysis'], {'ok': True})
        self.store.set_candidate_status('b1', 'rejected')
        got = self.store.get_candidate('b1', now=2000.0)
        self.assertEqual(got['status'], 'rejected')
        self.assertEqual(got['analysis'], {'ok': True})
        with self.assertRaises(ValueError):
            self.store.set_candidate_status('b1', 'bogus')

    def test_candidate_expiry_and_media(self):
        live = bundle(bundle_id='live', media=[MediaRef(reference_id='r1',
                                                        source=MessageRef('100', '200', '1'), path='/tmp/a.jpg')])
        self.store.put_candidate(live)
        self.store.put_candidate(bundle(bundle_id='dead', fingerprint='f2', expires_at=5000.0))
        self.assertEqual(self.store.live_media_paths(now=10000.0), {'/tmp/a.jpg'})
        self.assertIsNone(self.store.get_candidate('dead', now=10000.0))
        self.assertEqual([item['bundle'].bundle_id for item in self.store.list_candidates(now=10000.0)], ['live'])
        self.store.set_candidate_status('live', 'cancelled')
        self.assertEqual(self.store.live_media_paths(now=10000.0), set())

    # ------------------------------------------------------------ deliveries

    def test_enqueue_delivery_dedup(self):
        original = delivery()
        self.assertTrue(self.store.enqueue_delivery(original))
        self.assertFalse(self.store.enqueue_delivery(original))
        stored = self.store.get_delivery('d1')
        self.assertEqual(stored['status'], 'queued')
        self.assertEqual(stored['nodes'], [{'type': 'node', 'data': {'id': 'x'}}])
        self.assertEqual(stored['tags'], ['a'])
        self.assertIsNone(stored['message_id'])
        self.assertIsNone(stored['attempted_at'])
        self.assertIsNone(stored['error_code'])
        self.assertFalse(self.store.enqueue_delivery(delivery(delivery_id='d2')))
        self.assertEqual(self.store.get_delivery('d1')['delivery_id'], 'd1')
        with self.assertRaises(StoreError):
            self.store.enqueue_delivery(delivery(fingerprint='other-f'))
        with self.assertRaises(ValueError):
            self.store.enqueue_delivery({key: value for key, value in delivery(delivery_id='d3').items()
                                         if key != 'nodes'})

    def test_delivery_sources(self):
        self.store.put_candidate(bundle(bundle_id='b1', messages=[message(message_id='11'),
                                                                  message(message_id='12')]))
        self.store.enqueue_delivery(delivery())
        row = self.store.db.execute('SELECT sources FROM deliveries').fetchone()
        self.assertEqual(json.loads(row['sources']), [
            {'account_id': '100', 'group_id': '200', 'message_id': '11'},
            {'account_id': '100', 'group_id': '200', 'message_id': '12'}])

    def test_deliveries_listing(self):
        for item in (delivery(delivery_id='a', fingerprint='fa', created_at=1000.0),
                     delivery(delivery_id='b', fingerprint='fb', created_at=2000.0),
                     delivery(delivery_id='c', fingerprint='fc', created_at=1500.0),
                     delivery(delivery_id='x', fingerprint='fx', created_at=3000.0, expires_at=500.0)):
            self.store.enqueue_delivery(item)
        self.assertEqual([item['delivery_id'] for item in self.store.deliveries(now=2500.0)], ['a', 'c', 'b'])
        self.store.db.execute("UPDATE deliveries SET status='sent' WHERE delivery_id='c'")
        self.assertEqual([item['delivery_id'] for item in self.store.deliveries(status='sent', now=2500.0)], ['c'])
        self.assertEqual(self.store.deliveries(group_id='999', now=2500.0), [])

    def test_delivery_for_message(self):
        self.store.enqueue_delivery(delivery())
        self.store.db.execute("UPDATE deliveries SET message_id='77' WHERE delivery_id='d1'")
        self.assertIsNotNone(self.store.delivery_for_message(MessageRef('100', '200', '77')))
        self.assertIsNone(self.store.delivery_for_message(MessageRef('100', '999', '77')))
        self.assertIsNone(self.store.delivery_for_message(MessageRef('100', '200', '78')))

    # ------------------------------------------------------------ claiming

    def test_claim_basic(self):
        self.store.enqueue_delivery(delivery())
        claimed = self.store.claim_delivery('d1', LIMITS, now=1000.0)
        self.assertEqual(claimed['status'], 'sending')
        self.assertEqual(claimed['attempted_at'], 1000.0)
        self.assertIsNone(self.store.claim_delivery('d1', LIMITS, now=1001.0))
        day = utc_day(1000.0)
        self.assertEqual(self.store.budget(day, '100:200', 'batches'), {'used': 0.0, 'reserved': 1.0})
        self.assertEqual(self.store.budget(day, '100:200', 'nodes'), {'used': 0.0, 'reserved': 2.0})
        self.assertEqual(self.store.budget(day, 'global', 'publish_bytes'), {'used': 0.0, 'reserved': 100.0})

    def test_claim_expired_and_pause(self):
        self.store.enqueue_delivery(delivery(delivery_id='d1', fingerprint='fa', expires_at=500.0))
        self.assertIsNone(self.store.claim_delivery('d1', LIMITS, now=600.0))
        self.store.enqueue_delivery(delivery(delivery_id='d2', fingerprint='fb'))
        self.store.set_paused('100', '200', True)
        self.assertIsNone(self.store.claim_delivery('d2', LIMITS, now=600.0))
        self.store.set_paused('100', '200', False)
        self.assertIsNotNone(self.store.claim_delivery('d2', LIMITS, now=600.0))
        self.store.finish_delivery('d2', 'sent', message_id='1')
        self.store.enqueue_delivery(delivery(delivery_id='d3', fingerprint='fc'))
        self.store.set_paused('*', '*', True)
        self.assertIsNone(self.store.claim_delivery('d3', LIMITS, now=700.0))
        self.store.set_paused('*', '*', False)
        self.store.set_paused('100', '*', True)
        self.assertIsNone(self.store.claim_delivery('d3', LIMITS, now=700.0))
        self.store.set_paused('100', '*', False)
        self.assertIsNotNone(self.store.claim_delivery('d3', LIMITS, now=700.0))

    def test_claim_part_ordering(self):
        self.store.enqueue_delivery(delivery(delivery_id='p0', fingerprint='fx', part_index=0, part_total=2))
        self.store.enqueue_delivery(delivery(delivery_id='p1', fingerprint='fx', part_index=1, part_total=2))
        self.assertIsNone(self.store.claim_delivery('p1', LIMITS, now=1000.0))
        self.assertIsNotNone(self.store.claim_delivery('p0', LIMITS, now=1000.0))
        self.assertIsNone(self.store.claim_delivery('p1', LIMITS, now=1001.0))
        self.store.finish_delivery('p0', 'sent', message_id='10')
        self.assertIsNotNone(self.store.claim_delivery('p1', LIMITS, now=1002.0))
        self.store.enqueue_delivery(delivery(delivery_id='q0', fingerprint='fy', part_index=0, part_total=2))
        self.store.enqueue_delivery(delivery(delivery_id='q1', fingerprint='fy', part_index=1, part_total=2))
        self.store.claim_delivery('q0', LIMITS, now=2000.0)
        self.store.finish_delivery('q0', 'failed', error_code='boom')
        self.assertIsNone(self.store.claim_delivery('q1', LIMITS, now=2001.0))

    def test_claim_no_skip(self):
        self.store.enqueue_delivery(delivery(delivery_id='old', fingerprint='fa', created_at=1000.0))
        self.store.enqueue_delivery(delivery(delivery_id='new', fingerprint='fb', created_at=2000.0))
        # Independent content can be scheduled while another bundle is waiting.
        self.assertIsNotNone(self.store.claim_delivery('new', LIMITS, now=3000.0))
        self.assertIsNotNone(self.store.claim_delivery('old', LIMITS, now=3000.0))
        self.store.finish_delivery('old', 'sent', message_id='5')
        self.assertEqual(self.store.get_delivery('new')['status'],'sending')

    def test_claim_min_interval(self):
        limits = dict(LIMITS, min_interval_seconds=600)
        self.store.enqueue_delivery(delivery(delivery_id='d1', fingerprint='fa'))
        self.store.enqueue_delivery(delivery(delivery_id='d2', fingerprint='fb'))
        self.assertIsNotNone(self.store.claim_delivery('d1', limits, now=1000.0))
        self.store.finish_delivery('d1', 'sent', message_id='1')
        self.assertIsNone(self.store.claim_delivery('d2', limits, now=1599.0))
        self.assertIsNotNone(self.store.claim_delivery('d2', limits, now=1600.0))

    def test_claim_utc_rollover(self):
        limits = dict(LIMITS, min_interval_seconds=600)
        t1 = datetime(2026, 6, 30, 23, 59, 59, tzinfo=timezone.utc).timestamp()
        self.store.enqueue_delivery(delivery(delivery_id='d1', fingerprint='fa',
                                             expires_at=t1 + 10 * DAY, created_at=t1 - 1000))
        self.store.enqueue_delivery(delivery(delivery_id='d2', fingerprint='fb',
                                             expires_at=t1 + 10 * DAY, created_at=t1 - 500))
        self.assertIsNotNone(self.store.claim_delivery('d1', limits, now=t1))
        self.store.finish_delivery('d1', 'sent', message_id='1')
        t2 = datetime(2026, 7, 1, 0, 0, 1, tzinfo=timezone.utc).timestamp()
        self.assertIsNone(self.store.claim_delivery('d2', limits, now=t2))
        self.assertIsNotNone(self.store.claim_delivery('d2', limits, now=t2 + 600))
        batches = {row['day']: row['used'] + row['reserved'] for row in
                   self.store.budget_summary('2026-06-30') + self.store.budget_summary('2026-07-01')
                   if row['metric'] == 'batches' and row['scope'] == '100:200'}
        self.assertEqual(batches, {'2026-06-30': 1.0, '2026-07-01': 1.0})

    def test_claim_daily_limits(self):
        limits = {'min_interval_seconds': 0, 'daily_batches': 1, 'daily_nodes': 100,
                  'daily_publish_bytes': 10 ** 9}
        self.store.enqueue_delivery(delivery(delivery_id='d1', fingerprint='fa'))
        self.store.enqueue_delivery(delivery(delivery_id='d2', fingerprint='fb'))
        self.assertIsNotNone(self.store.claim_delivery('d1', limits, now=1000.0))
        self.store.finish_delivery('d1', 'sent', message_id='1')
        self.assertIsNone(self.store.claim_delivery('d2', limits, now=1001.0))
        self.store.enqueue_delivery(delivery(delivery_id='d3', group='999', fingerprint='fc'))
        self.assertIsNotNone(self.store.claim_delivery('d3', limits, now=1002.0))
        day = utc_day(1000.0)
        self.assertEqual(self.store.budget(day, '100:200', 'batches'), {'used': 1.0, 'reserved': 0.0})
        node_limits = dict(LIMITS, daily_nodes=3)
        self.store.enqueue_delivery(delivery(delivery_id='d4', group='300', fingerprint='fd', node_count=5))
        self.assertIsNone(self.store.claim_delivery('d4', node_limits, now=1000.0))
        self.assertEqual(self.store.budget(day, '100:300', 'nodes'), {'used': 0.0, 'reserved': 0.0})
        self.assertEqual(self.store.budget(day, '100:300', 'batches'), {'used': 0.0, 'reserved': 0.0})
        byte_limits = dict(LIMITS, daily_publish_bytes=50)
        self.store.enqueue_delivery(delivery(delivery_id='d5', group='400', fingerprint='fe',
                                             estimated_bytes=100, node_count=1))
        self.assertIsNone(self.store.claim_delivery('d5', byte_limits, now=1000.0))
        self.assertEqual(self.store.budget(day, 'global', 'publish_bytes')['reserved'], 100.0)

    def test_claim_concurrent_two_connections(self):
        self.store.enqueue_delivery(delivery(estimated_bytes=100, node_count=2))
        barrier = threading.Barrier(2)
        results = []

        def claim():
            store = Store(self.path)
            try:
                barrier.wait(timeout=10)
                results.append(store.claim_delivery('d1', LIMITS, now=5000.0))
            finally:
                store.close()

        threads = [threading.Thread(target=claim) for _ in range(2)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        self.assertEqual(len([result for result in results if result is not None]), 1)
        check = Store(self.path)
        try:
            self.assertEqual(check.get_delivery('d1')['status'], 'sending')
            day = utc_day(5000.0)
            self.assertEqual(check.budget(day, 'global', 'publish_bytes'), {'used': 0.0, 'reserved': 100.0})
            self.assertEqual(check.budget(day, '100:200', 'batches'), {'used': 0.0, 'reserved': 1.0})
        finally:
            check.close()

    # ------------------------------------------------------------ outcomes

    def test_finish_sent(self):
        self.store.enqueue_delivery(delivery(estimated_bytes=100, node_count=2))
        self.store.claim_delivery('d1', LIMITS, now=1000.0)
        with self.assertRaises(ValueError):
            self.store.finish_delivery('d1', 'sent')
        self.store.finish_delivery('d1', 'sent', message_id='77')
        finished = self.store.get_delivery('d1')
        self.assertEqual(finished['status'], 'sent')
        self.assertEqual(finished['message_id'], '77')
        self.assertEqual(finished['attempted_at'], 1000.0)
        day = utc_day(1000.0)
        self.assertEqual(self.store.budget(day, '100:200', 'batches'), {'used': 1.0, 'reserved': 0.0})
        self.assertEqual(self.store.budget(day, '100:200', 'nodes'), {'used': 2.0, 'reserved': 0.0})
        self.assertEqual(self.store.budget(day, 'global', 'publish_bytes'), {'used': 100.0, 'reserved': 0.0})
        self.store.finish_delivery('d1', 'failed')
        self.assertEqual(self.store.get_delivery('d1')['status'], 'sent')
        self.store.enqueue_delivery(delivery(delivery_id='q', fingerprint='fq'))
        self.store.finish_delivery('q', 'sent', message_id='1')
        self.assertEqual(self.store.get_delivery('q')['status'], 'queued')
        with self.assertRaises(ValueError):
            self.store.finish_delivery('q', 'bogus')

    def test_finish_failed_unknown(self):
        for index, (delivery_id, status) in enumerate((('x1', 'failed'), ('x2', 'unknown'))):
            self.store.enqueue_delivery(delivery(delivery_id=delivery_id, fingerprint=f'fx{index}'))
            self.store.claim_delivery(delivery_id, LIMITS, now=1000.0)
            self.store.finish_delivery(delivery_id, status, error_code='err')
            finished = self.store.get_delivery(delivery_id)
            self.assertEqual(finished['status'], status)
            self.assertEqual(finished['error_code'], 'err')
        day = utc_day(1000.0)
        self.assertEqual(self.store.budget(day, '100:200', 'batches'), {'used': 2.0, 'reserved': 0.0})
        self.assertEqual(self.store.budget(day, '100:200', 'nodes'), {'used': 4.0, 'reserved': 0.0})
        self.assertEqual(self.store.budget(day, 'global', 'publish_bytes'), {'used': 200.0, 'reserved': 0.0})

    def test_finish_cancelled(self):
        self.store.enqueue_delivery(delivery(delivery_id='k1', fingerprint='fk1'))
        self.store.claim_delivery('k1', LIMITS, now=1000.0)
        self.store.finish_delivery('k1', 'cancelled')
        day = utc_day(1000.0)
        self.assertEqual(self.store.budget(day, '100:200', 'batches'), {'used': 0.0, 'reserved': 0.0})
        self.assertEqual(self.store.budget(day, 'global', 'publish_bytes'), {'used': 0.0, 'reserved': 0.0})
        self.assertEqual(self.store.get_delivery('k1')['status'], 'cancelled')

    def test_confirm_delivery(self):
        self.store.enqueue_delivery(delivery(delivery_id='c1', fingerprint='fc1'))
        self.store.claim_delivery('c1', LIMITS, now=1000.0)
        self.store.finish_delivery('c1', 'unknown', error_code='lost')
        self.store.confirm_delivery('c1', '55')
        confirmed = self.store.get_delivery('c1')
        self.assertEqual(confirmed['status'], 'sent')
        self.assertEqual(confirmed['message_id'], '55')
        with self.assertRaises(StoreError):
            self.store.confirm_delivery('c1', '56')
        self.assertEqual(self.store.get_delivery('c1')['message_id'], '55')
        self.store.enqueue_delivery(delivery(delivery_id='c2', fingerprint='fc2'))
        self.store.claim_delivery('c2', LIMITS, now=1001.0)
        self.store.finish_delivery('c2', 'failed', error_code='nope')
        with self.assertRaises(StoreError):
            self.store.confirm_delivery('c2', '57')
        self.assertEqual(self.store.get_delivery('c2')['status'], 'failed')

    def test_retry_delivery(self):
        limits = dict(LIMITS, daily_batches=10)
        self.store.enqueue_delivery(delivery(delivery_id='t1', fingerprint='ft1', expires_at=100000.0))
        self.store.claim_delivery('t1', limits, now=1000.0)
        self.store.finish_delivery('t1', 'failed', error_code='boom')
        self.store.retry_delivery('t1', now=1500.0)
        retried = self.store.get_delivery('t1')
        self.assertEqual(retried['status'], 'queued')
        self.assertIsNone(retried['error_code'])
        self.assertIsNone(retried['message_id'])
        self.assertIsNotNone(self.store.claim_delivery('t1', limits, now=1500.0))
        day = utc_day(1000.0)
        self.assertEqual(self.store.budget(day, '100:200', 'batches'), {'used': 1.0, 'reserved': 1.0})
        self.store.finish_delivery('t1', 'sent', message_id='6')
        with self.assertRaises(StoreError):
            self.store.retry_delivery('t1', now=1600.0)
        self.assertEqual(self.store.get_delivery('t1')['status'], 'sent')
        self.store.enqueue_delivery(delivery(delivery_id='t2', group='999', fingerprint='ft2', expires_at=2000.0))
        self.store.claim_delivery('t2', limits, now=1000.0)
        self.store.finish_delivery('t2', 'failed', error_code='expired')
        with self.assertRaises(StoreError):
            self.store.retry_delivery('t2', now=3000.0)
        self.assertEqual(self.store.get_delivery('t2')['status'], 'failed')

    def test_recover_inflight(self):
        for delivery_id, fingerprint in (('r1', 'fr1'), ('r2', 'fr2'), ('r3', 'fr3')):
            self.store.enqueue_delivery(delivery(delivery_id=delivery_id, fingerprint=fingerprint))
        self.store.claim_delivery('r1', LIMITS, now=1000.0)
        self.store.claim_delivery('r2', LIMITS, now=1001.0)
        self.assertEqual(self.store.recover_inflight(), 2)
        self.assertEqual(self.store.get_delivery('r1')['status'], 'unknown')
        self.assertEqual(self.store.get_delivery('r2')['status'], 'unknown')
        self.assertEqual(self.store.get_delivery('r3')['status'], 'queued')
        day = utc_day(1000.0)
        self.assertEqual(self.store.budget(day, '100:200', 'batches'), {'used': 2.0, 'reserved': 0.0})
        self.assertEqual(self.store.budget(day, 'global', 'publish_bytes'), {'used': 200.0, 'reserved': 0.0})

    def test_cancel_candidate(self):
        self.store.put_candidate(bundle(bundle_id='b1', fingerprint='f1'))
        self.store.enqueue_delivery(delivery(delivery_id='q1', group='999', bundle_id='b1', fingerprint='fq1'))
        self.store.enqueue_delivery(delivery(delivery_id='s1', bundle_id='b1', fingerprint='fs1'))
        self.store.enqueue_delivery(delivery(delivery_id='u1', bundle_id='b1', fingerprint='fu1',
                                             created_at=2000.0))
        self.store.claim_delivery('s1', LIMITS, now=1000.0)
        self.store.finish_delivery('s1', 'sent', message_id='5')
        self.store.claim_delivery('u1', LIMITS, now=1001.0)
        self.store.finish_delivery('u1', 'unknown', error_code='lost')
        self.store.cancel_candidate('b1', 'boring')
        self.assertEqual(self.store.get_candidate('b1', now=2000.0)['status'], 'cancelled')
        self.assertEqual(self.store.db.execute(
            "SELECT cancel_reason FROM candidates WHERE bundle_id='b1'").fetchone()['cancel_reason'], 'boring')
        self.assertEqual(self.store.get_delivery('q1')['status'], 'cancelled')
        self.assertEqual(self.store.get_delivery('s1')['status'], 'sent')
        self.assertEqual(self.store.get_delivery('u1')['status'], 'unknown')

    # ------------------------------------------------------------ feedback

    def test_feedback_roundtrip(self):
        signal = feedback_signal()
        self.assertTrue(self.store.put_feedback(signal))
        self.assertFalse(self.store.put_feedback(signal))
        self.assertEqual(self.store.db.execute('SELECT COUNT(*) AS n FROM feedback').fetchone()['n'], 1)
        self.assertFalse(self.store.put_feedback(dict(signal, active=False)))
        self.assertEqual(self.store.db.execute('SELECT COUNT(*) AS n FROM feedback').fetchone()['n'], 1)
        self.assertEqual(self.store.feedback_since('100', '200', 0.0, now=2000.0), [])
        self.assertFalse(self.store.put_feedback(dict(signal, active=True)))
        got = self.store.feedback_since('100', '200', 0.0, now=2000.0)
        self.assertEqual(len(got), 1)
        self.assertEqual(got[0]['signal_id'], 's1')
        self.assertEqual(got[0]['payload'], {'e': 1})
        self.assertTrue(got[0]['active'])
        with self.assertRaises(ValueError):
            self.store.put_feedback(dict(signal, direction='bogus'))

    def test_feedback_since_scoped(self):
        self.store.put_feedback(feedback_signal(signal_id='s1'))
        self.store.put_feedback(feedback_signal(signal_id='s2', group='999'))
        self.store.put_feedback(feedback_signal(signal_id='s3', account='999'))
        self.store.put_feedback(feedback_signal(signal_id='s4', occurred_at=5000.0))
        self.store.put_feedback(feedback_signal(signal_id='s5', expires_at=2000.0))
        got = self.store.feedback_since('100', '200', 0.0, now=6000.0)
        self.assertEqual([item['signal_id'] for item in got], ['s1', 's4'])
        got = self.store.feedback_since('100', '200', 1500.0, now=6000.0)
        self.assertEqual([item['signal_id'] for item in got], ['s4'])

    # ------------------------------------------------------------ group state

    def test_group_state(self):
        self.assertEqual(self.store.group_state('100', '200'),
                         {'account_id': '100', 'group_id': '200', 'paused': False, 'preferences': {},
                          'version': 0, 'snapshots': [], 'last_learning_at': 0.0, 'last_cleanup_at': 0.0})
        self.store.set_group_state('100', '200', {'preferences': {'a': 1}, 'version': 2, 'snapshots': ['s'],
                                                  'last_learning_at': 5.0, 'paused': True, 'ignored': 1})
        state = self.store.group_state('100', '200')
        self.assertTrue(state['paused'])
        self.assertEqual(state['preferences'], {'a': 1})
        self.assertEqual(state['version'], 2)
        self.assertEqual(state['snapshots'], ['s'])
        self.assertEqual(state['last_learning_at'], 5.0)
        self.assertEqual(state['last_cleanup_at'], 0.0)
        self.store.set_group_state('100', '200', {'last_cleanup_at': 7.0})
        self.assertEqual(self.store.group_state('100', '200')['version'], 2)
        self.assertEqual(self.store.group_state('100', '200')['last_cleanup_at'], 7.0)

    def test_paused_scopes(self):
        self.assertFalse(self.store.paused('100', '200'))
        self.store.set_paused('100', '200', True)
        self.assertTrue(self.store.paused('100', '200'))
        self.assertFalse(self.store.paused('100', '999'))
        self.store.set_paused('100', '200', False)
        self.store.set_paused('*', '*', True)
        self.assertTrue(self.store.paused('100', '999'))
        self.store.set_paused('*', '*', False)
        self.store.set_paused('100', '*', True)
        self.assertTrue(self.store.paused('100', '999'))
        self.assertFalse(self.store.paused('200', '999'))

    def test_set_paused_merges(self):
        self.store.set_group_state('100', '200', {'preferences': {'a': 1}})
        self.store.set_paused('100', '200', True)
        state = self.store.group_state('100', '200')
        self.assertTrue(state['paused'])
        self.assertEqual(state['preferences'], {'a': 1})

    # ------------------------------------------------------------ budgets

    def test_budget_reserve_settle(self):
        self.assertTrue(self.store.reserve_budget('2026-01-01', 'g', 'm', 4.0, 10.0))
        self.assertFalse(self.store.reserve_budget('2026-01-01', 'g', 'm', 7.0, 10.0))
        self.assertEqual(self.store.budget('2026-01-01', 'g', 'm'), {'used': 0.0, 'reserved': 4.0})
        self.store.settle_budget('2026-01-01', 'g', 'm', 4.0, 3.0)
        self.assertEqual(self.store.budget('2026-01-01', 'g', 'm'), {'used': 3.0, 'reserved': 0.0})
        self.store.reserve_budget('2026-01-02', 'x', 'n', 10.0, 100.0)
        self.store.settle_budget('2026-01-02', 'x', 'n', 4.0, 5.0)
        self.assertEqual(self.store.budget('2026-01-02', 'x', 'n'), {'used': 4.0, 'reserved': 6.0})
        self.store.settle_budget('2026-01-02', 'x', 'n', 100.0, 3.0)
        self.assertEqual(self.store.budget('2026-01-02', 'x', 'n'), {'used': 7.0, 'reserved': 0.0})
        for bad in (-1.0, float('nan'), float('inf')):
            with self.assertRaises(ValueError):
                self.store.reserve_budget('2026-01-01', 'g', 'm', bad, 10.0)
        with self.assertRaises(ValueError):
            self.store.reserve_budget('2026-13-40', 'g', 'm', 1.0, 10.0)
        with self.assertRaises(ValueError):
            self.store.settle_budget('2026-01-01', 'g', 'm', -1.0, 0.0)
        self.assertEqual(self.store.budget('2026-01-03', 'g', 'm'), {'used': 0.0, 'reserved': 0.0})

    def test_budget_concurrent_reserve(self):
        barrier = threading.Barrier(2)
        results = []

        def reserve():
            store = Store(self.path)
            try:
                barrier.wait(timeout=10)
                results.append(store.reserve_budget('2026-05-05', 'g', 'm', 6.0, 10.0))
            finally:
                store.close()

        threads = [threading.Thread(target=reserve) for _ in range(2)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        self.assertEqual(sorted(results), [False, True])
        self.assertEqual(self.store.budget('2026-05-05', 'g', 'm'), {'used': 0.0, 'reserved': 6.0})

    def test_summary_counts(self):
        self.store.put_message(message())
        self.store.put_candidate(bundle())
        self.store.enqueue_delivery(delivery())
        self.store.put_feedback(feedback_signal())
        self.store.set_group_state('100', '200', {'paused': True})
        self.store.reserve_budget('2026-01-01', 'g', 'm', 1.0, 10.0)
        self.assertEqual(self.store.summary(), {'messages': 1, 'candidates': 1, 'deliveries': 1,
                                                'feedback': 1, 'group_state': 1, 'daily_budgets': 1})

    # ------------------------------------------------------------ cleanup

    def test_cleanup_expired_content(self):
        self.store.put_message(message(message_id='1', expires_at=10000.0))
        self.store.put_message(message(message_id='2', expires_at=3000.0))
        self.store.put_candidate(bundle(bundle_id='c1', fingerprint='fc1', expires_at=10000.0))
        self.store.put_candidate(bundle(bundle_id='c2', fingerprint='fc2', expires_at=3000.0))
        self.store.put_feedback(feedback_signal(signal_id='f1', expires_at=10000.0))
        self.store.put_feedback(feedback_signal(signal_id='f2', expires_at=3000.0))
        self.store.enqueue_delivery(delivery(delivery_id='q_old', group='999', bundle_id='c2', fingerprint='fq',
                                             expires_at=4000.0))
        self.store.enqueue_delivery(delivery(delivery_id='s_old', bundle_id='c2', fingerprint='fs',
                                             expires_at=4000.0, tags=['keep']))
        self.store.enqueue_delivery(delivery(delivery_id='u_old', bundle_id='c2', fingerprint='fu',
                                             expires_at=4000.0, created_at=1500.0))
        self.store.enqueue_delivery(delivery(delivery_id='keep_ref', bundle_id='c2', fingerprint='fr',
                                             expires_at=10000.0, created_at=2000.0))
        self.store.enqueue_delivery(delivery(delivery_id='ancient', group='888', bundle_id='c2', fingerprint='fv',
                                             created_at=-8000000.0, expires_at=100000.0))
        self.store.claim_delivery('s_old', LIMITS, now=2000.0)
        self.store.finish_delivery('s_old', 'sent', message_id='9')
        self.store.claim_delivery('u_old', LIMITS, now=2100.0)
        self.store.finish_delivery('u_old', 'unknown', error_code='lost')
        self.store.reserve_budget('1960-01-01', 'g', 'm', 1.0, 10.0)
        self.store.reserve_budget(utc_day(5000.0), 'global', 'm', 1.0, 10.0)
        result = self.store.cleanup(now=5000.0)
        self.assertEqual(result['messages'], 1)
        self.assertEqual(result['candidates'], 1)
        self.assertEqual(result['feedback'], 1)
        self.assertEqual(result['deliveries_cancelled'], 2)
        self.assertEqual(result['deliveries_erased'], 4)
        self.assertEqual(result['deliveries_removed'], 1)
        self.assertEqual(result['budgets'], 1)
        self.assertFalse(result['busy'])
        self.assertIsNotNone(self.store.get_message(MessageRef('100', '200', '1'), now=5000.0))
        self.assertIsNone(self.store.get_message(MessageRef('100', '200', '2'), now=5000.0))
        self.assertIsNotNone(self.store.get_candidate('c1', now=5000.0))
        self.assertIsNone(self.store.get_candidate('c2', now=5000.0))
        self.assertEqual([item['signal_id'] for item in self.store.feedback_since('100', '200', 0.0, now=5000.0)],
                         ['f1'])
        queued = self.store.get_delivery('q_old')
        self.assertEqual(queued['status'], 'cancelled')
        self.assertEqual(queued['nodes'], [])
        self.assertEqual(queued['tags'], [])
        self.assertIsNone(queued['error_code'])
        sent = self.store.get_delivery('s_old')
        self.assertEqual(sent['status'], 'sent')
        self.assertEqual(sent['message_id'], '9')
        self.assertEqual(sent['fingerprint'], 'fs')
        self.assertEqual(sent['nodes'], [])
        self.assertEqual(sent['tags'], [])
        unknown = self.store.get_delivery('u_old')
        self.assertEqual(unknown['status'], 'unknown')
        self.assertIsNone(unknown['error_code'])
        self.assertIsNone(self.store.get_delivery('ancient'))
        retained = self.store.get_delivery('keep_ref')
        self.assertEqual(retained['bundle_id'], 'c2')
        self.assertEqual(retained['nodes'], [{'type': 'node', 'data': {'id': 'x'}}])
        self.assertFalse(self.store.enqueue_delivery(delivery(delivery_id='dedup', group='999', bundle_id='c2',
                                                              fingerprint='fq')))

    def test_cleanup_compact_reclaims_bytes(self):
        big = [{'type': 'text', 'text': 'x' * 4096}]
        for index in range(250):
            self.store.put_message(message(message_id=str(index), segments=big, expires_at=100.0))
        self.store.put_candidate(bundle(bundle_id='big', fingerprint='fb', expires_at=100.0,
                                        messages=[message(message_id='13', segments=big)]))
        self.store.db.execute('PRAGMA wal_checkpoint(FULL)')
        result = self.store.cleanup(now=500.0, compact=True)
        self.assertEqual(result['messages'], 250)
        self.assertEqual(result['candidates'], 1)
        self.assertFalse(result['busy'])
        self.assertLess(result['bytes_after'], result['bytes_before'])
        self.assertEqual(self.store.db.execute('PRAGMA freelist_count').fetchone()[0], 0)
        wal = Path(str(self.path) + '-wal')
        self.assertEqual(wal.stat().st_size, 0)

    def test_cleanup_reader_lock_release(self):
        self.store.put_message(message(expires_at=100.0))
        reader = sqlite3.connect(self.path, isolation_level=None)
        reader.execute('BEGIN')
        reader.execute('SELECT COUNT(*) FROM messages').fetchone()
        try:
            result = self.store.cleanup(now=500.0, compact=True)
            self.assertEqual(result['messages'], 1)
            self.assertTrue(result['busy'])
        finally:
            reader.execute('COMMIT')
            reader.close()
        retry = self.store.cleanup(now=500.0, compact=True)
        self.assertFalse(retry['busy'])
        self.assertEqual(self.store.db.execute('PRAGMA freelist_count').fetchone()[0], 0)

    # ------------------------------------------------------------ restart

    def test_restart_recovery(self):
        self.store.put_message(message())
        self.store.enqueue_delivery(delivery())
        self.store.claim_delivery('d1', LIMITS, now=1000.0)
        self.store.close()
        self.store = Store(self.path)
        self.addCleanup(self.store.close)
        self.assertIsNotNone(self.store.get_message(MessageRef('100', '200', '1'), now=1001.0))
        self.assertEqual(self.store.get_delivery('d1')['status'], 'sending')
        self.assertEqual(self.store.recover_inflight(), 1)
        self.assertEqual(self.store.get_delivery('d1')['status'], 'unknown')


if __name__ == '__main__':
    unittest.main()
