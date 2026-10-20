"""SQLite persistence for messages, candidates, deliveries, feedback, group state and budgets.

The store owns the schema, transactions and retention policy. It depends only on
the shared contracts module and the standard library. Time is handled as float
unix timestamps; budget days are UTC dates formatted YYYY-MM-DD.
"""

from __future__ import annotations

from contextlib import contextmanager
from dataclasses import asdict
import datetime
import json
import math
import os
import sqlite3
import time
from pathlib import Path
from typing import Any, Iterator

from pullshit.contracts import DAY, ChatMessage, ContentBundle, MessageRef, canonical, platform_id, digest

MESSAGE_LIFETIME = 3 * DAY
DELIVERY_RETENTION = 90 * DAY
BUDGET_RETENTION_DAYS = 30

TABLES = ('candidates', 'daily_budgets', 'deliveries', 'feedback', 'group_state', 'messages')

CANDIDATE_STATUSES = frozenset({'collected', 'bundled', 'analyzed', 'eligible', 'rejected', 'needs_review', 'cancelled'})
FEEDBACK_DIRECTIONS = frozenset({'positive', 'negative', 'unknown'})
GROUP_STATE_FIELDS = ('paused', 'preferences', 'version', 'snapshots', 'last_learning_at', 'last_cleanup_at')
LIMIT_KEYS = ('min_interval_seconds', 'daily_batches', 'daily_segments', 'daily_publish_bytes')
DELIVERY_KEYS = ('delivery_id', 'account_id', 'group_id', 'bundle_id', 'fingerprint', 'part_index', 'part_total',
                 'nodes', 'estimated_bytes', 'node_count', 'expires_at', 'created_at', 'tags')
FEEDBACK_KEYS = ('signal_id', 'account_id', 'group_id', 'bundle_id', 'publication_id', 'user_id', 'kind',
                 'direction', 'tags', 'occurred_at', 'expires_at', 'payload')

_SCHEMA = """
CREATE TABLE messages (
    account_id TEXT NOT NULL,
    group_id TEXT NOT NULL,
    message_id TEXT NOT NULL,
    user_id TEXT NOT NULL DEFAULT '',
    nickname TEXT NOT NULL DEFAULT '',
    occurred_at REAL NOT NULL,
    segments TEXT NOT NULL DEFAULT '[]',
    own INTEGER NOT NULL DEFAULT 0,
    recalled INTEGER NOT NULL DEFAULT 0,
    expires_at REAL NOT NULL,
    PRIMARY KEY (account_id, group_id, message_id)
);
CREATE INDEX idx_messages_scope_time ON messages (account_id, group_id, occurred_at);

CREATE TABLE candidates (
    bundle_id TEXT PRIMARY KEY,
    fingerprint TEXT NOT NULL,
    body TEXT,
    media_paths TEXT NOT NULL DEFAULT '[]',
    created_at REAL NOT NULL,
    expires_at REAL NOT NULL,
    status TEXT NOT NULL DEFAULT 'bundled',
    analysis TEXT,
    cancel_reason TEXT
);
CREATE INDEX idx_candidates_status_time ON candidates (status, created_at);

CREATE TABLE deliveries (
    delivery_id TEXT PRIMARY KEY,
    account_id TEXT NOT NULL,
    group_id TEXT NOT NULL,
    bundle_id TEXT NOT NULL,
    fingerprint TEXT NOT NULL,
    part_index INTEGER NOT NULL,
    part_total INTEGER NOT NULL,
    nodes TEXT NOT NULL,
    sources TEXT NOT NULL DEFAULT '[]',
    estimated_bytes INTEGER NOT NULL,
    node_count INTEGER NOT NULL,
    expires_at REAL NOT NULL,
    created_at REAL NOT NULL,
    tags TEXT NOT NULL DEFAULT '[]',
    status TEXT NOT NULL DEFAULT 'queued',
    message_id TEXT,
    error_code TEXT,
    attempted_at REAL,
    charged_day TEXT,
    charged_batches INTEGER NOT NULL DEFAULT 0,
    charged_nodes INTEGER NOT NULL DEFAULT 0,
    charged_bytes INTEGER NOT NULL DEFAULT 0,
    UNIQUE (account_id, group_id, fingerprint, part_index)
);
CREATE INDEX idx_deliveries_scope_status ON deliveries (account_id, group_id, status, created_at, part_index);

CREATE TABLE feedback (
    signal_id TEXT PRIMARY KEY,
    account_id TEXT NOT NULL,
    group_id TEXT NOT NULL,
    bundle_id TEXT,
    publication_id TEXT,
    user_id TEXT NOT NULL,
    kind TEXT NOT NULL,
    direction TEXT NOT NULL,
    tags TEXT NOT NULL DEFAULT '[]',
    occurred_at REAL NOT NULL,
    expires_at REAL NOT NULL,
    payload TEXT NOT NULL DEFAULT '{}',
    active INTEGER NOT NULL DEFAULT 1
);
CREATE INDEX idx_feedback_scope_time ON feedback (account_id, group_id, occurred_at);

CREATE TABLE group_state (
    account_id TEXT NOT NULL,
    group_id TEXT NOT NULL,
    paused INTEGER NOT NULL DEFAULT 0,
    preferences TEXT NOT NULL DEFAULT '{}',
    version INTEGER NOT NULL DEFAULT 0,
    snapshots TEXT NOT NULL DEFAULT '[]',
    last_learning_at REAL NOT NULL DEFAULT 0,
    last_cleanup_at REAL NOT NULL DEFAULT 0,
    PRIMARY KEY (account_id, group_id)
);

CREATE TABLE daily_budgets (
    day TEXT NOT NULL,
    scope TEXT NOT NULL,
    metric TEXT NOT NULL,
    used REAL NOT NULL DEFAULT 0,
    reserved REAL NOT NULL DEFAULT 0,
    PRIMARY KEY (day, scope, metric)
);
"""


class StoreError(RuntimeError):
    """A persistence invariant was violated, e.g. an unsupported schema or an immutable id conflict."""


class _Blocked(Exception):
    """Internal marker: a delivery claim was refused and its transaction must roll back."""


class Store:
    """Owns the SQLite database for the six business tables."""

    def __init__(self, path: Path, retention: dict | None = None):
        self.retention = {'message_days': 3, 'content_days': 7, 'delivery_days': 90, 'budget_days': 30, **(retention or {})}
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        fresh = not self.path.exists() or self.path.stat().st_size == 0
        self.db = sqlite3.connect(self.path, isolation_level=None)
        self.db.row_factory = sqlite3.Row
        try:
            self.db.execute('PRAGMA busy_timeout = 2000')
            version = self.db.execute('PRAGMA user_version').fetchone()[0]
            if version == 0 and fresh:
                self.db.execute('PRAGMA auto_vacuum = INCREMENTAL')
                self.db.execute('PRAGMA journal_mode = WAL')
                self.db.execute('PRAGMA synchronous = FULL')
                self.db.execute('PRAGMA foreign_keys = ON')
                self.db.executescript(_SCHEMA)
                self.db.execute('PRAGMA user_version = 1')
            elif version == 1:
                self.db.execute('PRAGMA journal_mode = WAL')
                self.db.execute('PRAGMA synchronous = FULL')
                self.db.execute('PRAGMA foreign_keys = ON')
                self._check_schema()
            else:
                raise StoreError('unsupported database schema')
            self._private_permissions()
        except BaseException:
            self.db.close()
            raise

    def close(self) -> None:
        self.db.close()

    # ------------------------------------------------------------------ setup

    def _check_schema(self) -> None:
        rows = self.db.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'").fetchall()
        if sorted(row['name'] for row in rows) != sorted(TABLES):
            raise StoreError('unsupported database schema')
        if self.db.execute('PRAGMA auto_vacuum').fetchone()[0] != 2:
            raise StoreError('unsupported database schema')

    def _private_permissions(self) -> None:
        os.chmod(self.path, 0o600)
        for suffix in ('-wal', '-shm'):
            sidecar = Path(str(self.path) + suffix)
            if sidecar.exists():
                os.chmod(sidecar, 0o600)

    @staticmethod
    def _now(now: float | None) -> float:
        return time.time() if now is None else float(now)

    @staticmethod
    def _utc_day(now: float) -> str:
        return datetime.datetime.fromtimestamp(now, tz=datetime.timezone.utc).strftime('%Y-%m-%d')

    @staticmethod
    def _number(value: Any, name: str) -> float:
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            raise ValueError(f'invalid {name}')
        number = float(value)
        if not math.isfinite(number):
            raise ValueError(f'invalid {name}')
        return number

    @classmethod
    def _amount(cls, value: Any, name: str) -> float:
        number = cls._number(value, name)
        if number < 0:
            raise ValueError(f'invalid {name}')
        return number

    @staticmethod
    def _day(day: Any) -> str:
        if not isinstance(day, str):
            raise ValueError('invalid day')
        try:
            datetime.date.fromisoformat(day)
        except ValueError:
            raise ValueError('invalid day') from None
        return day

    @staticmethod
    def _scope_metric(scope: Any, metric: Any) -> None:
        if not isinstance(scope, str) or not scope:
            raise ValueError('invalid scope')
        if not isinstance(metric, str) or not metric:
            raise ValueError('invalid metric')

    @staticmethod
    def _platform(account_id: Any, group_id: Any) -> tuple[str, str]:
        return platform_id(account_id), platform_id(group_id)

    @contextmanager
    def _tx(self) -> Iterator[None]:
        self.db.execute('BEGIN IMMEDIATE')
        try:
            yield
        except BaseException:
            self.db.execute('ROLLBACK')
            raise
        else:
            self.db.execute('COMMIT')

    # ----------------------------------------------------------------- messages

    def put_message(self, message: ChatMessage) -> bool:
        """Insert an idempotent (account, group, message) record; return True only for a new row.

        Recalling is sticky and expiry never extends: an existing row keeps its
        recall flag and takes the minimum of the stored and incoming expiry.
        """
        if not isinstance(message, ChatMessage):
            raise TypeError('message must be a ChatMessage')
        account_id, group_id = self._platform(message.ref.account_id, message.ref.group_id)
        message_id = platform_id(message.ref.message_id, signed=True)
        if not isinstance(message.segments, list) or any(not isinstance(segment, dict) for segment in message.segments):
            raise ValueError('invalid segments')
        occurred_at = self._number(message.occurred_at, 'occurred_at')
        expires_at = min(self._number(message.expires_at, 'expires_at'), min(occurred_at,time.time()) + min(7,self.retention['message_days']) * DAY)
        existing = self.db.execute(
            'SELECT 1 FROM messages WHERE account_id=? AND group_id=? AND message_id=?',
            (account_id, group_id, message_id)).fetchone()
        if existing is None:
            self.db.execute(
                'INSERT INTO messages (account_id, group_id, message_id, user_id, nickname, occurred_at, '
                'segments, own, recalled, expires_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)',
                (account_id, group_id, message_id, message.user_id or '', message.nickname or '',
                 occurred_at, canonical(message.segments), int(bool(message.own)),
                 int(bool(message.recalled)), expires_at))
            return True
        user = message.user_id or ''
        nickname = message.nickname or ''
        self.db.execute(
            'UPDATE messages SET recalled = recalled OR ?, '
            'segments = CASE WHEN ? THEN ? ELSE segments END, '
            "user_id = CASE WHEN ? <> '' THEN ? ELSE user_id END, "
            "nickname = CASE WHEN ? <> '' THEN ? ELSE nickname END, "
            'own = own OR ?, expires_at = MIN(expires_at, ?) '
            'WHERE account_id=? AND group_id=? AND message_id=?',
            (int(bool(message.recalled)), bool(message.segments), canonical(message.segments),
             user, user, nickname, nickname, int(bool(message.own)), expires_at,
             account_id, group_id, message_id))
        return False

    def get_message(self, ref: MessageRef, now: float | None = None) -> ChatMessage | None:
        """Return a live message for the scoped ref, or None when missing or expired."""
        account_id, group_id = self._platform(ref.account_id, ref.group_id)
        message_id = platform_id(ref.message_id, signed=True)
        row = self.db.execute(
            'SELECT user_id, nickname, occurred_at, segments, own, recalled, expires_at FROM messages '
            'WHERE account_id=? AND group_id=? AND message_id=? AND expires_at > ?',
            (account_id, group_id, message_id, self._now(now))).fetchone()
        if row is None:
            return None
        return self._message(row, account_id, group_id, message_id)

    def _message(self, row: sqlite3.Row, account_id: str, group_id: str, message_id: str) -> ChatMessage:
        return ChatMessage(
            ref=MessageRef(account_id, group_id, message_id),
            user_id=row['user_id'], nickname=row['nickname'], occurred_at=row['occurred_at'],
            segments=json.loads(row['segments']), own=bool(row['own']), recalled=bool(row['recalled']),
            expires_at=row['expires_at'])

    def recall_message(self, ref: MessageRef, now: float | None = None) -> None:
        """Mark the scoped message recalled; insert a minimal tombstone with a 3-day expiry if absent."""
        account_id, group_id = self._platform(ref.account_id, ref.group_id)
        message_id = platform_id(ref.message_id, signed=True)
        when = self._now(now)
        with self._tx():
            existing = self.db.execute(
                'SELECT 1 FROM messages WHERE account_id=? AND group_id=? AND message_id=?',
                (account_id, group_id, message_id)).fetchone()
            if existing is None:
                self.db.execute(
                    'INSERT INTO messages (account_id, group_id, message_id, user_id, nickname, occurred_at, '
                    "segments, own, recalled, expires_at) VALUES (?, ?, ?, '', '', ?, '[]', 0, 1, ?)",
                    (account_id, group_id, message_id, when, when + MESSAGE_LIFETIME))
            else:
                self.db.execute(
                    'UPDATE messages SET recalled = 1, expires_at = MIN(expires_at, ?) '
                    'WHERE account_id=? AND group_id=? AND message_id=?',
                    (when + MESSAGE_LIFETIME, account_id, group_id, message_id))

    def nearby(self, ref: MessageRef, occurred_at: float, window_seconds: float = 120,
               limit: int = 10, now: float | None = None) -> list[ChatMessage]:
        """Live neighbours of a message within the same account and group, closest first."""
        account_id, group_id = self._platform(ref.account_id, ref.group_id)
        anchor = self._number(occurred_at, 'occurred_at')
        window = self._amount(window_seconds, 'window_seconds')
        if isinstance(limit, bool) or not isinstance(limit, int) or limit < 0:
            raise ValueError('invalid limit')
        rows = self.db.execute(
            'SELECT message_id, user_id, nickname, occurred_at, segments, own, recalled, expires_at FROM messages '
            'WHERE account_id=? AND group_id=? AND expires_at > ? AND message_id != ? '
            'AND occurred_at BETWEEN ? AND ? '
            'ORDER BY ABS(occurred_at - ?), occurred_at, message_id LIMIT ?',
            (account_id, group_id, self._now(now), ref.message_id, anchor - window, anchor + window,
             anchor, limit)).fetchall()
        return [self._message(row, account_id, group_id, row['message_id']) for row in rows]

    # --------------------------------------------------------------- candidates

    def unassigned_refs(self, refs, now=None):
        """Return roots not already assigned to a durable discovery observation."""
        keys=canonical([ref.key for ref in refs])
        rows=self.db.execute(
            "SELECT DISTINCT r.value FROM candidates c, json_each(c.body,'$.trigger_refs') r "
            "WHERE c.body IS NOT NULL AND c.expires_at>? AND c.status!='bundled' "
            "AND json_extract(c.body,'$.publication.version') IS NULL "
            "AND r.value IN (SELECT value FROM json_each(?))",(self._now(now),keys)).fetchall()
        assigned={row[0] for row in rows}
        return [ref for ref in refs if ref.key not in assigned]

    def unit_history(self, unit_key, now=None):
        """Read a bounded review summary; messages and identities are not copied into summaries."""
        rows = self.db.execute(
            "SELECT c.bundle_id,c.analysis FROM candidates c "
            "WHERE c.expires_at>? AND c.analysis IS NOT NULL AND EXISTS "
            "(SELECT 1 FROM json_each(c.analysis,'$._selection.unit_keys') u WHERE u.value=?) "
            "ORDER BY c.created_at DESC LIMIT 3", (self._now(now),unit_key)).fetchall()
        statuses=self.db.execute(
            "SELECT DISTINCT d.status FROM deliveries d JOIN candidates c ON c.bundle_id=d.bundle_id "
            "WHERE d.expires_at>? AND c.body IS NOT NULL AND EXISTS "
            "(SELECT 1 FROM json_each(c.body,'$.publication.unit_keys') u WHERE u.value=?)",
            (self._now(now),unit_key)).fetchall() if rows else []
        summaries=[]
        for row in rows:
            review=json.loads(row['analysis'])['_selection']
            summaries.append({'observation_id':row['bundle_id'],'decision':review['decision'],
                              'reason':review['reason'][:600],'selected':unit_key in review.get('selected_keys',[]),
                              'delivery_statuses':sorted(item[0] for item in statuses)})
        return summaries

    def seen_publication(self, account, group, bundle, now=None):
        """Suppress a repeated image core, or a fully covered text selection, per recipient."""
        manifest = bundle.publication
        wanted = set((manifest.get('image_components', []) + manifest.get('forward_components', []))
                     or manifest.get('components', []))
        if not wanted:
            return False
        scope=(account,group,self._now(now),bundle.fingerprint)
        rows=self.db.execute(
            "SELECT DISTINCT p.value FROM deliveries d JOIN candidates c ON d.bundle_id=c.bundle_id, "
            "json_each(c.body,'$.publication.components') p "
            "WHERE d.account_id=? AND d.group_id=? AND d.expires_at>? AND c.body IS NOT NULL "
            "AND d.status IN ('queued','sending','sent','unknown','failed') AND d.fingerprint!=? "
            "AND p.value IN (SELECT value FROM json_each(?))",(*scope,canonical(sorted(wanted)))).fetchall()
        seen={row[0] for row in rows}
        if manifest.get('image_components'):
            legacy=self.db.execute(
                "SELECT DISTINCT json_extract(m.value,'$.sha256') FROM deliveries d "
                "JOIN candidates c ON d.bundle_id=c.bundle_id, json_each(c.body,'$.media') m "
                "WHERE d.account_id=? AND d.group_id=? AND d.expires_at>? AND c.body IS NOT NULL "
                "AND d.status IN ('queued','sending','sent','unknown','failed') AND d.fingerprint!=? "
                "AND json_extract(c.body,'$.publication.version') IS NULL",scope).fetchall()
            seen.update(digest(['image',row[0]]) for row in legacy if row[0])
        return wanted <= seen

    def put_candidate(self, bundle: ContentBundle, analysis: dict | None = None) -> bool:
        """Insert a content bundle; return True only when the id was new.

        The same bundle id with a different fingerprint raises StoreError; a
        repeated id with identical content never extends the stored expiry.
        """
        if not isinstance(bundle, ContentBundle):
            raise TypeError('bundle must be a ContentBundle')
        if not isinstance(bundle.bundle_id, str) or not bundle.bundle_id:
            raise ValueError('invalid bundle_id')
        if not isinstance(bundle.fingerprint, str) or not bundle.fingerprint:
            raise ValueError('invalid fingerprint')
        created_at = self._number(bundle.created_at, 'created_at')
        expires_at = min(self._number(bundle.expires_at, 'expires_at'),
                         min(created_at,min((m.occurred_at for m in bundle.messages), default=created_at)) + min(7, self.retention['content_days']) * DAY)
        bundle.expires_at = expires_at
        if analysis is not None and not isinstance(analysis, dict):
            raise TypeError('analysis must be a dict or None')
        existing = self.db.execute('SELECT fingerprint FROM candidates WHERE bundle_id=?',
                                   (bundle.bundle_id,)).fetchone()
        if existing is not None:
            if existing['fingerprint'] != bundle.fingerprint:
                raise StoreError('immutable bundle id conflict')
            return False
        media_paths = canonical([media.path for media in bundle.media if media.path])
        self.db.execute(
            'INSERT INTO candidates (bundle_id, fingerprint, body, media_paths, created_at, expires_at, status, analysis) '
            "VALUES (?, ?, ?, ?, ?, ?, 'bundled', ?)",
            (bundle.bundle_id, bundle.fingerprint, canonical(bundle), media_paths, created_at, expires_at,
             canonical(analysis) if analysis is not None else None))
        return True

    def get_candidate(self, bundle_id: str, now: float | None = None) -> dict | None:
        """Return {'bundle', 'status', 'analysis'} for a live candidate, else None."""
        row = self.db.execute(
            'SELECT body, status, analysis FROM candidates WHERE bundle_id=? AND body IS NOT NULL AND expires_at > ?',
            (bundle_id, self._now(now))).fetchone()
        if row is None:
            return None
        return self._candidate(row)

    def list_candidates(self, limit: int = 100, status: str | None = None,
                        now: float | None = None) -> list[dict]:
        """Live candidates ordered by creation time, optionally filtered by status."""
        if isinstance(limit, bool) or not isinstance(limit, int) or limit < 0:
            raise ValueError('invalid limit')
        if status is None:
            rows = self.db.execute(
                'SELECT body, status, analysis FROM candidates WHERE body IS NOT NULL AND expires_at > ? '
                'ORDER BY created_at, bundle_id LIMIT ?', (self._now(now), limit)).fetchall()
        else:
            rows = self.db.execute(
                'SELECT body, status, analysis FROM candidates WHERE body IS NOT NULL AND expires_at > ? '
                'AND status=? ORDER BY created_at, bundle_id LIMIT ?', (self._now(now), status, limit)).fetchall()
        return [self._candidate(row) for row in rows]

    @staticmethod
    def _candidate(row: sqlite3.Row) -> dict:
        return {'bundle': ContentBundle.from_dict(json.loads(row['body'])),
                'status': row['status'],
                'analysis': json.loads(row['analysis']) if row['analysis'] else {}}

    def set_candidate_status(self, bundle_id: str, status: str, analysis: dict | None = None) -> None:
        if status not in CANDIDATE_STATUSES:
            raise ValueError('invalid candidate status')
        if analysis is not None and not isinstance(analysis, dict):
            raise TypeError('analysis must be a dict or None')
        if analysis is None:
            self.db.execute('UPDATE candidates SET status=? WHERE bundle_id=?', (status, bundle_id))
        else:
            self.db.execute('UPDATE candidates SET status=?, analysis=? WHERE bundle_id=?',
                            (status, canonical(analysis), bundle_id))

    def record_review(self, bundle_id: str, record: dict) -> None:
        """Attach an independent review without replacing the immutable publication."""
        self.db.execute("UPDATE candidates SET body=json_set(body,'$.review',json(?)) "
                        "WHERE bundle_id=? AND body IS NOT NULL AND status!='cancelled'",
                        (canonical(record),bundle_id))

    def live_media_paths(self, now: float | None = None) -> set[str]:
        rows = self.db.execute(
            "SELECT media_paths FROM candidates WHERE body IS NOT NULL AND expires_at > ? AND status != 'cancelled'",
            (self._now(now),)).fetchall()
        paths: set[str] = set()
        for row in rows:
            paths.update(json.loads(row['media_paths']))
        return paths

    def cancel_candidate(self, bundle_id: str, reason: str) -> None:
        """Cancel a candidate and its queued deliveries; sent and unknown deliveries stay untouched."""
        if not isinstance(reason, str) or not reason:
            raise ValueError('invalid reason')
        with self._tx():
            self.db.execute("UPDATE candidates SET status='cancelled', cancel_reason=? WHERE bundle_id=?",
                            (reason, bundle_id))
            self.db.execute("UPDATE deliveries SET status='cancelled' WHERE bundle_id=? AND status='queued'",
                            (bundle_id,))

    # ---------------------------------------------------------------- deliveries

    def enqueue_deliveries(self, deliveries: list[dict]) -> list[bool]:
        """Commit all parts of a publication together."""
        with self._tx():
            return [self.enqueue_delivery(delivery) for delivery in deliveries]

    def enqueue_delivery(self, delivery: dict) -> bool:
        """Persist a queued delivery; return True only when inserted.

        UNIQUE(account_id, group_id, fingerprint, part_index) deduplicates: an
        existing entry is never overwritten, and a delivery id reused with
        different content raises StoreError.
        """
        if not isinstance(delivery, dict):
            raise TypeError('delivery must be a dict')
        missing = [key for key in DELIVERY_KEYS if key not in delivery]
        if missing:
            raise ValueError(f'missing delivery keys: {missing}')
        delivery_id = delivery['delivery_id']
        if not isinstance(delivery_id, str) or not delivery_id:
            raise ValueError('invalid delivery_id')
        account_id, group_id = self._platform(delivery['account_id'], delivery['group_id'])
        bundle_id = delivery['bundle_id']
        fingerprint = delivery['fingerprint']
        if not isinstance(bundle_id, str) or not bundle_id or not isinstance(fingerprint, str) or not fingerprint:
            raise ValueError('invalid bundle identity')
        part_index = delivery['part_index']
        part_total = delivery['part_total']
        if isinstance(part_index, bool) or not isinstance(part_index, int) or \
                isinstance(part_total, bool) or not isinstance(part_total, int) or \
                part_total < 1 or not 0 <= part_index < part_total:
            raise ValueError('invalid part indexes')
        nodes = delivery['nodes']
        if not isinstance(nodes, list) or any(not isinstance(node, dict) for node in nodes):
            raise ValueError('invalid nodes')
        estimated_bytes = delivery['estimated_bytes']
        node_count = delivery['node_count']
        if isinstance(estimated_bytes, bool) or not isinstance(estimated_bytes, int) or estimated_bytes < 0:
            raise ValueError('invalid estimated_bytes')
        if isinstance(node_count, bool) or not isinstance(node_count, int) or node_count < 0:
            raise ValueError('invalid node_count')
        created_at = self._number(delivery['created_at'], 'created_at')
        expires_at = min(self._number(delivery['expires_at'], 'expires_at'), created_at+min(7,self.retention['content_days'])*DAY)
        tags = delivery['tags']
        if not isinstance(tags, list) or any(not isinstance(tag, str) for tag in tags):
            raise ValueError('invalid tags')
        status = delivery.get('status', 'queued')
        if status != 'queued':
            raise ValueError('invalid delivery status')
        message_id = delivery.get('message_id')
        error_code = delivery.get('error_code')
        attempted_at = delivery.get('attempted_at')
        if message_id is not None:
            message_id = platform_id(message_id, signed=True)
        if error_code is not None and not isinstance(error_code, str):
            raise ValueError('invalid error_code')
        if attempted_at is not None:
            attempted_at = self._number(attempted_at, 'attempted_at')
        sources = '[]'
        candidate = self.db.execute('SELECT body FROM candidates WHERE bundle_id=?', (bundle_id,)).fetchone()
        if candidate is not None and candidate['body']:
            sources = canonical([asdict(ref) for ref in ContentBundle.from_dict(json.loads(candidate['body'])).sources])
        try:
            self.db.execute(
                'INSERT INTO deliveries (delivery_id, account_id, group_id, bundle_id, fingerprint, part_index, '
                'part_total, nodes, sources, estimated_bytes, node_count, expires_at, created_at, tags, status, '
                'message_id, error_code, attempted_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)',
                (delivery_id, account_id, group_id, bundle_id, fingerprint, part_index, part_total,
                 canonical(nodes), sources, estimated_bytes, node_count, expires_at, created_at,
                 canonical(tags), status, message_id, error_code, attempted_at))
        except sqlite3.IntegrityError as exc:
            existing = self.db.execute(
                'SELECT account_id, group_id, fingerprint, part_index FROM deliveries WHERE delivery_id=?',
                (delivery_id,)).fetchone()
            if existing is not None and (existing['account_id'], existing['group_id'],
                                         existing['fingerprint'], existing['part_index']) != \
                    (account_id, group_id, fingerprint, part_index):
                raise StoreError('immutable delivery id conflict') from exc
            return False
        return True

    @staticmethod
    def _delivery_dict(row: sqlite3.Row) -> dict:
        return {
            'delivery_id': row['delivery_id'], 'account_id': row['account_id'], 'group_id': row['group_id'],
            'bundle_id': row['bundle_id'], 'fingerprint': row['fingerprint'],
            'part_index': row['part_index'], 'part_total': row['part_total'],
            'nodes': json.loads(row['nodes']), 'estimated_bytes': row['estimated_bytes'],
            'node_count': row['node_count'], 'expires_at': row['expires_at'], 'created_at': row['created_at'],
            'tags': json.loads(row['tags']), 'status': row['status'], 'message_id': row['message_id'],
            'error_code': row['error_code'], 'attempted_at': row['attempted_at'],
        }

    def cancel_delivery(self, delivery_id: str, reason: str) -> None:
        self.db.execute("UPDATE deliveries SET status='cancelled', error_code=? WHERE delivery_id=? AND status IN ('queued','failed')",
                        (reason,delivery_id))

    def get_delivery(self, delivery_id: str) -> dict | None:
        row = self.db.execute('SELECT * FROM deliveries WHERE delivery_id=?', (delivery_id,)).fetchone()
        return None if row is None else self._delivery_dict(row)

    def deliveries(self, status: str | None = None, group_id: str | None = None,
                   now: float | None = None) -> list[dict]:
        """Live deliveries ordered by created_at and part_index, optionally filtered."""
        clauses = ['expires_at > ?']
        params: list[Any] = [self._now(now)]
        if status is not None:
            clauses.append('status = ?')
            params.append(status)
        if group_id is not None:
            clauses.append('group_id = ?')
            params.append(group_id)
        rows = self.db.execute(
            f'SELECT * FROM deliveries WHERE {" AND ".join(clauses)} ORDER BY created_at, part_index',
            params).fetchall()
        return [self._delivery_dict(row) for row in rows]

    def delivery_for_message(self, ref: MessageRef) -> dict | None:
        account_id, group_id = self._platform(ref.account_id, ref.group_id)
        message_id = platform_id(ref.message_id, signed=True)
        row = self.db.execute(
            'SELECT * FROM deliveries WHERE account_id=? AND group_id=? AND message_id=?',
            (account_id, group_id, message_id)).fetchone()
        return None if row is None else self._delivery_dict(row)

    def claim_delivery(self, delivery_id: str, limits: dict, now: float | None = None) -> dict | None:
        """Atomically claim a queued delivery, or return None when a rule blocks it.

        All checks and quota reservations run in one BEGIN IMMEDIATE transaction:
        live queued state, pause scopes, earlier parts of the same bundle sent,
        per-scope minimum spacing, and the daily
        group batches/nodes plus global publish bytes budgets.
        """
        when = self._now(now)
        self._check_limits(limits)
        try:
            with self._tx():
                row = self.db.execute('SELECT * FROM deliveries WHERE delivery_id=?', (delivery_id,)).fetchone()
                if row is None or row['status'] != 'queued' or row['expires_at'] <= when:
                    raise _Blocked()
                account_id, group_id = row['account_id'], row['group_id']
                parts = self.db.execute('SELECT COUNT(*) FROM deliveries WHERE account_id=? AND group_id=? AND fingerprint=?',
                                        (account_id, group_id, row['fingerprint'])).fetchone()[0]
                if parts != row['part_total']:
                    raise _Blocked()
                if self._paused(account_id, group_id):
                    raise _Blocked()
                blocked = self.db.execute(
                    'SELECT 1 FROM deliveries WHERE account_id=? AND group_id=? AND fingerprint=? '
                    "AND part_index < ? AND status != 'sent' LIMIT 1",
                    (account_id, group_id, row['fingerprint'], row['part_index'])).fetchone()
                if blocked is not None:
                    raise _Blocked()
                if limits['min_interval_seconds'] > 0:
                    last = self.db.execute(
                        'SELECT MAX(attempted_at) AS last_attempted FROM deliveries WHERE account_id=? AND group_id=?',
                        (account_id, group_id)).fetchone()['last_attempted']
                    if last is not None and when - last < limits['min_interval_seconds']:
                        raise _Blocked()
                day = self._utc_day(when)
                scope = f'{account_id}:{group_id}'
                reserved = (self._reserve(day, scope, 'batches', 1, limits['daily_batches'])
                            and self._reserve(day, scope, 'nodes', row['node_count'], limits['daily_segments'])
                            and self._reserve(day, 'global', 'publish_bytes', row['estimated_bytes'],
                                              limits['daily_publish_bytes']))
                if not reserved:
                    raise _Blocked()
                self.db.execute(
                    "UPDATE deliveries SET status='sending', attempted_at=?, charged_day=?, "
                    'charged_batches=1, charged_nodes=?, charged_bytes=? WHERE delivery_id=?',
                    (when, day, row['node_count'], row['estimated_bytes'], delivery_id))
                return self.get_delivery(delivery_id)
        except _Blocked:
            return None

    def finish_delivery(self, delivery_id: str, status: str, message_id: str | None = None,
                        error_code: str | None = None) -> None:
        """Record the terminal outcome of a sending delivery and settle its quotas."""
        if status not in {'sent', 'failed', 'unknown', 'cancelled'}:
            raise ValueError('invalid delivery status')
        if status == 'sent':
            if not message_id:
                raise ValueError('sent requires a receipt message id')
            message_id = platform_id(message_id, signed=True)
        if error_code is not None and not isinstance(error_code, str):
            raise ValueError('invalid error_code')
        with self._tx():
            row = self.db.execute('SELECT * FROM deliveries WHERE delivery_id=?', (delivery_id,)).fetchone()
            if row is None or row['status'] != 'sending':
                return
            self.db.execute(
                'UPDATE deliveries SET status=?, message_id=COALESCE(?, message_id), error_code=COALESCE(?, error_code) '
                'WHERE delivery_id=?', (status, message_id, error_code, delivery_id))
            self._settle_attempt(row, charged=status in {'sent', 'failed', 'unknown'})

    def confirm_delivery(self, delivery_id: str, message_id: str) -> None:
        """Confirm an unknown delivery as sent with a receipt."""
        message_id = platform_id(message_id, signed=True)
        with self._tx():
            cursor = self.db.execute("UPDATE deliveries SET status='sent', message_id=? "
                                     "WHERE delivery_id=? AND status='unknown'", (message_id, delivery_id))
            if cursor.rowcount != 1:
                raise StoreError('only an unknown delivery can be confirmed')

    def retry_delivery(self, delivery_id: str, now: float | None = None) -> None:
        """Return an unexpired failed delivery to the queue; a later claim charges quotas again."""
        cursor = self.db.execute(
            "UPDATE deliveries SET status='queued', message_id=NULL, error_code=NULL "
            "WHERE delivery_id=? AND status='failed' AND expires_at > ?",
            (delivery_id, self._now(now)))
        if cursor.rowcount != 1:
            raise StoreError('only a live failed delivery can be retried')

    def recover_inflight(self) -> int:
        """Mark sending deliveries unknown (call after taking the runtime exclusive lock)."""
        with self._tx():
            rows = self.db.execute("SELECT * FROM deliveries WHERE status='sending'").fetchall()
            for row in rows:
                self._settle_attempt(row, charged=True)
            return self.db.execute("UPDATE deliveries SET status='unknown' WHERE status='sending'").rowcount

    def _settle_attempt(self, row: sqlite3.Row, charged: bool) -> None:
        if row['charged_day'] is None or not row['charged_batches']:
            return
        actual = (row['charged_batches'], row['charged_nodes'], row['charged_bytes']) if charged else (0, 0, 0)
        scope = f"{row['account_id']}:{row['group_id']}"
        self._settle(row['charged_day'], scope, 'batches', row['charged_batches'], actual[0])
        self._settle(row['charged_day'], scope, 'nodes', row['charged_nodes'], actual[1])
        self._settle(row['charged_day'], 'global', 'publish_bytes', row['charged_bytes'], actual[2])

    def _check_limits(self, limits: dict) -> None:
        if not isinstance(limits, dict):
            raise ValueError('invalid limits')
        missing = [key for key in LIMIT_KEYS if key not in limits]
        if missing:
            raise ValueError(f'missing limits: {missing}')
        for key in ('daily_batches', 'daily_segments'):
            value = limits[key]
            if isinstance(value, bool) or not isinstance(value, int) or value < 0:
                raise ValueError(f'invalid limits.{key}')
        for key in ('min_interval_seconds', 'daily_publish_bytes'):
            self._amount(limits[key], f'limits.{key}')

    # ----------------------------------------------------------------- feedback

    def put_feedback(self, signal: dict) -> bool:
        """Persist a feedback signal; duplicates are idempotent.

        A repeated signal id with a changed active flag only updates the flag,
        without adding another signal row.
        """
        if not isinstance(signal, dict):
            raise TypeError('signal must be a dict')
        missing = [key for key in FEEDBACK_KEYS if key not in signal]
        if missing:
            raise ValueError(f'missing feedback keys: {missing}')
        signal_id = signal['signal_id']
        if not isinstance(signal_id, str) or not signal_id:
            raise ValueError('invalid signal_id')
        account_id, group_id = self._platform(signal['account_id'], signal['group_id'])
        for key in ('bundle_id', 'publication_id'):
            value = signal[key]
            if value is not None and (not isinstance(value, str) or not value):
                raise ValueError(f'invalid {key}')
        user_id = signal['user_id']
        kind = signal['kind']
        if not isinstance(user_id, str) or not user_id or not isinstance(kind, str) or not kind:
            raise ValueError('invalid feedback identity')
        direction = signal['direction']
        if direction not in FEEDBACK_DIRECTIONS:
            raise ValueError('invalid feedback direction')
        tags = signal['tags']
        if not isinstance(tags, list) or any(not isinstance(tag, str) for tag in tags):
            raise ValueError('invalid tags')
        occurred_at = self._number(signal['occurred_at'], 'occurred_at')
        expires_at = min(self._number(signal['expires_at'], 'expires_at'), occurred_at + min(7, self.retention['content_days']) * DAY)
        payload = signal['payload']
        if not isinstance(payload, dict):
            raise ValueError('invalid payload')
        active = int(bool(signal.get('active', True)))
        try:
            self.db.execute(
                'INSERT INTO feedback (signal_id, account_id, group_id, bundle_id, publication_id, user_id, kind, '
                'direction, tags, occurred_at, expires_at, payload, active) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)',
                (signal_id, account_id, group_id, signal['bundle_id'], signal['publication_id'], user_id, kind,
                 direction, canonical(tags), occurred_at, expires_at, canonical(payload), active))
        except sqlite3.IntegrityError:
            self.db.execute('UPDATE feedback SET active=? WHERE signal_id=? AND active != ?',
                            (active, signal_id, active))
            return False
        return True

    @staticmethod
    def _feedback_dict(row: sqlite3.Row) -> dict:
        result=dict(row)
        result['tags']=json.loads(result['tags'])
        result['payload']=json.loads(result['payload'])
        result['active']=bool(result['active'])
        return result

    def get_feedback(self, signal_id: str, now: float | None = None) -> dict | None:
        row=self.db.execute('SELECT * FROM feedback WHERE signal_id=? AND expires_at>?',
                            (signal_id,self._now(now))).fetchone()
        return self._feedback_dict(row) if row else None

    def pending_feedback(self, account_id: str, groups: set[str], *, now: float | None = None, limit: int = 200) -> list[dict]:
        if not groups: return []
        allowed=sorted(groups); placeholders=','.join('?' for _ in allowed)
        rows=self.db.execute("SELECT * FROM feedback WHERE account_id=? AND active=1 AND expires_at>? "
                             "AND json_extract(payload,'$.processing')='pending' "
                             f"AND group_id IN ({placeholders}) ORDER BY occurred_at,signal_id LIMIT ?",
                             (account_id,self._now(now),*allowed,max(1,min(200,limit)))).fetchall()
        return [self._feedback_dict(row) for row in rows]

    def complete_feedback(self, signal_id: str, *, payload: dict, bundle_id: str | None = None,
                          publication_id: str | None = None, direction: str = 'unknown',
                          tags: list[str] | None = None, expires_at: float | None = None,
                          now: float | None = None) -> dict | None:
        if direction not in FEEDBACK_DIRECTIONS or not isinstance(payload,dict):
            raise ValueError('invalid feedback result')
        if tags is not None and (not isinstance(tags,list) or any(not isinstance(tag,str) for tag in tags)):
            raise ValueError('invalid feedback tags')
        current=self.get_feedback(signal_id,now=now)
        if current is None: return None
        expiry=current['expires_at'] if expires_at is None else min(current['expires_at'],self._number(expires_at,'expires_at'))
        self.db.execute('UPDATE feedback SET bundle_id=COALESCE(?,bundle_id),publication_id=COALESCE(?,publication_id),'
                        'direction=?,tags=?,payload=?,expires_at=? WHERE signal_id=?',
                        (bundle_id,publication_id,direction,canonical(tags if tags is not None else current['tags']),
                         canonical(payload),expiry,signal_id))
        return self.get_feedback(signal_id,now=now)

    def feedback_since(self, account_id: str, group_id: str, since: float,
                       now: float | None = None) -> list[dict]:
        """Live active feedback for the scope with occurred_at >= since."""
        account_id, group_id = self._platform(account_id, group_id)
        since_value = self._number(since, 'since')
        rows = self.db.execute(
            'SELECT signal_id, bundle_id, publication_id, user_id, kind, direction, tags, occurred_at, '
            'expires_at, payload FROM feedback '
            'WHERE account_id=? AND group_id=? AND active=1 AND expires_at > ? AND occurred_at >= ? '
            'ORDER BY occurred_at, signal_id',
            (account_id, group_id, self._now(now), since_value)).fetchall()
        return [{'signal_id': row['signal_id'], 'account_id': account_id, 'group_id': group_id,
                 'bundle_id': row['bundle_id'], 'publication_id': row['publication_id'],
                 'user_id': row['user_id'], 'kind': row['kind'], 'direction': row['direction'],
                 'tags': json.loads(row['tags']), 'occurred_at': row['occurred_at'],
                 'expires_at': row['expires_at'], 'payload': json.loads(row['payload']), 'active': True}
                for row in rows]

    # ----------------------------------------------------------------- group state

    def group_state(self, account_id: str, group_id: str) -> dict:
        row = self.db.execute(
            'SELECT paused, preferences, version, snapshots, last_learning_at, last_cleanup_at FROM group_state '
            'WHERE account_id=? AND group_id=?', (account_id, group_id)).fetchone()
        state = {'account_id': account_id, 'group_id': group_id, 'paused': False, 'preferences': {},
                 'version': 0, 'snapshots': [], 'last_learning_at': 0.0, 'last_cleanup_at': 0.0}
        if row is not None:
            state.update({'paused': bool(row['paused']), 'preferences': json.loads(row['preferences']),
                          'version': row['version'], 'snapshots': json.loads(row['snapshots']),
                          'last_learning_at': row['last_learning_at'], 'last_cleanup_at': row['last_cleanup_at']})
        return state

    def set_group_state(self, account_id: str, group_id: str, values: dict) -> None:
        """Merge the provided state fields into the scoped group state."""
        updates: dict[str, Any] = {}
        for key in GROUP_STATE_FIELDS:
            if key not in values:
                continue
            value = values[key]
            if key == 'paused':
                updates[key] = int(bool(value))
            elif key == 'preferences':
                if not isinstance(value, dict):
                    raise ValueError('invalid preferences')
                updates[key] = canonical(value)
            elif key == 'version':
                if isinstance(value, bool) or not isinstance(value, int):
                    raise ValueError('invalid version')
                updates[key] = value
            elif key == 'snapshots':
                if not isinstance(value, list):
                    raise ValueError('invalid snapshots')
                updates[key] = canonical(value)
            else:
                updates[key] = self._number(value, key)
        if not updates:
            return
        with self._tx():
            existing = self.db.execute('SELECT 1 FROM group_state WHERE account_id=? AND group_id=?',
                                       (account_id, group_id)).fetchone()
            if existing is None:
                columns = ', '.join(updates)
                placeholders = ', '.join('?' for _ in updates)
                self.db.execute(
                    f'INSERT INTO group_state (account_id, group_id, {columns}) VALUES (?, ?, {placeholders})',
                    (account_id, group_id, *updates.values()))
            else:
                assignments = ', '.join(f'{key} = ?' for key in updates)
                self.db.execute(f'UPDATE group_state SET {assignments} WHERE account_id=? AND group_id=?',
                                (*updates.values(), account_id, group_id))

    def paused(self, account_id: str, group_id: str) -> bool:
        """Pause flag combining the ('*','*'), (account_id,'*') and (account_id,group_id) scopes."""
        return self._paused(account_id, group_id)

    def _paused(self, account_id: str, group_id: str) -> bool:
        rows = self.db.execute(
            "SELECT paused FROM group_state WHERE (account_id='*' AND group_id='*') "
            "OR (account_id=? AND group_id='*') OR (account_id='*' AND group_id=?) OR (account_id=? AND group_id=?)",
            (account_id, group_id, account_id, group_id)).fetchall()
        return any(bool(row['paused']) for row in rows)

    def set_paused(self, account_id: str, group_id: str, paused: bool) -> None:
        self.db.execute(
            'INSERT INTO group_state (account_id, group_id, paused) VALUES (?, ?, ?) '
            'ON CONFLICT (account_id, group_id) DO UPDATE SET paused = excluded.paused',
            (account_id, group_id, int(bool(paused))))

    # --------------------------------------------------------------------- budgets

    def reserve_budget(self, day: str, scope: str, metric: str, amount: float, limit: float) -> bool:
        """Atomically reserve an amount when used + reserved + amount stays within the limit."""
        day = self._day(day)
        self._scope_metric(scope, metric)
        amount = self._amount(amount, 'amount')
        limit = self._amount(limit, 'limit')
        with self._tx():
            return self._reserve(day, scope, metric, amount, limit)

    def _reserve(self, day: str, scope: str, metric: str, amount: float, limit: float) -> bool:
        row = self.db.execute('SELECT used, reserved FROM daily_budgets WHERE day=? AND scope=? AND metric=?',
                              (day, scope, metric)).fetchone()
        used = row['used'] if row is not None else 0.0
        reserved = row['reserved'] if row is not None else 0.0
        if used + reserved + amount > limit:
            return False
        self.db.execute(
            'INSERT INTO daily_budgets (day, scope, metric, used, reserved) VALUES (?, ?, ?, 0, ?) '
            'ON CONFLICT (day, scope, metric) DO UPDATE SET reserved = reserved + excluded.reserved',
            (day, scope, metric, amount))
        return True

    def settle_budget(self, day: str, scope: str, metric: str, reserved: float, actual: float) -> None:
        """Release exactly the own reservation and charge the conservative actual part."""
        day = self._day(day)
        self._scope_metric(scope, metric)
        reserved = self._amount(reserved, 'reserved')
        actual = self._amount(actual, 'actual')
        with self._tx():
            self._settle(day, scope, metric, reserved, actual)

    def _settle(self, day: str, scope: str, metric: str, reserved: float, actual: float) -> None:
        row = self.db.execute('SELECT used, reserved FROM daily_budgets WHERE day=? AND scope=? AND metric=?',
                              (day, scope, metric)).fetchone()
        if row is None:
            return
        release = min(reserved, row['reserved'])
        charge = min(actual, reserved)
        self.db.execute('UPDATE daily_budgets SET reserved=reserved-?, used=used+? WHERE day=? AND scope=? AND metric=?',
                        (release, charge, day, scope, metric))

    def budget(self, day: str, scope: str, metric: str) -> dict:
        day = self._day(day)
        self._scope_metric(scope, metric)
        row = self.db.execute('SELECT used, reserved FROM daily_budgets WHERE day=? AND scope=? AND metric=?',
                              (day, scope, metric)).fetchone()
        if row is None:
            return {'used': 0.0, 'reserved': 0.0}
        return {'used': float(row['used']), 'reserved': float(row['reserved'])}

    def budget_summary(self, day: str) -> list[dict]:
        day = self._day(day)
        rows = self.db.execute(
            'SELECT scope, metric, used, reserved FROM daily_budgets WHERE day=? ORDER BY scope, metric',
            (day,)).fetchall()
        return [{'day': day, 'scope': row['scope'], 'metric': row['metric'],
                 'used': float(row['used']), 'reserved': float(row['reserved'])} for row in rows]

    # --------------------------------------------------------------------- summary

    def summary(self) -> dict:
        return {table: self.db.execute(f'SELECT COUNT(*) AS count FROM {table}').fetchone()['count']
                for table in TABLES}

    # --------------------------------------------------------------------- cleanup

    def cleanup(self, now: float | None = None, compact: bool = False) -> dict:
        """Delete expired content and old rows in one transaction; optionally reclaim space.

        Expired queued deliveries become cancelled; every expired delivery loses
        nodes, tags, sources and errors while receipts, fingerprints and statuses
        survive until the 90 day retention. Compact runs an incremental vacuum
        and a TRUNCATE checkpoint and reports lock busyness for retry.
        """
        when = self._now(now)
        bytes_before = self.path.stat().st_size
        result: dict[str, Any] = {'busy': False}
        with self._tx():
            result['messages'] = self.db.execute('DELETE FROM messages WHERE expires_at <= ?', (when,)).rowcount
            result['candidates'] = self.db.execute('DELETE FROM candidates WHERE expires_at <= ?', (when,)).rowcount
            result['feedback'] = self.db.execute('DELETE FROM feedback WHERE expires_at <= ?', (when,)).rowcount
            expired = self.db.execute('SELECT delivery_id, status FROM deliveries WHERE expires_at <= ?',
                                      (when,)).fetchall()
            cancelled = 0
            for row in expired:
                if row['status'] == 'queued':
                    cancelled += 1
                self.db.execute(
                    "UPDATE deliveries SET status=CASE WHEN status='queued' THEN 'cancelled' ELSE status END, "
                    "nodes='[]', tags='[]', sources='[]', error_code=NULL WHERE delivery_id=?",
                    (row['delivery_id'],))
            result['deliveries_cancelled'] = cancelled
            result['deliveries_erased'] = len(expired)
            result['deliveries_removed'] = self.db.execute(
                'DELETE FROM deliveries WHERE created_at <= ?', (when - self.retention['delivery_days'] * DAY,)).rowcount
            result['budgets'] = self.db.execute(
                'DELETE FROM daily_budgets WHERE day < ?',
                (self._utc_day(when - self.retention['budget_days'] * DAY),)).rowcount
        if compact:
            result['busy'] = self._compact()
        result['wal_bytes_after'] = Path(str(self.path)+'-wal').stat().st_size if Path(str(self.path)+'-wal').exists() else 0
        result['bytes_before'] = bytes_before
        result['bytes_after'] = self.path.stat().st_size
        return result

    def _compact(self) -> bool:
        busy = False
        try:
            self.db.execute('PRAGMA wal_checkpoint(PASSIVE)')
            deadline = time.monotonic()+5
            while time.monotonic()<deadline:
                free = self.db.execute('PRAGMA freelist_count').fetchone()[0]
                if free == 0:
                    break
                self.db.execute('PRAGMA incremental_vacuum')
            busy = busy or self.db.execute('PRAGMA freelist_count').fetchone()[0] != 0
        except sqlite3.OperationalError as exc:
            if 'locked' in str(exc) or 'busy' in str(exc):
                busy = True
            else:
                raise
        try:
            checkpoint = self.db.execute('PRAGMA wal_checkpoint(TRUNCATE)').fetchone()
            busy = busy or bool(checkpoint[0])
        except sqlite3.OperationalError as exc:
            if 'locked' in str(exc) or 'busy' in str(exc):
                busy = True
            else:
                raise
        return busy
