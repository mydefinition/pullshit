import unittest
import sqlite3
from datetime import datetime, timedelta, timezone
from dataclasses import replace
from unittest.mock import patch

from pullshit.adapters.qq.events import normalize, segments
from pullshit.storage.store import MIGRATION_1, MIGRATION_2, SCHEMA_3, Store, StoreError
from support import Workspace, message, request


class StoreTests(Workspace, unittest.TestCase):
    def setUp(self):
        super().setUp()
        self.path = self.root / "data/development/test.sqlite3"
        self.store = Store(self.path)
        self.addCleanup(self.store.close)

    def event(self, raw, account="900001", sources=None, targets=None):
        return normalize(raw, account, sources or {"900101"}, targets or {"900201"})

    def test_event_deduplication_and_scoped_ids_survive_restart(self):
        event = self.event(message())
        self.assertTrue(self.store.ingest(event))
        self.assertFalse(self.store.ingest(event))
        self.assertTrue(self.store.ingest(self.event(message(group="900201"))))
        other = self.event(message(account=900002), account="900002")
        self.assertTrue(self.store.ingest(other))
        restarted = Store(self.path)
        try:
            self.assertEqual(restarted.summary()["messages"], 3)
            self.assertIsNotNone(restarted.message("900001", "900101", "1"))
            self.assertIsNone(restarted.message("900001", "900999", "1"))
        finally:
            restarted.close()

    def test_unauthorized_groups_accounts_and_own_messages(self):
        self.assertIsNone(self.event(message(group="900999")))
        self.assertIsNone(self.event(message(account=900002)))
        own = self.event(message(user=900001))
        self.store.ingest(own)
        self.assertEqual(self.store.message("900001", "900101", "1")["own_message"], 1)

    def test_recall_before_message_and_feedback_group_attribution(self):
        recall = {"post_type": "notice", "notice_type": "group_recall", "self_id": 900001,
                  "group_id": 900101, "message_id": 1, "user_id": 900401, "time": 1791590001}
        self.store.ingest(self.event(recall))
        self.store.ingest(self.event(message()))
        self.assertEqual(self.store.message("900001", "900101", "1")["recalled"], 1)
        req = request()
        self.store.begin_probe("900001", req, 5)
        self.store.finish_part("900001", req["probe_id"], 0, "sent", "77")
        reply = message(group="900201", message_id=2, content=[{"type": "reply", "data": {"id": 77}}, {"type": "text", "data": {"text": "ha"}}])
        self.store.ingest(self.event(reply))
        self.store.ingest(self.event(message(message_id=3, content=reply["message"])))
        rows = self.store.db.execute("SELECT group_id,probe_id FROM feedback ORDER BY group_id").fetchall()
        self.assertEqual([(r[0], r[1]) for r in rows], [("900101", None), ("900201", req["probe_id"])])

    def test_quota_reserved_atomically_and_duplicate_probe_rejected(self):
        req = request(2)
        self.assertTrue(self.store.begin_probe("900001", req, 3))
        self.assertFalse(self.store.begin_probe("900001", req, 3))
        other = request(2)
        other["probe_id"] = "other"
        with self.assertRaises(StoreError):
            self.store.begin_probe("900001", other, 3)
        req["target_group_id"] = "900999"
        with self.assertRaises(StoreError):
            self.store.begin_probe("900001", req, 5)

    def test_interrupted_probe_and_expired_raw_data(self):
        req = request(2)
        self.store.begin_probe("900001", req, 5)
        self.store.finish_part("900001", req["probe_id"], 0, "sent", "77")
        self.store.recover_probe("900001", req["probe_id"])
        status = self.store.probe("900001", req["probe_id"])
        self.assertEqual(status["status"], "unknown")
        self.assertEqual([p["status"] for p in status["parts"]], ["sent", "unknown"])
        self.store.ingest(replace(self.event(message()), received_at="2000-01-01T00:00:00.000000Z"))
        self.assertEqual(self.store.prune(7), 1)
        self.assertEqual(self.store.summary()["messages"], 0)

    def test_forward_limits_missing_context_and_segment_order(self):
        content = [{"type": "node", "data": {"user_id": 900401, "content": [{"type": "node", "data": {"content": [{"type": "text", "data": {"text": "deep"}}]}}]}}]
        normalized, issues = segments(content, max_depth=1)
        self.assertIn("forward_depth_limit", issues)
        self.assertEqual(normalized[0]["data"]["user_id"], "900401")
        _, issues = segments([{"type": "text", "data": {"text": str(i)}} for i in range(10)], max_nodes=2)
        self.assertIn("segment_node_limit", issues)
        result, issues = segments([{ "type": "forward", "data": {"id": "ref"}}, {"type": "image", "data": {"file": "x"}}])
        self.assertEqual([x["type"] for x in result], ["forward", "image"])
        self.assertIn("forward_not_fetched", issues)

    def test_single_autoincrement_keys_and_foreign_keys_disabled(self):
        self.assertEqual(self.store.db.execute("PRAGMA foreign_keys").fetchone()[0], 0)
        for table in SCHEMA_3:
            columns = self.store.db.execute(f"PRAGMA table_info({table})").fetchall()
            self.assertEqual([row[1] for row in columns if row[5]], ["id"])
            self.assertEqual(self.store.db.execute(f"PRAGMA foreign_key_list({table})").fetchall(), [])
        self.store.set_paused("900001", True)
        first = self.store.db.execute("SELECT id FROM control").fetchone()[0]
        self.store.db.execute("DELETE FROM control")
        self.store.set_paused("900002", True)
        self.assertGreater(self.store.db.execute("SELECT id FROM control").fetchone()[0], first)

    def test_v2_migration_preserves_all_tables_and_associations(self):
        path = self.path.with_name("v2.sqlite3")
        db = sqlite3.connect(path)
        db.executescript(MIGRATION_1 + MIGRATION_2)
        db.execute("INSERT INTO raw_events VALUES(41,'key','900001','900101','message.group.normal',1,'2026-10-12T00:00:00Z','{}')")
        db.execute("INSERT INTO messages VALUES('900001','900101','99',41,'900401',0,'[]','[]',1)")
        db.execute("INSERT INTO probes VALUES('900001','probe','900101','900201','900301','hash','sent','date','date')")
        db.execute("INSERT INTO probe_parts VALUES('900001','probe',0,'sent','99',NULL)")
        db.execute("INSERT INTO feedback VALUES(41,'900001','900101','99','reply','probe')")
        db.execute("INSERT INTO connection_gaps VALUES(9,'900001','date',NULL,'startup')")
        db.execute("INSERT INTO control VALUES('*',1)")
        db.execute("INSERT INTO context_cache VALUES('900001','900101','message','99','found','{}',NULL,'date')")
        old = {table: (list(row[1] for row in db.execute(f"PRAGMA table_info({table})")),
                       db.execute(f"SELECT * FROM {table}").fetchall()) for table in SCHEMA_3}
        db.execute("PRAGMA user_version=2")
        db.commit(); db.close()
        migrated = Store(path)
        try:
            self.assertEqual(migrated.db.execute("PRAGMA user_version").fetchone()[0], 3)
            for table, (columns, rows) in old.items():
                result = migrated.db.execute(f"SELECT {','.join(columns)} FROM {table}").fetchall()
                self.assertEqual([tuple(row) for row in result], rows)
            self.assertTrue(migrated.paused("900001"))
            self.assertTrue(migrated.ingest(self.event(message(message_id=100))))
            self.assertGreater(migrated.db.execute("SELECT max(id) FROM raw_events").fetchone()[0], 41)
        finally:
            migrated.close()

    def test_failed_migration_rolls_back_all_changes(self):
        path = self.path.with_name("invalid-v2.sqlite3")
        db = sqlite3.connect(path)
        db.executescript(MIGRATION_1 + MIGRATION_2)
        db.execute("INSERT INTO control VALUES(NULL,1)")
        db.execute("PRAGMA user_version=2")
        db.commit(); db.close()
        with self.assertRaises(sqlite3.IntegrityError):
            Store(path)
        db = sqlite3.connect(path)
        try:
            self.assertEqual(db.execute("PRAGMA user_version").fetchone()[0], 2)
            self.assertEqual(db.execute("SELECT * FROM control").fetchall(), [(None, 1)])
            self.assertFalse(db.execute("SELECT name FROM sqlite_master WHERE name LIKE '%_v3'").fetchall())
        finally:
            db.close()

    def test_retention_cleans_associations_and_uses_failure_cache_ttl(self):
        content = [{"type": "reply", "data": {"id": 77}}]
        self.store.ingest(replace(self.event(message(content=content)), received_at="2000-01-01T00:00:00.000000Z"))
        self.store.ingest(self.event(message(message_id=2)))
        self.store.begin_probe("900001", request(), 5)
        self.store.set_paused("*", True)
        for reference, status in (("old-found", "found"), ("failed", "unavailable"), ("fresh", "found")):
            self.store.cache_context("900001", "900101", "message", reference, status, {})
        self.store.db.execute("UPDATE context_cache SET checked_at='2000-01-01T00:00:00.000000Z' WHERE reference_id='old-found'")
        failed_at = (datetime.now(timezone.utc) - timedelta(seconds=61)).isoformat().replace("+00:00", "Z")
        self.store.db.execute("UPDATE context_cache SET checked_at=? WHERE reference_id='failed'", (failed_at,))
        self.assertEqual(self.store.prune(7), 1)
        self.assertEqual(self.store.summary()["messages"], 1)
        self.assertEqual(self.store.summary()["feedback"], 0)
        self.assertEqual(self.store.summary()["probes"], 1)
        self.assertTrue(self.store.paused("900001"))
        self.assertEqual([r[0] for r in self.store.db.execute("SELECT reference_id FROM context_cache")], ["fresh"])

    def test_cleanup_failure_rolls_back_associated_deletes(self):
        self.store.ingest(replace(self.event(message(content=[{"type": "reply", "data": {"id": 77}}])),
                                  received_at="2000-01-01T00:00:00.000000Z"))
        self.store.cache_context("900001", "900101", "message", "old", "found", {})
        self.store.db.execute("UPDATE context_cache SET checked_at='2000-01-01T00:00:00.000000Z'")
        self.store.db.execute("CREATE TRIGGER block_delete BEFORE DELETE ON raw_events BEGIN SELECT RAISE(ABORT,'blocked'); END")
        with self.assertRaises(sqlite3.IntegrityError):
            self.store.prune(7)
        self.assertEqual(self.store.summary()["messages"], 1)
        self.assertEqual(self.store.summary()["feedback"], 1)
        self.assertEqual(self.store.db.execute("SELECT count(*) FROM context_cache").fetchone()[0], 1)

    def test_compact_reclaims_disk_and_preserves_surviving_ids(self):
        for number in range(1, 31):
            event = self.event(message(message_id=number, content=[{"type": "text", "data": {"text": "x" * 50000}}]))
            self.store.ingest(replace(event, received_at="2000-01-01T00:00:00.000000Z"))
        self.store.ingest(self.event(message(message_id=31)))
        survivor = self.store.message("900001", "900101", "31")["id"]
        pages_before = self.store.db.execute("PRAGMA page_count").fetchone()[0]
        self.store.prune(7)
        self.assertEqual(self.store.compact(), {"database_compacted": True, "wal_truncated": True})
        self.assertLess(self.store.db.execute("PRAGMA page_count").fetchone()[0], pages_before)
        self.assertEqual(self.store.message("900001", "900101", "31")["id"], survivor)
        self.assertEqual(self.path.stat().st_size,
                         self.store.db.execute("PRAGMA page_count").fetchone()[0] * self.store.db.execute("PRAGMA page_size").fetchone()[0])
