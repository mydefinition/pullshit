"""SQLite migrations, atomic event ingestion and conservative send bookkeeping."""

from __future__ import annotations

import hashlib
import json
import os
import sqlite3
from datetime import datetime, timedelta, timezone
from pathlib import Path

from pullshit.adapters.qq.events import Event, canonical, utc_now


class StoreError(ValueError):
    pass


MIGRATION_1 = """
CREATE TABLE raw_events (
  id INTEGER PRIMARY KEY, event_key TEXT NOT NULL UNIQUE,
  account_id TEXT NOT NULL, group_id TEXT NOT NULL, event_type TEXT NOT NULL,
  occurred_at INTEGER NOT NULL, received_at TEXT NOT NULL, payload TEXT NOT NULL
);
CREATE TABLE messages (
  account_id TEXT NOT NULL, group_id TEXT NOT NULL, message_id TEXT NOT NULL,
  event_id INTEGER NOT NULL REFERENCES raw_events(id) ON DELETE CASCADE,
  user_id TEXT NOT NULL, own_message INTEGER NOT NULL, segments TEXT NOT NULL,
  issues TEXT NOT NULL, recalled INTEGER NOT NULL DEFAULT 0,
  PRIMARY KEY(account_id, group_id, message_id)
);
CREATE TABLE probes (
  account_id TEXT NOT NULL, probe_id TEXT NOT NULL, source_group_id TEXT NOT NULL,
  target_group_id TEXT NOT NULL, operator_id TEXT NOT NULL, content_hash TEXT NOT NULL,
  status TEXT NOT NULL CHECK(status IN ('sending','sent','failed','unknown','cancelled')),
  created_at TEXT NOT NULL, updated_at TEXT NOT NULL,
  PRIMARY KEY(account_id, probe_id)
);
CREATE TABLE probe_parts (
  account_id TEXT NOT NULL, probe_id TEXT NOT NULL, part INTEGER NOT NULL,
  status TEXT NOT NULL CHECK(status IN ('sending','sent','failed','unknown','cancelled')),
  message_id TEXT, error_code TEXT,
  PRIMARY KEY(account_id, probe_id, part),
  FOREIGN KEY(account_id, probe_id) REFERENCES probes(account_id, probe_id)
);
CREATE UNIQUE INDEX probe_receipt ON probe_parts(account_id, message_id, probe_id, part)
  WHERE message_id IS NOT NULL;
CREATE TABLE feedback (
  event_id INTEGER NOT NULL REFERENCES raw_events(id) ON DELETE CASCADE,
  account_id TEXT NOT NULL, group_id TEXT NOT NULL, target_message_id TEXT NOT NULL,
  kind TEXT NOT NULL, probe_id TEXT,
  PRIMARY KEY(event_id, target_message_id, kind)
);
CREATE TABLE connection_gaps (
  id INTEGER PRIMARY KEY, account_id TEXT NOT NULL, started_at TEXT NOT NULL,
  ended_at TEXT, reason TEXT NOT NULL
);
CREATE TABLE control (
  account_id TEXT PRIMARY KEY, paused INTEGER NOT NULL
);
"""

MIGRATION_2 = """
CREATE TABLE context_cache (
  account_id TEXT NOT NULL, group_id TEXT NOT NULL, kind TEXT NOT NULL,
  reference_id TEXT NOT NULL, status TEXT NOT NULL, payload TEXT,
  error_code TEXT, checked_at TEXT NOT NULL,
  PRIMARY KEY(account_id, group_id, kind, reference_id)
);
CREATE INDEX context_event_time ON raw_events(account_id, group_id, occurred_at, id);
"""


SCHEMA_3 = {
    "raw_events": """
      id INTEGER PRIMARY KEY AUTOINCREMENT, event_key TEXT NOT NULL UNIQUE,
      account_id TEXT NOT NULL, group_id TEXT NOT NULL, event_type TEXT NOT NULL,
      occurred_at INTEGER NOT NULL, received_at TEXT NOT NULL, payload TEXT NOT NULL
    """,
    "messages": """
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      account_id TEXT NOT NULL, group_id TEXT NOT NULL, message_id TEXT NOT NULL,
      event_id INTEGER NOT NULL, user_id TEXT NOT NULL, own_message INTEGER NOT NULL,
      segments TEXT NOT NULL, issues TEXT NOT NULL, recalled INTEGER NOT NULL DEFAULT 0,
      UNIQUE(account_id, group_id, message_id)
    """,
    "probes": """
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      account_id TEXT NOT NULL, probe_id TEXT NOT NULL, source_group_id TEXT NOT NULL,
      target_group_id TEXT NOT NULL, operator_id TEXT NOT NULL, content_hash TEXT NOT NULL,
      status TEXT NOT NULL CHECK(status IN ('sending','sent','failed','unknown','cancelled')),
      created_at TEXT NOT NULL, updated_at TEXT NOT NULL, UNIQUE(account_id, probe_id)
    """,
    "probe_parts": """
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      account_id TEXT NOT NULL, probe_id TEXT NOT NULL, part INTEGER NOT NULL,
      status TEXT NOT NULL CHECK(status IN ('sending','sent','failed','unknown','cancelled')),
      message_id TEXT, error_code TEXT, UNIQUE(account_id, probe_id, part)
    """,
    "feedback": """
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      event_id INTEGER NOT NULL, account_id TEXT NOT NULL, group_id TEXT NOT NULL,
      target_message_id TEXT NOT NULL, kind TEXT NOT NULL, probe_id TEXT,
      UNIQUE(event_id, target_message_id, kind)
    """,
    "connection_gaps": """
      id INTEGER PRIMARY KEY AUTOINCREMENT, account_id TEXT NOT NULL,
      started_at TEXT NOT NULL, ended_at TEXT, reason TEXT NOT NULL
    """,
    "control": """
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      account_id TEXT NOT NULL UNIQUE, paused INTEGER NOT NULL
    """,
    "context_cache": """
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      account_id TEXT NOT NULL, group_id TEXT NOT NULL, kind TEXT NOT NULL,
      reference_id TEXT NOT NULL, status TEXT NOT NULL, payload TEXT,
      error_code TEXT, checked_at TEXT NOT NULL,
      UNIQUE(account_id, group_id, kind, reference_id)
    """,
}


def migrate_v3(db: sqlite3.Connection) -> None:
    # Preserve event IDs used by message and feedback associations.
    for table, definition in SCHEMA_3.items():
        columns = ",".join(row[1] for row in db.execute(f"PRAGMA table_info({table})"))
        db.execute(f"CREATE TABLE {table}_v3 ({definition})")
        db.execute(f"INSERT INTO {table}_v3({columns}) SELECT {columns} FROM {table}")
        db.execute(f"DROP TABLE {table}")
        db.execute(f"ALTER TABLE {table}_v3 RENAME TO {table}")
    for statement in (
        "CREATE INDEX context_event_time ON raw_events(account_id,group_id,occurred_at,id)",
        "CREATE INDEX event_retention ON raw_events(received_at)",
        "CREATE INDEX message_event ON messages(event_id)",
        "CREATE INDEX feedback_event ON feedback(event_id)",
        "CREATE INDEX probe_receipt ON probe_parts(account_id,message_id) WHERE message_id IS NOT NULL",
    ):
        db.execute(statement)


class Store:
    def __init__(self, path: Path):
        path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        os.chmod(path.parent, 0o700)
        descriptor = os.open(path, os.O_CREAT | os.O_RDWR, 0o600)
        os.close(descriptor)
        os.chmod(path, 0o600)
        self.db = sqlite3.connect(path, timeout=10, isolation_level=None)
        self.db.row_factory = sqlite3.Row
        self.db.execute("PRAGMA foreign_keys=OFF")
        self.db.execute("PRAGMA journal_mode=WAL")
        self.db.execute("PRAGMA synchronous=FULL")
        try:
            # Check the version while holding the write lock: concurrent openers
            # must observe the previous opener's completed migration.
            self.db.execute("BEGIN IMMEDIATE")
            version = self.db.execute("PRAGMA user_version").fetchone()[0]
            if version not in {0, 1, 2, 3}:
                raise StoreError("数据库版本不支持")
            for target, sql in ((1, MIGRATION_1), (2, MIGRATION_2)):
                if version < target:
                    for statement in sql.split(";"):
                        if statement.strip():
                            self.db.execute(statement)
                    self.db.execute(f"PRAGMA user_version={target}")
            if version < 3:
                migrate_v3(self.db)
                self.db.execute("PRAGMA user_version=3")
            self.db.commit()
        except BaseException:
            if self.db.in_transaction:
                self.db.rollback()
            self.db.close()
            raise

    def close(self) -> None:
        self.db.close()

    def ingest(self, event: Event) -> bool:
        self.db.execute("BEGIN IMMEDIATE")
        try:
            cursor = self.db.execute(
                "INSERT OR IGNORE INTO raw_events(event_key,account_id,group_id,event_type,occurred_at,received_at,payload) VALUES(?,?,?,?,?,?,?)",
                (event.key, event.account_id, event.group_id, event.event_type,
                 event.occurred_at, event.received_at, canonical(event.raw)),
            )
            if cursor.rowcount == 0:
                self.db.commit()
                return False
            event_id = cursor.lastrowid
            if event.message is not None:
                recalled = self.db.execute(
                    "SELECT 1 FROM raw_events WHERE account_id=? AND group_id=? AND event_type='notice.group_recall' AND json_extract(payload,'$.message_id') IS NOT NULL AND CAST(json_extract(payload,'$.message_id') AS TEXT)=?",
                    (event.account_id, event.group_id, event.message_id),
                ).fetchone() is not None
                self.db.execute("INSERT INTO messages(account_id,group_id,message_id,event_id,user_id,own_message,segments,issues,recalled) VALUES(?,?,?,?,?,?,?,?,?)", (
                    event.account_id, event.group_id, event.message_id, event_id,
                    event.user_id, int(event.own_message), canonical(event.message),
                    canonical(event.issues), int(recalled),
                ))
                if not event.own_message:
                    for segment in event.message:
                        if segment["type"] == "reply" and "missing_reply_id" not in event.issues:
                            self._feedback(event_id, event, segment["data"]["id"], "reply")
            elif event.event_type == "notice.group_recall" and event.message_id is not None:
                self.db.execute("UPDATE messages SET recalled=1 WHERE account_id=? AND group_id=? AND message_id=?",
                                (event.account_id, event.group_id, event.message_id))
            elif event.event_type.startswith("notice.group_msg_emoji_like") and event.message_id is not None:
                self._feedback(event_id, event, event.message_id, "emoji_unverified")
            self.db.commit()
            return True
        except BaseException:
            self.db.rollback()
            raise

    def _feedback(self, event_id: int, event: Event, message_id: str, kind: str) -> None:
        receipt = self.db.execute(
            "SELECT p.probe_id FROM probe_parts pp JOIN probes p USING(account_id,probe_id) WHERE p.account_id=? AND p.target_group_id=? AND pp.message_id=?",
            (event.account_id, event.group_id, message_id),
        ).fetchone()
        self.db.execute("INSERT OR IGNORE INTO feedback(event_id,account_id,group_id,target_message_id,kind,probe_id) VALUES(?,?,?,?,?,?)", (
            event_id, event.account_id, event.group_id, message_id, kind,
            receipt[0] if receipt else None,
        ))

    def summary(self) -> dict:
        return {table: self.db.execute(f"SELECT count(*) FROM {table}").fetchone()[0]
                for table in ("raw_events", "messages", "feedback", "probes", "connection_gaps")}

    def message(self, account: str, group: str, message: str) -> dict | None:
        row = self.db.execute("SELECT * FROM messages WHERE account_id=? AND group_id=? AND message_id=?",
                              (account, group, message)).fetchone()
        if not row:
            return None
        result = dict(row)
        result["segments"] = json.loads(result["segments"])
        result["issues"] = json.loads(result["issues"])
        return result

    def context_account(self, group: str, message: str) -> str:
        rows = self.db.execute(
            "SELECT account_id FROM messages WHERE group_id=? AND message_id=? UNION "
            "SELECT account_id FROM context_cache WHERE group_id=? AND kind='message' AND reference_id=?",
            (group, message, group, message),
        ).fetchall()
        if not rows:
            rows = self.db.execute(
                "SELECT account_id FROM messages WHERE group_id=? UNION "
                "SELECT account_id FROM context_cache WHERE group_id=?", (group, group),
            ).fetchall()
        if len(rows) != 1:
            raise StoreError("本地账号缺失或存在歧义，请使用在线上下文查询")
        return rows[0][0]

    def context_message(self, account: str, group: str, message: str) -> dict | None:
        row = self.db.execute(
            "SELECT e.payload,m.recalled FROM messages m JOIN raw_events e ON m.event_id=e.id "
            "WHERE m.account_id=? AND m.group_id=? AND m.message_id=?", (account, group, message),
        ).fetchone()
        if not row:
            return None
        return {"payload": json.loads(row[0]), "recalled": bool(row[1])}

    def context_recalled(self, account: str, group: str, message: str) -> bool:
        return self.db.execute(
            "SELECT 1 FROM raw_events WHERE account_id=? AND group_id=? "
            "AND event_type='notice.group_recall' "
            "AND CAST(json_extract(payload,'$.message_id') AS TEXT)=? LIMIT 1",
            (account, group, message),
        ).fetchone() is not None

    def context_cached(self, account: str, group: str, kind: str, reference: str) -> dict | None:
        row = self.db.execute(
            "SELECT * FROM context_cache WHERE account_id=? AND group_id=? AND kind=? AND reference_id=?",
            (account, group, kind, reference),
        ).fetchone()
        if not row:
            return None
        result = dict(row)
        result["payload"] = json.loads(result["payload"]) if result["payload"] else None
        return result

    def cache_context(self, account: str, group: str, kind: str, reference: str,
                      status: str, payload: dict | None = None, error_code: str | None = None) -> None:
        self.db.execute(
            "INSERT INTO context_cache(account_id,group_id,kind,reference_id,status,payload,error_code,checked_at) VALUES(?,?,?,?,?,?,?,?) "
            "ON CONFLICT(account_id,group_id,kind,reference_id) DO UPDATE SET "
            "status=excluded.status,payload=excluded.payload,error_code=excluded.error_code,checked_at=excluded.checked_at",
            (account, group, kind, reference, status, canonical(payload) if payload is not None else None,
             error_code, utc_now()),
        )

    def nearby_messages(self, account: str, group: str, message: str, timestamp: int,
                        each_side: int, window: int) -> dict:
        anchor = self.db.execute(
            "SELECT event_id FROM messages WHERE account_id=? AND group_id=? AND message_id=?",
            (account, group, message),
        ).fetchone()
        # A cached platform message has no local arrival position. For those,
        # same-second neighbours go in 'after' and remain explicitly weak clues.
        position = anchor[0] if anchor else -1
        result = {}
        for side, comparison, order in (("before", "<", "DESC"), ("after", ">", "ASC")):
            rows = self.db.execute(
                "SELECT e.payload,m.recalled FROM messages m JOIN raw_events e ON m.event_id=e.id "
                "WHERE m.account_id=? AND m.group_id=? AND m.message_id<>? "
                "AND e.occurred_at BETWEEN ? AND ? "
                f"AND (e.occurred_at,e.id) {comparison} (?,?) "
                f"ORDER BY e.occurred_at {order},e.id {order} LIMIT ?",
                (account, group, message, timestamp - window, timestamp + window,
                 timestamp, position, each_side),
            ).fetchall()
            values = [{"payload": json.loads(row[0]), "recalled": bool(row[1])} for row in rows]
            result[side] = list(reversed(values)) if side == "before" else values
        return result

    def paused(self, account: str) -> bool:
        row = self.db.execute("SELECT max(paused) FROM control WHERE account_id IN (?, '*')", (account,)).fetchone()
        return row is not None and bool(row[0])

    def set_paused(self, account: str, paused: bool) -> None:
        self.db.execute("INSERT INTO control(account_id,paused) VALUES(?,?) ON CONFLICT(account_id) DO UPDATE SET paused=excluded.paused",
                        (account, int(paused)))

    def begin_probe(self, account: str, request: dict, limit: int) -> bool:
        digest = hashlib.sha256(canonical(request).encode()).hexdigest()
        now = utc_now()
        self.db.execute("BEGIN IMMEDIATE")
        try:
            row = self.db.execute("SELECT * FROM probes WHERE account_id=? AND probe_id=?",
                                  (account, request["probe_id"])).fetchone()
            if row:
                if row["content_hash"] != digest:
                    raise StoreError("同一探测 ID 已用于不同内容")
                self.db.commit()
                return False
            if self.paused(account):
                raise StoreError("发送已暂停")
            today = now[:10]
            count = self.db.execute("SELECT count(*) FROM probe_parts pp JOIN probes p USING(account_id,probe_id) WHERE p.account_id=? AND substr(p.created_at,1,10)=?",
                                    (account, today)).fetchone()[0]
            if count + len(request["messages"]) > limit:
                raise StoreError("每日探测消息额度不足")
            self.db.execute("INSERT INTO probes(account_id,probe_id,source_group_id,target_group_id,operator_id,content_hash,status,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?,?)", (
                account, request["probe_id"], request["source_group_id"], request["target_group_id"],
                request["operator_id"], digest, "sending", now, now,
            ))
            for index in range(len(request["messages"])):
                # Conservatively reserve uncertain states before any side effect.
                self.db.execute("INSERT INTO probe_parts(account_id,probe_id,part,status,message_id,error_code) VALUES(?,?,?,'sending',NULL,NULL)",
                                (account, request["probe_id"], index))
            self.db.commit()
            return True
        except BaseException:
            self.db.rollback()
            raise

    def finish_part(self, account: str, probe: str, part: int, status: str,
                    message_id: str | None = None, error_code: str | None = None) -> None:
        self.db.execute("BEGIN IMMEDIATE")
        try:
            row = self.db.execute("SELECT status FROM probe_parts WHERE account_id=? AND probe_id=? AND part=?",
                                  (account, probe, part)).fetchone()
            if not row or row[0] not in {"sending", "unknown"}:
                raise StoreError("探测状态不允许此更新")
            self.db.execute("UPDATE probe_parts SET status=?,message_id=?,error_code=? WHERE account_id=? AND probe_id=? AND part=?",
                            (status, message_id, error_code, account, probe, part))
            self._update_probe(account, probe)
            if message_id:
                self.db.execute("UPDATE feedback SET probe_id=? WHERE account_id=? AND group_id=(SELECT target_group_id FROM probes WHERE account_id=? AND probe_id=?) AND target_message_id=?",
                                (probe, account, account, probe, message_id))
            self.db.commit()
        except BaseException:
            self.db.rollback()
            raise

    def _update_probe(self, account: str, probe: str) -> None:
        states = {row[0] for row in self.db.execute("SELECT status FROM probe_parts WHERE account_id=? AND probe_id=?", (account, probe))}
        status = next((state for state in ("unknown", "sending", "failed", "cancelled") if state in states), "sent")
        self.db.execute("UPDATE probes SET status=?,updated_at=? WHERE account_id=? AND probe_id=?",
                        (status, utc_now(), account, probe))

    def recover_probe(self, account: str, probe: str) -> None:
        # Called only while holding the environment's exclusive sender lock.
        self.db.execute("BEGIN IMMEDIATE")
        try:
            self.db.execute("UPDATE probe_parts SET status='unknown',error_code='interrupted' WHERE account_id=? AND probe_id=? AND status='sending'", (account, probe))
            self._update_probe(account, probe)
            self.db.commit()
        except BaseException:
            self.db.rollback()
            raise

    def probe(self, account: str, probe: str) -> dict | None:
        row = self.db.execute("SELECT probe_id,status FROM probes WHERE account_id=? AND probe_id=?", (account, probe)).fetchone()
        if not row:
            return None
        return {**dict(row), "parts": [dict(r) for r in self.db.execute(
            "SELECT part,status,message_id,error_code FROM probe_parts WHERE account_id=? AND probe_id=? ORDER BY part", (account, probe))]}

    def gap(self, account: str, reason: str) -> int:
        return self.db.execute("INSERT INTO connection_gaps(account_id,started_at,reason) VALUES(?,?,?)",
                               (account, utc_now(), reason)).lastrowid

    def close_gap(self, gap: int) -> None:
        self.db.execute("UPDATE connection_gaps SET ended_at=? WHERE id=?", (utc_now(), gap))

    def bind_gap(self, gap: int, account: str) -> None:
        self.db.execute("UPDATE connection_gaps SET account_id=? WHERE id=? AND account_id=''", (account, gap))

    def probe_account(self, probe: str) -> str:
        rows = self.db.execute("SELECT account_id FROM probes WHERE probe_id=?", (probe,)).fetchall()
        if len(rows) != 1:
            raise StoreError("探测 ID 不存在或跨账号重名，请使用明确的探测 ID")
        return rows[0][0]

    def prune(self, days: int, failure_cache_seconds: int = 60) -> int:
        now = datetime.now(timezone.utc)
        before = (now - timedelta(days=days)).isoformat(timespec="microseconds").replace("+00:00", "Z")
        failed_before = (now - timedelta(seconds=failure_cache_seconds)).isoformat(timespec="microseconds").replace("+00:00", "Z")
        self.db.execute("BEGIN IMMEDIATE")
        try:
            self.db.execute(
                "DELETE FROM context_cache WHERE (status='found' AND checked_at<?) "
                "OR (status<>'found' AND checked_at<?)", (before, failed_before),
            )
            self.db.execute("DELETE FROM feedback WHERE event_id IN (SELECT id FROM raw_events WHERE received_at<?)", (before,))
            self.db.execute("DELETE FROM messages WHERE event_id IN (SELECT id FROM raw_events WHERE received_at<?)", (before,))
            removed = self.db.execute("DELETE FROM raw_events WHERE received_at<?", (before,)).rowcount
            self.db.commit()
            return removed
        except BaseException:
            self.db.rollback()
            raise

    def compact(self) -> dict:
        # Explicit maintenance: rebuilding may wait for other database users.
        self.db.execute("VACUUM")
        busy, _, _ = self.db.execute("PRAGMA wal_checkpoint(TRUNCATE)").fetchone()
        return {"database_compacted": True, "wal_truncated": not bool(busy)}
