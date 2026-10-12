"""Offline fixture ingestion. This module has no platform or send dependency."""

from __future__ import annotations

import json
from pathlib import Path

from pullshit.config import ConfigError, decimal_id
from pullshit.adapters.qq.events import normalize
from pullshit.storage.store import Store


def replay(root: Path, fixture: Path, database: Path, account: str,
           sources: set[str], targets: set[str]) -> dict:
    root = root.resolve()
    database = (root / database).resolve()
    scope = root / "data/replay"
    if scope.resolve() != scope or database == scope or not database.is_relative_to(scope):
        raise ConfigError("回放数据库必须位于 data/replay 独立目录")
    decimal_id(account)
    for item in sources | targets:
        decimal_id(item)
    store = Store(database)
    counts = {"inserted": 0, "duplicates": 0, "ignored": 0, "invalid": 0}
    try:
        with fixture.open() as stream:
            for number, line in enumerate(stream, 1):
                if not line.strip():
                    continue
                try:
                    raw = json.loads(line)
                    event = normalize(raw, account, sources, targets)
                except (ValueError, TypeError):
                    counts["invalid"] += 1
                    continue
                if event is None:
                    counts["ignored"] += 1
                elif store.ingest(event):
                    counts["inserted"] += 1
                else:
                    counts["duplicates"] += 1
        return {**counts, "stored": store.summary()}
    finally:
        store.close()
