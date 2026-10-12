import copy
import json
import os
import tempfile
from pathlib import Path
from unittest.mock import patch

from pullshit.config import load_settings


class Workspace:
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="pullshit-test-")
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        repo = Path(__file__).resolve().parents[1]
        (self.root / "config/local").mkdir(parents=True)
        for name in ("defaults.json", "development.json", "production.json"):
            (self.root / "config" / name).write_text((repo / "config" / name).read_text())
        self.local = {
            "napcat": {"transport": "websocket", "endpoint": "ws://127.0.0.1:6700/"},
            "authorization": {
                "source_group_ids": ["900101"], "target_group_ids": ["900201"],
                "operator_qq_ids": ["900301"],
                "routes": [{"source_group_id": "900101", "target_group_id": "900201"}],
            },
        }
        self.write_local()
        self.env = patch.dict(os.environ, {"PULLSHIT_DEVELOPMENT_NAPCAT_ACCESS_TOKEN": "test-only-token"})
        self.env.start()
        self.addCleanup(self.env.stop)

    def write_local(self):
        (self.root / "config/local/development.json").write_text(json.dumps(self.local))
        (self.root / "config/local/production.json").write_text("{}")

    def settings(self, sending=False):
        if sending:
            self.local["sending"] = {"enabled": True, "paused": False}
            self.local["runtime"] = {"mode": "probe"}
        self.write_local()
        return load_settings(self.root, "development").with_account("900001")


def message(group="900101", message_id=1, user=900401, account=900001, content=None):
    return {"post_type": "message", "message_type": "group", "sub_type": "normal",
            "time": 1791590000, "self_id": account, "group_id": group,
            "message_id": message_id, "user_id": user,
            "message": content or [{"type": "text", "data": {"text": "synthetic sample"}}]}


def request(parts=1):
    return {"probe_id": "synthetic-probe", "source_group_id": "900101",
            "target_group_id": "900201", "operator_id": "900301",
            "messages": [[{"type": "text", "data": {"text": "synthetic probe"}}] for _ in range(parts)]}
