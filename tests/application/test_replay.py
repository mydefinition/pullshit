import io
import json
import unittest
from pathlib import Path
from unittest.mock import patch

from pullshit.entrypoints.cli import main
from pullshit.config import ConfigError
from pullshit.application.replay import replay
from support import Workspace, message


class ReplayTests(Workspace, unittest.TestCase):
    def test_sanitized_real_event_fixture_replays_without_network(self):
        fixture = Path(__file__).resolve().parents[1] / "fixtures/napcat-sanitized-events.jsonl"
        with patch("pullshit.adapters.qq.napcat.connect", side_effect=AssertionError("offline must not connect")):
            first = replay(self.root, fixture, Path("data/replay/real.sqlite3"), "900001",
                           {"900101", "900102"}, {"900101"})
            second = replay(self.root, fixture, Path("data/replay/real.sqlite3"), "900001",
                            {"900101", "900102"}, {"900101"})
        self.assertEqual(first["inserted"], 5)
        self.assertEqual(first["stored"]["messages"], 4)
        self.assertEqual(first["stored"]["feedback"], 1)
        self.assertEqual(second["duplicates"], 5)
        self.assertEqual(first["stored"], second["stored"])

    def test_replay_is_repeatable_and_never_connects(self):
        fixture = self.root / "events.jsonl"
        fixture.write_text('\n'.join([json.dumps(message()), json.dumps(message()), json.dumps(message(group="900999")), "invalid", json.dumps(message(message_id=2))]))
        with patch("pullshit.adapters.qq.napcat.connect", side_effect=AssertionError("offline must not connect")):
            first = replay(self.root, fixture, Path("data/replay/test.sqlite3"), "900001", {"900101"}, set())
            second = replay(self.root, fixture, Path("data/replay/test.sqlite3"), "900001", {"900101"}, set())
        self.assertEqual((first["inserted"], first["duplicates"], first["ignored"], first["invalid"]), (2, 1, 1, 1))
        self.assertEqual(second["inserted"], 0)
        self.assertEqual(first["stored"], second["stored"])

    def test_live_database_and_symlink_are_rejected(self):
        fixture = self.root / "events.jsonl"
        fixture.write_text(json.dumps(message()))
        for database in (Path("data/development/pullshit.sqlite3"), Path("../outside.sqlite3")):
            with self.assertRaises(ConfigError):
                replay(self.root, fixture, database, "900001", {"900101"}, set())
        (self.root / "data").mkdir()
        (self.root / "data/development").mkdir()
        (self.root / "data/replay").symlink_to(self.root / "data/development", target_is_directory=True)
        with self.assertRaises(ConfigError):
            replay(self.root, fixture, Path("data/replay/test.sqlite3"), "900001", {"900101"}, set())

    def test_config_check_output_is_redacted(self):
        stream = io.StringIO()
        with patch("sys.stdout", stream):
            code = main(["--root", str(self.root), "check-config", "--profile", "development"])
        self.assertEqual(code, 0)
        self.assertNotIn("900001", stream.getvalue())
        self.assertNotIn("test-only-token", stream.getvalue())

    def test_pause_and_resume_work_without_platform_login(self):
        for command, expected in (("pause", True), ("resume", False)):
            stream = io.StringIO()
            with patch("sys.stdout", stream):
                code = main(["--root", str(self.root), command, "--profile", "development"])
            self.assertEqual(code, 0)
            self.assertEqual(json.loads(stream.getvalue())["paused"], expected)
