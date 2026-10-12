import json
import unittest
from unittest.mock import patch

from pullshit.config import ConfigError, load_settings, merge
from support import Workspace


class ConfigurationTests(Workspace, unittest.TestCase):
    def test_defaults_and_environment_isolation(self):
        dev = self.settings()
        prod = load_settings(self.root, "production")
        self.assertFalse(dev.data["sending"]["enabled"])
        self.assertTrue(prod.data["sending"]["paused"])
        self.assertNotEqual(dev.path("storage", "database_path"), prod.path("storage", "database_path"))
        self.assertEqual(prod.account, "")
        self.assertEqual(merge({"a": {"ids": [1], "flag": True}}, {"a": {"ids": [], "flag": False}}),
                         {"a": {"ids": [], "flag": False}})

    def test_invalid_local_values(self):
        cases = [
            {"profile": "production"}, {"schema_version": 1}, {"extra": "secret"},
            {"sending": {"enabled": "false"}}, {"sending": {"enabled": None}},
            {"napcat": {"request_timeout_seconds": True}},
            {"storage": {"database_path": "data/production/pullshit.sqlite3"}},
            {"logging": {"include_raw_payload": True}},
            {"media": {"allow_private_network": True}},
            {"napcat": {"endpoint": "ws://127.0.0.1/?access_token=secret"}},
            {"napcat": {"endpoint": "ws://user:secret@127.0.0.1/"}},
            {"napcat": {"endpoint": "ws://127.0.0.1/event"}},
        ]
        for overlay in cases:
            with self.subTest(overlay=overlay):
                (self.root / "config/local/development.json").write_text(json.dumps(overlay))
                with self.assertRaises(ConfigError) as caught:
                    load_settings(self.root, "development")
                self.assertNotIn("secret", str(caught.exception))

    def test_incomplete_template_is_valid_but_not_online_ready(self):
        (self.root / "config/local/development.json").write_text("{}")
        settings = load_settings(self.root, "development")
        with self.assertRaises(ConfigError):
            settings.validate_live("receive")

    def test_duplicate_json_key_and_wrong_id_type(self):
        path = self.root / "config/local/development.json"
        path.write_text('{"qq":{"account_id":"900001","account_id":"900002"}}')
        with self.assertRaises(ConfigError):
            load_settings(self.root, "development")
        self.local["authorization"]["source_group_ids"] = [900101]
        self.write_local()
        with self.assertRaises(ConfigError):
            load_settings(self.root, "development")

    def test_route_validation_and_empty_array_override(self):
        self.local["authorization"]["routes"].append({"source_group_id": "900101", "target_group_id": "900999"})
        with self.assertRaises(ConfigError):
            self.settings()
        self.local["authorization"]["routes"] = []
        self.local["authorization"]["source_group_ids"] = []
        self.assertEqual(self.settings().sources, set())

    def test_token_and_sending_controls(self):
        settings = self.settings()
        settings.validate_live("receive")
        with self.assertRaises(ConfigError):
            settings.validate_live("probe")
        with patch.dict("os.environ", {}, clear=True):
            with self.assertRaises(ConfigError):
                settings.validate_live("receive")
        self.settings(sending=True).validate_live("probe")

    def test_symlink_environment_directory_rejected(self):
        (self.root / "data").mkdir()
        (self.root / "data/production").mkdir()
        (self.root / "data/development").symlink_to(self.root / "data/production", target_is_directory=True)
        with self.assertRaises(ConfigError):
            self.settings()
