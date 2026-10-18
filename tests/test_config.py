from pullshit.config import ConfigError,load_settings
from tests.support import Workspace

class ConfigTests(Workspace):
    def test_broadcast_requires_only_source_and_target_sets(self):
        self.assertTrue(self.settings.permits('900101','900201'))
        self.assertTrue(self.settings.permits('900101','900202'))
        self.assertFalse(self.settings.permits('900101','900101'))
        self.assertEqual(self.settings.broadcast_targets(self.bundle()),{'900201','900202'})

    def test_defaults_match_chosen_policy(self):
        self.assertEqual(self.settings.data['limits']['max_message_segments'],100)
        self.assertEqual(self.settings.data['limits']['min_interval_seconds'],600)
        self.assertEqual(self.settings.data['limits']['daily_segments'],500)
        self.assertEqual(self.settings.data['retention']['message_days'],3)
    def test_limits_expiry_unknown_fields_and_scope_validation(self):
        for section,change in [('retention',{'content_days':8}),('limits',{'max_message_segments':101}),('agent',{'minimum_score':1.2}),('storage',{'database_path':'data/production/state.sqlite3'}),('runtime',{'surprise':True})]:
            with self.subTest(section=section):
                self.local[section]=change; self.write_config()
                with self.assertRaises(ConfigError): load_settings(self.root,'development')
                del self.local[section]
    def test_requires_explicit_live_model_budget(self):
        with self.assertRaises(ConfigError): self.settings.validate_live()
