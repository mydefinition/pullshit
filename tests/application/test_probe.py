import asyncio
import copy
import unittest

from pullshit.config import ConfigError
from pullshit.adapters.qq.napcat import ApiError, TransportError
from pullshit.application.probe import send_probe, sender_lock, validate_request
from pullshit.storage.store import Store, StoreError
from support import Workspace, request


class FakeClient:
    def __init__(self, outcomes, after=None):
        self.outcomes = iter(outcomes)
        self.calls = []
        self.after = after

    async def call(self, action, params):
        self.calls.append((action, params))
        value = next(self.outcomes)
        if self.after:
            self.after(len(self.calls))
        if isinstance(value, BaseException):
            raise value
        return value


class ProbeTests(Workspace, unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        super().setUp()
        self.cfg = self.settings(sending=True)
        self.store = Store(self.cfg.path("storage", "database_path"))
        self.addCleanup(self.store.close)

    async def test_partial_success_and_lost_receipt_never_resend(self):
        req = request(3)
        client = FakeClient([{"message_id": -7}, TransportError("lost")])
        result = await send_probe(self.cfg, self.store, client, req)
        self.assertEqual(result["status"], "unknown")
        self.assertEqual([p["status"] for p in result["parts"]], ["sent", "unknown", "cancelled"])
        self.assertEqual(result["parts"][0]["message_id"], "-7")
        await send_probe(self.cfg, self.store, client, req)
        self.assertEqual(len(client.calls), 2)

    async def test_explicit_failure_and_malformed_receipt(self):
        for outcome, expected in [(ApiError("rejected"), "failed"), ({}, "unknown"), ({"message_id": True}, "unknown")]:
            req = request()
            req["probe_id"] = expected + str(len(self.store.db.execute("SELECT * FROM probes").fetchall()))
            result = await send_probe(self.cfg, self.store, FakeClient([outcome]), req)
            self.assertEqual(result["status"], expected)

    async def test_pause_is_rechecked_between_parts(self):
        client = FakeClient([{"message_id": 7}], after=lambda _: self.store.set_paused(self.cfg.account, True))
        result = await send_probe(self.cfg, self.store, client, request(2))
        self.assertEqual([p["status"] for p in result["parts"]], ["sent", "cancelled"])
        self.assertEqual(len(client.calls), 1)

    async def test_route_change_cancels_remaining_parts(self):
        def change(_):
            self.local["authorization"]["routes"] = []
            self.write_local()
        result = await send_probe(self.cfg, self.store, FakeClient([{"message_id": 7}], after=change), request(2))
        self.assertEqual([p["status"] for p in result["parts"]], ["sent", "cancelled"])

    async def test_cancelled_inflight_is_unknown(self):
        with self.assertRaises(asyncio.CancelledError):
            await send_probe(self.cfg, self.store, FakeClient([asyncio.CancelledError()]), request())
        self.assertEqual(self.store.probe(self.cfg.account, "synthetic-probe")["status"], "unknown")

    async def test_unauthorized_and_disabled_requests_have_no_side_effect(self):
        client = FakeClient([])
        for field in ("source_group_id", "target_group_id", "operator_id"):
            req = request()
            req[field] = "900999"
            with self.assertRaises(ConfigError):
                await send_probe(self.cfg, self.store, client, req)
        self.local["sending"]["enabled"] = False
        with self.assertRaises(ConfigError):
            await send_probe(self.settings(), self.store, client, request())
        self.assertEqual(client.calls, [])
        self.assertEqual(self.store.summary()["probes"], 0)

    def test_sender_lock_and_remote_image_are_rejected(self):
        with sender_lock(self.cfg.path("storage", "database_path")):
            with self.assertRaises(StoreError):
                with sender_lock(self.cfg.path("storage", "database_path")):
                    pass
        req = request()
        req["messages"] = [[{"type": "image", "data": {"file": "http://127.0.0.1/private"}}]]
        with self.assertRaises(ConfigError):
            validate_request(req, self.cfg)
