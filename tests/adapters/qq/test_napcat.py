import asyncio
import json
import unittest

from websockets.asyncio.server import serve

from pullshit.adapters.qq.napcat import ApiError, NapCat, TransportError
from pullshit.config import ConfigError
from support import message


class NapCatTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.handlers = []
        self.server = await serve(self.handle, "127.0.0.1", 0)
        self.endpoint = f"ws://127.0.0.1:{self.server.sockets[0].getsockname()[1]}/"
        self.account = 900001
        self.events = []
        self.behavior = "ok"
        self.send_calls = 0

    async def asyncTearDown(self):
        self.server.close()
        await self.server.wait_closed()

    async def handle(self, websocket):
        self.assertEqual(websocket.request.headers["Authorization"], "Bearer test-only-token")
        async for frame in websocket:
            request = json.loads(frame)
            if request["action"] == "get_login_info":
                for event in self.events:
                    await websocket.send(json.dumps(event))
                data = {"user_id": self.account}
            elif request["action"] == "send_group_msg":
                self.send_calls += 1
                if self.behavior == "drop":
                    await websocket.close()
                    return
                if self.behavior == "timeout":
                    continue
                data = {"message_id": -11}
            else:
                data = {"version": "synthetic"}
            await websocket.send(json.dumps({"status": "ok", "retcode": 0, "data": data, "echo": request["echo"]}))

    async def test_interleaved_events_and_read_only_permissions(self):
        self.events = [message()]
        async with NapCat(self.endpoint, "test-only-token", "900001", timeout=1) as client:
            self.assertEqual((await client.event())["message_id"], 1)
            self.assertEqual((await client.call("get_version_info"))["version"], "synthetic")
            with self.assertRaises(ApiError):
                await client.call("send_group_msg", {})
            with self.assertRaises(ApiError):
                await client.call("set_group_admin", {})
        self.assertEqual(self.send_calls, 0)

    async def test_wrong_login_account(self):
        self.account = 900002
        with self.assertRaises(ConfigError):
            async with NapCat(self.endpoint, "test-only-token", "900001", timeout=1):
                pass

    async def test_account_is_discovered_without_application_configuration(self):
        async with NapCat(self.endpoint, "test-only-token", timeout=1) as client:
            self.assertEqual(client.account, "900001")

    async def test_account_switch_prevents_send(self):
        async with NapCat(self.endpoint, "test-only-token", timeout=1, allow_send=True) as client:
            self.account = 900002
            with self.assertRaises(ConfigError):
                await client.call("send_group_msg", {"group_id": 900201})
        self.assertEqual(self.send_calls, 0)

    async def test_lost_receipt_and_timeout(self):
        for behavior in ("drop", "timeout"):
            self.behavior = behavior
            async with NapCat(self.endpoint, "test-only-token", "900001", timeout=1, allow_send=True) as client:
                with self.assertRaises(TransportError):
                    await client.call("send_group_msg", {"group_id": 900201})
                self.assertEqual(client.pending, {})

    async def test_concurrent_calls_have_distinct_echoes(self):
        async with NapCat(self.endpoint, "test-only-token", "900001", timeout=1) as client:
            values = await asyncio.gather(*[client.call("get_version_info") for _ in range(5)])
            self.assertEqual(len(values), 5)
            self.assertEqual(client.pending, {})
