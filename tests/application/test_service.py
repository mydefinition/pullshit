import asyncio
import json
import sqlite3
import unittest

from websockets.asyncio.server import serve

from pullshit.application.service import receive
from support import Workspace, message


class ReceiverTests(Workspace, unittest.IsolatedAsyncioTestCase):
    async def test_reconnect_preserves_deduplication_and_records_gaps(self):
        connections = 0

        async def handler(websocket):
            nonlocal connections
            connections += 1
            request = json.loads(await websocket.recv())
            await websocket.send(json.dumps({"status": "ok", "retcode": 0, "data": {"user_id": 900001}, "echo": request["echo"]}))
            await websocket.send(json.dumps(message()))
            if connections == 1:
                await websocket.close()
            else:
                await websocket.send(json.dumps(message(message_id=2)))
                await websocket.wait_closed()

        async with serve(handler, "127.0.0.1", 0) as server:
            port = server.sockets[0].getsockname()[1]
            self.local["napcat"]["endpoint"] = f"ws://127.0.0.1:{port}/"
            self.write_local()
            result = await receive(self.root, "development", max_events=3)
        self.assertEqual(connections, 2)
        self.assertEqual(result["messages"], 2)
        self.assertEqual(result["raw_events"], 2)
        self.assertEqual(result["connection_gaps"], 2)

    async def test_periodic_cleanup_runs_on_an_idle_connection(self):
        self.local["storage"] = {"cleanup_interval_seconds": 1}
        directory = self.root / "data/development/media"
        directory.mkdir(parents=True)
        digest = "a" * 64
        (directory / digest).write_bytes(b"expired image")
        (directory / f"{digest}.json").write_text(json.dumps({"cached_at": "2000-01-01T00:00:00+00:00"}))

        async def handler(websocket):
            request = json.loads(await websocket.recv())
            await websocket.send(json.dumps({"status": "ok", "retcode": 0, "data": {"user_id": 900001}, "echo": request["echo"]}))
            await websocket.send(json.dumps(message()))
            db = sqlite3.connect(self.root / "data/development/pullshit.sqlite3", isolation_level=None)
            try:
                async with asyncio.timeout(3):
                    while db.execute("SELECT count(*) FROM messages").fetchone()[0] == 0:
                        await asyncio.sleep(0.01)
                    db.execute("UPDATE raw_events SET received_at='2000-01-01T00:00:00.000000Z'")
                    # No inbound events are needed to trigger the cleanup timer.
                    while db.execute("SELECT count(*) FROM messages").fetchone()[0] != 0:
                        await asyncio.sleep(0.01)
                self.assertFalse((directory / digest).exists())
                self.assertFalse((directory / f"{digest}.json").exists())
                await websocket.send(json.dumps(message(message_id=2)))
                await websocket.wait_closed()
            finally:
                db.close()

        async with serve(handler, "127.0.0.1", 0) as server:
            port = server.sockets[0].getsockname()[1]
            self.local["napcat"]["endpoint"] = f"ws://127.0.0.1:{port}/"
            self.write_local()
            result = await asyncio.wait_for(receive(self.root, "development", max_events=2), 5)
        self.assertEqual(result["messages"], 1)
        self.assertEqual(result["raw_events"], 1)
