import asyncio
import copy
import hashlib
import json
import sqlite3
import sys
import time
import unittest
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from unittest.mock import patch

from pullshit.entrypoints.cli import dispatch, parser
from pullshit.config import ConfigError
from pullshit.application.context import ContextResolver, download_image
from pullshit.adapters.qq.events import normalize
from pullshit.media.cache import MediaError
from pullshit.adapters.qq.napcat import ApiError
from pullshit.storage.store import MIGRATION_1, Store, StoreError
from support import Workspace, message


def segment(kind, **data):
    return {"type": kind, "data": data}


class Client:
    account = "900001"

    def __init__(self, messages=None, forwards=None, hook=None):
        self.messages, self.forwards = messages or {}, forwards or {}
        self.calls = []
        self.hook = hook

    async def call(self, action, params=None):
        self.calls.append((action, params))
        if self.hook:
            await self.hook(action, params)
        if action == "get_login_info":
            return {"user_id": self.account}
        mapping = self.messages if action == "get_msg" else self.forwards
        key = params["message_id"] if action == "get_msg" else params["id"]
        if key not in mapping:
            raise ApiError("synthetic rejection")
        return copy.deepcopy(mapping[key])


class ContextTests(Workspace, unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        super().setUp()
        self.settings_value = self.settings()
        self.store = Store(self.settings_value.path("storage", "database_path"))
        self.addCleanup(self.store.close)

    def put(self, raw):
        event = normalize(raw, str(raw["self_id"]), {"900101"}, {"900201"})
        self.store.ingest(event)

    def resolver(self, client=None, **kw):
        return ContextResolver(self.settings(), self.store, client, **kw)

    async def test_local_reply_chain_and_weak_neighbors_are_scoped(self):
        self.put(message(message_id=1))
        self.put(message(message_id=2, content=[segment("reply", id=1), segment("text", text="reply")]))
        self.put(message(message_id=3, user=900001))
        self.put(message(group="900201", message_id=1, content=[segment("text", text="foreign group")]))
        self.put(message(account=900002, message_id=1, content=[segment("text", text="foreign account")]))
        result = await self.resolver().resolve("900101", "2")
        quote = result["message"]["segments"][0]["context"]
        self.assertEqual(quote["message_id"], "1")
        self.assertEqual(result["nearby"]["relation"], "time_neighbor")
        self.assertTrue(result["nearby"]["after"][0]["own_message"])
        self.assertNotIn("foreign", json.dumps(result))
        self.assertEqual(result["stats"]["fetch_calls"], 0)
        with self.assertRaises(StoreError):
            self.store.context_account("900101", "1")
        with self.assertRaises(ConfigError):
            await self.resolver().resolve("900999", "1")

    async def test_offline_cli_does_not_construct_platform_connection(self):
        self.put(message())
        args = parser().parse_args(["--root", str(self.root), "context", "--profile", "development",
                                   "--group-id", "900101", "--message-id", "1"])
        with patch("pullshit.adapters.qq.napcat.NapCat", side_effect=AssertionError("must stay offline")):
            result = await asyncio.to_thread(dispatch, args)
        self.assertFalse(result["online"])
        args.cache_media = True
        with self.assertRaises(ConfigError):
            await asyncio.to_thread(dispatch, args)

    async def test_missing_offline_and_cache_reuse_do_not_create_live_events(self):
        client = Client({1: message()})
        missing = await self.resolver().resolve("900101", "1")
        self.assertIn("missing_offline", missing["issues"])
        first = await self.resolver(client).resolve("900101", "1")
        self.assertEqual(first["stats"]["fetch_calls"], 1)
        second = await self.resolver(client).resolve("900101", "1")
        offline = await self.resolver().resolve("900101", "1")
        self.assertEqual(second["stats"]["fetch_calls"], 0)
        self.assertEqual(offline["message"]["source"], "cache")
        self.assertEqual(self.store.summary()["messages"], 0)
        self.assertEqual([x[0] for x in client.calls], ["get_login_info", "get_msg"])

    async def test_message_identity_and_group_and_account_are_checked(self):
        for changed in ({"group_id": "900201"}, {"self_id": 900002},
                        {"message_id": 2}, {"message_type": "private"}):
            with self.subTest(changed=changed):
                self.store.db.execute("DELETE FROM context_cache")
                raw = message(); raw.update(changed)
                result = await self.resolver(Client({1: raw})).resolve("900101", "1")
                self.assertIn("invalid_or_mismatched_payload", result["issues"])
                cached = self.store.context_cached("900001", "900101", "message", "1")
                self.assertIsNone(cached["payload"])

    async def test_failure_cache_expires_and_allows_retry(self):
        client = Client()
        first = await self.resolver(client).resolve("900101", "1")
        self.assertIn("api_rejected", first["issues"])
        client.messages[1] = message()
        second = await self.resolver(client).resolve("900101", "1")
        self.assertEqual(second["stats"]["fetch_calls"], 0)
        self.store.db.execute("UPDATE context_cache SET checked_at='2000-01-01T00:00:00Z'")
        third = await self.resolver(client).resolve("900101", "1")
        self.assertEqual(third["message"]["status"], "found")

    async def test_revocation_during_fetch_discards_result_and_identity_change_stops(self):
        async def revoke(action, params):
            if action == "get_msg":
                self.local["authorization"]["source_group_ids"] = []
                self.local["authorization"]["routes"] = []
                self.write_local()
        with self.assertRaises(ConfigError):
            await self.resolver(Client({1: message()}, hook=revoke)).resolve("900101", "1")
        self.assertIsNone(self.store.context_cached("900001", "900101", "message", "1"))
        self.local["authorization"]["source_group_ids"] = ["900101"]
        self.write_local()
        client = Client({1: message()})
        resolver = self.resolver(client)
        client.account = "900002"
        with self.assertRaises(ConfigError):
            await resolver.resolve("900101", "1")
        self.assertEqual([c[0] for c in client.calls], ["get_login_info"])

    async def test_reply_cycle_depth_and_total_fetch_budget(self):
        self.put(message(message_id=1, content=[segment("reply", id=2)]))
        self.put(message(message_id=2, content=[segment("reply", id=1)]))
        cycle = await self.resolver().resolve("900101", "1")
        self.assertIn("reply_cycle", cycle["issues"])
        self.local["context"] = {"max_reply_depth": 1}; self.write_local()
        self.put(message(message_id=3, content=[segment("reply", id=1)]))
        depth = await self.resolver().resolve("900101", "3")
        self.assertIn("reply_depth_limit", depth["issues"])
        self.local["context"] = {"max_fetch_calls": 1}; self.write_local()
        self.put(message(message_id=4, content=[segment("reply", id=10), segment("reply", id=11)]))
        budget = await self.resolver(Client({10: message(message_id=10), 11: message(message_id=11)})).resolve("900101", "4")
        self.assertEqual(budget["stats"]["fetch_calls"], 1)
        self.assertIn("fetch_limit", budget["issues"])

    async def test_forward_shapes_nested_cycles_and_embedded_reply_provenance(self):
        self.put(message(content=[segment("forward", id="outer")]))
        nodes = [segment("node", user_id="900401", content=[segment("text", text="first"),
                     segment("reply", id=88), segment("forward", id="inner")])]
        client = Client(forwards={"outer": {"message": nodes}, "inner": {"messages": [
            message(group="900999", message_id=88, content=[segment("forward", id="outer")])]}})
        result = await self.resolver(client).resolve("900101", "1")
        tree = result["message"]["segments"][0]["context"]
        self.assertFalse(tree["nodes"][0]["origin_group_verified"])
        self.assertIn("forward_cycle", result["issues"])
        self.assertIn("embedded_reply_source_unverified", result["issues"])
        self.assertEqual([c[0] for c in client.calls], ["get_login_info", "get_forward_msg"] * 2)
        self.assertNotIn("900999", json.dumps(result))
        repeated = await self.resolver().resolve("900101", "1")
        self.assertEqual(repeated["stats"]["fetch_calls"], 0)

    async def test_inline_forward_global_node_and_depth_limits(self):
        node = segment("node", content=[segment("text", text="deep")])
        for _ in range(8):
            node = segment("node", content=[segment("forward", content=[node])])
        self.put(message(content=[segment("forward", content=[node])]))
        result = await self.resolver().resolve("900101", "1")
        self.assertIn("forward_depth_limit", result["issues"])
        self.put(message(message_id=2, content=[segment("forward", id="wide", content=[
            segment("node", content=[segment("text", text="item")]) for _ in range(200)])]))
        result = await self.resolver().resolve("900101", "2")
        self.assertLessEqual(result["stats"]["nodes"], 100)
        self.assertIn("node_limit", result["issues"])

    async def test_query_deadline_preserves_partial_root_and_does_not_cache_timeout(self):
        self.put(message(content=[segment("text", text="keep"), segment("reply", id=2)]))
        async def slow(action, params):
            if action == "get_msg":
                await asyncio.sleep(10)
        deadline = asyncio.get_running_loop().time() + .05
        result = await self.resolver(Client({2: message(message_id=2)}, hook=slow), deadline=deadline).resolve("900101", "1")
        self.assertIn("timeout", result["issues"])
        self.assertEqual(result["message"]["segments"][0]["data"]["text"], "keep")
        self.assertIsNone(self.store.context_cached("900001", "900101", "message", "2"))

    async def test_recall_and_text_budget(self):
        self.put(message(content=[segment("text", text="abcdefgh"), segment("reply", id=2)]))
        self.put({"post_type": "notice", "notice_type": "group_recall", "self_id": 900001,
                  "group_id": 900101, "message_id": 1, "user_id": 900401, "time": 1791590001})
        self.local["context"] = {"max_text_characters": 4}
        result = await self.resolver(Client()).resolve("900101", "1")
        self.assertTrue(result["message"]["recalled"])
        self.assertEqual(result["message"]["segments"][0]["data"]["text"], "abcd")
        self.assertEqual(result["stats"]["fetch_calls"], 0)
        self.assertIn("text_limit", result["issues"])

    async def test_recall_during_query_marks_root_and_stops_later_expansion(self):
        self.put(message(content=[segment("reply", id=2), segment("reply", id=3)]))
        async def recall(action, params):
            if action == "get_msg":
                self.put({"post_type": "notice", "notice_type": "group_recall", "self_id": 900001,
                          "group_id": 900101, "message_id": 1, "user_id": 900401, "time": 1791590001})
        client = Client({2: message(message_id=2), 3: message(message_id=3)}, hook=recall)
        result = await self.resolver(client).resolve("900101", "1")
        self.assertTrue(result["message"]["recalled"])
        self.assertEqual(result["stats"]["fetch_calls"], 1)
        self.assertEqual(result["message"]["status"], "recalled")

    async def test_media_cached_reused_and_corrupt_cache_is_explicit(self):
        url = "https://example.invalid/image?token=private"
        self.put(message(content=[segment("image", file="image-one", url=url)]))
        downloads = []
        async def downloader(address, directory, limits, timeout):
            downloads.append(address)
            payload = b"\x89PNG\r\n\x1a\nsynthetic"
            digest = hashlib.sha256(payload).hexdigest()
            directory.mkdir(parents=True, exist_ok=True)
            metadata = {"sha256": digest, "bytes": len(payload), "type": "png",
                        "cached_at": datetime.now(timezone.utc).isoformat()}
            (directory / digest).write_bytes(payload)
            (directory / f"{digest}.json").write_text(json.dumps(metadata))
            return metadata
        result = await self.resolver(Client(), cache_media=True, downloader=downloader).resolve("900101", "1")
        cached = result["message"]["segments"][0]["media"]
        self.assertEqual(cached["status"], "cached")
        self.assertNotIn("private", json.dumps(result))
        second = await self.resolver().resolve("900101", "1")
        self.assertEqual(second["stats"]["media_cache_hits"], 1)
        self.assertEqual(len(downloads), 1)
        from pathlib import Path
        Path(cached["path"]).write_bytes(b"corrupt")
        third = await self.resolver().resolve("900101", "1")
        self.assertIn("invalid_media_cache", third["issues"])
        self.assertEqual(third["message"]["segments"][0]["media"]["status"], "media_not_cached")

    async def test_media_attempt_limit_and_failure_cache(self):
        self.put(message(content=[segment("image", file=str(i), url="https://example.invalid/a") for i in range(8)]))
        async def failed(*args):
            raise MediaError("synthetic failure")
        result = await self.resolver(Client(), cache_media=True, downloader=failed).resolve("900101", "1")
        self.assertEqual(result["stats"]["media_downloads"], 5)
        self.assertIn("media_limit", result["issues"])
        second = await self.resolver(Client(), cache_media=True, downloader=failed).resolve("900101", "1")
        self.assertEqual(second["stats"]["media_downloads"], 3)

    async def test_failed_image_address_is_renewed_once_with_verified_message(self):
        old = segment("image", file="stable-image.jpg", url="https://example.invalid/old")
        fresh = segment("image", file="stable-image.jpg", url="https://example.invalid/fresh")
        self.put(message(content=[old]))
        downloads = []
        async def downloader(url, directory, limits, timeout):
            downloads.append(url)
            if url.endswith("old"):
                raise MediaError("obsolete address", "media_http_rejected")
            payload = b"GIF89asynthetic"; digest = hashlib.sha256(payload).hexdigest()
            directory.mkdir(parents=True, exist_ok=True)
            metadata = {"sha256": digest, "bytes": len(payload), "type": "gif",
                        "cached_at": datetime.now(timezone.utc).isoformat()}
            (directory / digest).write_bytes(payload)
            (directory / f"{digest}.json").write_text(json.dumps(metadata))
            return metadata
        client = Client({1: message(content=[fresh])})
        result = await self.resolver(client, cache_media=True, downloader=downloader).resolve("900101", "1")
        self.assertEqual(result["status"], "complete")
        self.assertTrue(result["message"]["segments"][0]["media"]["url_refreshed"])
        self.assertEqual(result["stats"]["fetch_calls"], 1)
        self.assertEqual(result["stats"]["media_downloads"], 2)
        second = await self.resolver().resolve("900101", "1")
        self.assertEqual(second["stats"]["media_cache_hits"], 1)
        metadata_path = self.settings().path("storage", "media_directory") / (result["message"]["segments"][0]["media"]["sha256"] + ".json")
        metadata = json.loads(metadata_path.read_text()); metadata["cached_at"] = "2000-01-01T00:00:00Z"
        metadata_path.write_text(json.dumps(metadata))
        expired = await self.resolver().resolve("900101", "1")
        self.assertIn("media_cache_expired", expired["issues"])

    async def test_media_size_failure_is_distinct_and_does_not_renew(self):
        self.put(message(content=[segment("image", file="image.jpg", url="https://example.invalid/size")]))
        async def downloader(*args):
            raise MediaError("oversized", "media_size_limit")
        client = Client()
        result = await self.resolver(client, cache_media=True, downloader=downloader).resolve("900101", "1")
        self.assertIn("media_size_limit", result["issues"])
        self.assertEqual(client.calls, [])

    async def test_real_worker_preserves_address_rejection_without_network(self):
        with self.assertRaises(MediaError) as caught:
            await download_image("http://127.0.0.1/a", self.root / "media", self.settings().data["media"], 2)
        self.assertEqual(caught.exception.code, "media_address_rejected")

    async def test_download_timeout_terminates_blocked_worker(self):
        factory = asyncio.create_subprocess_exec
        processes = []
        async def blocked(*args, **kwargs):
            process = await factory(sys.executable, "-c", "import time; time.sleep(60)", **kwargs)
            processes.append(process)
            return process
        with patch("pullshit.application.context.asyncio.create_subprocess_exec", blocked):
            with self.assertRaises(MediaError) as caught:
                await download_image("https://example.invalid/a", self.root / "media", {}, .05)
        self.assertEqual(caught.exception.code, "media_timeout")
        self.assertIsNotNone(processes[0].returncode)

    async def test_online_cli_is_read_only_and_connect_time_is_bounded(self):
        from websockets.asyncio.server import serve
        calls = []
        async def handler(ws):
            async for frame in ws:
                call = json.loads(frame); calls.append(call["action"])
                data = {"user_id": 900001} if call["action"] == "get_login_info" else message()
                await ws.send(json.dumps({"status": "ok", "retcode": 0, "data": data, "echo": call["echo"]}))
        async with serve(handler, "127.0.0.1", 0) as server:
            port = server.sockets[0].getsockname()[1]
            self.local["napcat"]["endpoint"] = f"ws://127.0.0.1:{port}/"; self.write_local()
            args = parser().parse_args(["--root", str(self.root), "context", "--profile", "development",
                                       "--group-id", "900101", "--message-id", "1", "--online"])
            result = await asyncio.to_thread(dispatch, args)
        self.assertEqual(result["message"]["source"], "platform")
        self.assertNotIn("send_group_msg", calls)
        self.assertEqual(calls, ["get_login_info", "get_login_info", "get_msg"])

    async def test_online_cli_deadline_includes_initial_login(self):
        from websockets.asyncio.server import serve
        async def stalled(ws):
            await ws.recv()
            await ws.wait_closed()
        async with serve(stalled, "127.0.0.1", 0) as server:
            port = server.sockets[0].getsockname()[1]
            self.local["napcat"]["endpoint"] = f"ws://127.0.0.1:{port}/"
            self.local["context"] = {"total_timeout_seconds": 1}; self.write_local()
            args = parser().parse_args(["--root", str(self.root), "context", "--profile", "development",
                                       "--group-id", "900101", "--message-id", "1", "--online"])
            start = time.monotonic()
            result = await asyncio.to_thread(dispatch, args)
        self.assertLess(time.monotonic() - start, 4)
        self.assertEqual(result["issues"], ["timeout"])
        self.assertEqual(result["stats"]["fetch_calls"], 0)

    async def test_nearby_window_limits_and_invalid_cache(self):
        self.local["context"] = {"nearby_messages_each_side": 2, "nearby_window_seconds": 10}
        for number in range(1, 9):
            raw = message(message_id=number); raw["time"] += number
            self.put(raw)
        old = message(message_id=99); old["time"] -= 1000; self.put(old)
        result = await self.resolver().resolve("900101", "4")
        self.assertEqual([v["message_id"] for v in result["nearby"]["before"]], ["2", "3"])
        self.assertEqual([v["message_id"] for v in result["nearby"]["after"]], ["5", "6"])
        self.store.cache_context("900001", "900101", "message", "10", "found", message(message_id=10))
        self.store.db.execute("UPDATE context_cache SET payload='invalid json'")
        result = await self.resolver().resolve("900101", "10")
        self.assertIn("invalid_cache", result["issues"])
        self.assertIn("missing_offline", result["issues"])


class MigrationTests(Workspace, unittest.TestCase):
    def test_migration_preserves_v1_data_and_concurrent_openers(self):
        path = self.root / "data/development/migration.sqlite3"
        path.parent.mkdir(parents=True)
        db = sqlite3.connect(path)
        db.executescript(MIGRATION_1)
        db.execute("PRAGMA user_version=1")
        db.execute("INSERT INTO control VALUES('*',1)"); db.commit(); db.close()
        def open_store(_):
            store = Store(path)
            try:
                return store.paused("*"), store.db.execute("PRAGMA user_version").fetchone()[0]
            finally:
                store.close()
        with ThreadPoolExecutor(max_workers=3) as pool:
            results = list(pool.map(open_store, range(3)))
        self.assertEqual(results, [(True, 3)] * 3)

    def test_future_database_version_is_rejected(self):
        path = self.root / "future.sqlite3"
        db = sqlite3.connect(path); db.execute("PRAGMA user_version=999"); db.close()
        with self.assertRaises(StoreError):
            Store(path)
