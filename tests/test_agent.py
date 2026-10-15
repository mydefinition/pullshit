"""pullshit.agent 执行层测试：真实假 worker 子进程按协议通信，不依赖运行库与 Hermes SDK。"""
from __future__ import annotations

import asyncio
import http.server
import importlib.util
import json
import os
import shutil
import sys
import tempfile
import threading
import time
import unittest
from pathlib import Path
from unittest.mock import patch
from types import SimpleNamespace

from pullshit.agent import hermes_plugin as plugin
from pullshit.agent import hermes_worker as worker
from pullshit.agent.executor import HermesExecutor, clean_stale_workers, vision_describe_image
from pullshit.contracts import (AgentTask, ChatMessage, ContentBundle, MediaRef,
                                MessageRef, asdict)

_FAKE_WORKER = '''\
import json, os, subprocess, sys, time

def send(message):
    sys.stdout.write(json.dumps(message, ensure_ascii=False) + "\\n")
    sys.stdout.flush()

def recv():
    return json.loads(sys.stdin.readline())

def standard_usage(model_calls=1, tool_calls=0, extra=None):
    usage = {"model_calls": model_calls, "tool_calls": tool_calls, "input_tokens": 80,
             "output_tokens": 40, "cost_usd": 0.008, "input_characters": 120, "complete": True}
    if extra:
        usage.update(extra)
    return usage

def main():
    envelope = json.loads(sys.stdin.readline())
    task = envelope["task"]
    prefs = task.get("preferences") or {}
    behavior = prefs.get("test", "valid")
    bundle = task.get("bundle") or {}
    messages = bundle.get("messages") or []
    mids = [m["ref"]["message_id"] for m in messages]
    media = bundle.get("media") or []
    if behavior == "valid":
        send({"type": "budget_request", "id": "b1", "estimate_usd": 0.01,
              "input_tokens": 100, "output_tokens": 50})
        decision = recv()
        send({"type": "usage", "request_id": decision["id"], "amount_usd": 0.008,
              "complete": True, "tokens": {"input": 80, "output": 40}})
        send({"type": "result", "result": {
            "decision": "eligible", "score": 0.9, "tags": ["fun"], "risks": [],
            "evidence_ids": mids, "reason": "证据充分的猎奇内容",
            "usage": standard_usage(), "inspected_images": [], "direction": None}})
    elif behavior == "invalid_schema":
        send({"type": "result", "result": {
            "decision": "eligible", "score": 5.0, "tags": [], "risks": [],
            "evidence_ids": mids, "reason": "分数越界", "usage": standard_usage(),
            "inspected_images": [], "direction": None}})
    elif behavior == "conflicting_verdict":
        send({"type": "result", "result": {
            "decision": "eligible", "score": 0.9, "risks": ["publication blocker"],
            "evidence_ids": mids, "reason": "Cannot publish the whole bundle",
            "usage": standard_usage(), "inspected_images": [], "direction": None}})
    elif behavior == "tool_overlimit":
        for index in range(3):
            send({"type": "tool_call", "id": "t%d" % index, "name": "get_context",
                  "arguments": {"max_chars": 100}})
            recv()
        send({"type": "result", "result": {
            "decision": "needs_review", "score": 0.0, "tags": [], "risks": [],
            "evidence_ids": [], "reason": "工具超限后放弃",
            "usage": standard_usage(1, 3), "inspected_images": [], "direction": None}})
    elif behavior == "timeout":
        child = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(300)"])
        with open("child.pid", "w") as handle:
            handle.write(str(child.pid))
        with open("started", "w") as handle:
            handle.write("x")
        time.sleep(300)
    elif behavior == "cancel":
        child = subprocess.Popen([sys.executable, "-c", "import signal,time; signal.signal(signal.SIGTERM,signal.SIG_IGN); open('child.ready','w').write('ready'); time.sleep(300)"])
        while not os.path.exists('child.ready'):
            time.sleep(.01)
        with open("child.pid", "w") as handle:
            handle.write(str(child.pid))
        with open("started", "w") as handle:
            handle.write("x")
        time.sleep(60)
    elif behavior == "concurrent":
        with open(prefs["my_sentinel"], "w") as handle:
            handle.write("x")
        deadline = time.time() + 10
        while not os.path.exists(prefs["peer_sentinel"]) and time.time() < deadline:
            time.sleep(0.05)
        if os.path.exists(prefs["peer_sentinel"]):
            send({"type": "result", "result": {
                "decision": "eligible", "score": 0.9, "tags": [], "risks": [],
                "evidence_ids": mids, "reason": "并发运行", "usage": standard_usage(),
                "inspected_images": [], "direction": None}})
        else:
            send({"type": "result", "result": {
                "decision": "needs_review", "score": 0.0, "tags": [], "risks": [],
                "evidence_ids": [], "reason": "not_concurrent", "usage": standard_usage(),
                "inspected_images": [], "direction": None}})
    elif behavior == "env_check":
        bad = []
        if "PULLSHIT_NAPCAT_ACCESS_TOKEN" in os.environ:
            bad.append("qq token leaked")
        if "PULLSHIT_AGENT_API_KEY" not in os.environ:
            bad.append("agent key missing")
        home = os.environ.get("HERMES_HOME", "")
        os.makedirs(home, exist_ok=True)
        if not home or not os.path.isdir(home):
            bad.append("hermes home missing")
        elif os.getcwd() not in home:
            bad.append("cwd outside hermes home")
        if bad:
            send({"type": "error", "code": "env_violation"})
        else:
            send({"type": "result", "result": {
                "decision": "needs_review", "score": 0.0, "tags": [], "risks": [],
                "evidence_ids": [], "reason": "环境隔离正常", "usage": standard_usage(),
                "inspected_images": [], "direction": None}})
    elif behavior == "budget_denied":
        send({"type": "budget_request", "id": "b1", "estimate_usd": 0.01,
              "input_tokens": 100, "output_tokens": 50})
        decision = recv()
        if decision.get("ok"):
            send({"type": "error", "code": "budget_not_denied"})
        else:
            send({"type": "result", "result": {
                "decision": "needs_review", "score": 0.0, "tags": [], "risks": [],
                "evidence_ids": [], "reason": "预算被拒后放弃", "usage": standard_usage(),
                "inspected_images": [], "direction": None}})
    elif behavior == "reserved_unknown":
        send({"type": "budget_request", "id": "b1", "estimate_usd": 0.01,
              "input_tokens": 100, "output_tokens": 50})
        recv()
        send({"type": "usage", "request_id": "b1", "amount_usd": 0.0,
              "complete": False, "tokens": {}})
        os._exit(3)
    elif behavior == "usage_incomplete":
        send({"type": "budget_request", "id": "b1", "estimate_usd": 0.01,
              "input_tokens": 100, "output_tokens": 50})
        recv()
        send({"type": "usage", "request_id": "b1", "amount_usd": 0.0,
              "complete": False, "tokens": {}})
        send({"type": "result", "result": {
            "decision": "needs_review", "score": 0.0, "tags": [], "risks": [],
            "evidence_ids": [], "reason": "用量不完整", "usage": standard_usage(),
            "inspected_images": [], "direction": None}})
    elif behavior == "sleep_then_result":
        time.sleep(float(prefs.get("sleep", 1.0)))
        send({"type": "result", "result": {
            "decision": "eligible", "score": 0.9, "tags": [], "risks": [],
            "evidence_ids": mids, "reason": "按时完成", "usage": standard_usage(),
            "inspected_images": [], "direction": None}})
    elif behavior == "inspect":
        media_path = media[0]["path"]
        send({"type": "tool_call", "id": "t1", "name": "inspect_image",
              "arguments": {"path": "/etc/passwd"}})
        first = recv()
        send({"type": "tool_call", "id": "t2", "name": "inspect_image",
              "arguments": {"path": media_path}})
        second = recv()
        if "error" in second:
            send({"type": "error", "code": second["error"]})
        else:
            description = second.get("result", {}).get("description", "")
            send({"type": "result", "result": {
                "decision": "eligible", "score": 0.9, "tags": [], "risks": [],
                "evidence_ids": mids, "reason": "已检查图片: " + description,
                "usage": standard_usage(1, 2), "inspected_images": [media_path],
                "direction": None}})
    elif behavior == "unverified_claim":
        send({"type": "result", "result": {
            "decision": "eligible", "score": 0.9, "tags": [], "risks": [],
            "evidence_ids": mids, "reason": "声称检查过图片",
            "usage": standard_usage(), "inspected_images": [media[0]["path"]], "direction": None}})
    elif behavior == "feedback":
        result = {"decision": "eligible", "score": 0.9, "tags": [], "risks": [],
                  "evidence_ids": mids, "reason": "反馈解读", "usage": standard_usage(),
                  "inspected_images": []}
        if prefs.get("direction"):
            result["direction"] = prefs["direction"]
        send({"type": "result", "result": result})
    else:
        send({"type": "error", "code": "unknown_behavior"})

main()
'''


class FakeStore:
    def __init__(self, daily_cap=None):
        self.daily_cap = daily_cap
        self.reservations = []
        self.settlements = []
        self.spent = 0.0

    def reserve_budget(self, day, scope, category, amount, daily_budget_usd):
        if self.daily_cap is not None and self.spent + amount > self.daily_cap:
            return None
        self.spent += amount
        rid = 'r%d' % len(self.reservations)
        self.reservations.append([day, scope, category, amount, rid])
        return True

    def settle_budget(self, day, scope, category, reservation, amount):
        for entry in self.reservations:
            if entry[0] == day and entry[3] == reservation:
                self.spent = self.spent - entry[3] + amount
                entry[3] = amount
                self.settlements.append((day, scope, category, reservation, amount))
                return
        raise KeyError('unknown reservation')

    def held(self):
        return {entry[4]: entry[3] for entry in self.reservations}


class Clock:
    def __init__(self):
        self.now = time.time()

    def __call__(self):
        return self.now

    def advance(self, seconds):
        self.now += seconds


class FakeSettings:
    def __init__(self, data):
        self.data = data
        self.root = Path(data['root'])
        self.sources = set(data['authorization']['source_group_ids'])
        self.targets = set(data['authorization']['target_group_ids'])
        self._routes = {(r['source_group_id'], r['target_group_id'])
                        for r in data['authorization']['routes']}

    def path(self, section, key):
        return self.root / self.data[section][key]

    def permits(self, source, target):
        return (source, target) in self._routes


def agent_data(root: Path, **overrides) -> dict:
    data = {
        'root': str(root),
        'agent': {
            'python': sys.executable, 'provider': 'test', 'model': 'test-model',
            'base_url': 'http://127.0.0.1:1', 'api_key_env': 'PULLSHIT_AGENT_API_KEY',
            'vision_model': 'vision-x', 'vision_base_url': '',
            'vision_api_key_env': 'PULLSHIT_VISION_API_KEY',
            'concurrency': 2, 'timeout_seconds': 10, 'model_calls': 8, 'tool_calls': 8,
            'max_output_tokens': 2048, 'max_input_characters': 64000,
            'daily_budget_usd': 10.0, 'task_budget_usd': 2.0,
            'input_usd_per_million': 0.5, 'output_usd_per_million': 1.5,
            'minimum_score': 0.75,
        },
        'storage': {'worker_directory': str(root / 'workers'), 'database_path': '',
                    'media_directory': str(root / 'media')},
        'authorization': {
            'source_group_ids': ['111'], 'target_group_ids': ['222'],
            'routes': [{'source_group_id': '111', 'target_group_id': '222'}],
        },
        'media': {'max_image_bytes': 1048576, 'download_timeout_seconds': 15},
    }
    data['agent'].update(overrides)
    return data


def make_task(root: Path, kind='selection', deadline=None, limits=None,
              preferences=None, source_group='111', target_group='222',
              with_media=False, clock=None) -> AgentTask:
    now = clock() if clock else time.time()
    ref = MessageRef('1', source_group, '1001')
    message = ChatMessage(ref=ref, user_id='5', nickname='张三', occurred_at=now,
                          segments=[{'type': 'text', 'data': '一个有趣的猎奇内容'}])
    media = []
    if with_media:
        media_dir = root / 'media'
        media_dir.mkdir(parents=True, exist_ok=True)
        path = media_dir / 'sample.png'
        path.write_bytes(b'fake-image-bytes')
        media.append(MediaRef(reference_id='media-1', source=ref, path=str(path),
                              sha256='x', size=16, mime='image/png'))
    bundle = ContentBundle(bundle_id='b1', fingerprint='fp', messages=[message],
                           created_at=now, expires_at=now + 86400, media=media)
    return AgentTask(
        task_id='task-%s' % (abs(hash(json.dumps(preferences or {}, sort_keys=True))) % 1000000),
        kind=kind, target_group_id=target_group,
        deadline=deadline if deadline is not None else now + 60,
        bundle=bundle, preferences=preferences or {},
        limits=limits or {'model_calls': 8, 'tool_calls': 8, 'output_tokens': 2048,
                          'input_characters': 64000},
    )


class VisionHandler(http.server.BaseHTTPRequestHandler):
    requests = []

    def do_POST(self):
        length = int(self.headers.get('Content-Length', 0))
        body = json.loads(self.rfile.read(length))
        self.__class__.requests.append((self.headers.get('Authorization'), body))
        payload = json.dumps({
            'choices': [{'message': {'content': '一张有趣的图'}}],
            'usage': {'prompt_tokens': 120, 'completion_tokens': 40},
        }).encode()
        self.send_response(200)
        self.send_header('Content-Type', 'application/json')
        self.send_header('Content-Length', str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)

    def log_message(self, *args):
        pass


class AgentTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.temp = Path(tempfile.mkdtemp(prefix='pullshit-agent-'))
        self.addCleanup(shutil.rmtree, self.temp, ignore_errors=True)
        VisionHandler.requests = []
        self.fake_dir = self.temp / 'fake_modules'
        self.fake_dir.mkdir()
        (self.fake_dir / 'fake_agent_worker.py').write_text(_FAKE_WORKER, encoding='utf-8')
        self.store = FakeStore()
        self.clock = Clock()
        os.environ['PULLSHIT_AGENT_API_KEY'] = 'test-agent-key'
        os.environ['PULLSHIT_VISION_API_KEY'] = 'test-vision-key'
        os.environ['PULLSHIT_NAPCAT_ACCESS_TOKEN'] = 'qq-secret'
        self.addCleanup(os.environ.pop, 'PULLSHIT_AGENT_API_KEY', None)
        self.addCleanup(os.environ.pop, 'PULLSHIT_VISION_API_KEY', None)
        self.addCleanup(os.environ.pop, 'PULLSHIT_NAPCAT_ACCESS_TOKEN', None)

    def make_executor(self, settings=None, store=None, tools=None, clock=None, **agent_overrides):
        root = settings.root if settings else self.temp
        data = agent_data(root, **agent_overrides)
        settings = settings or FakeSettings(data)
        return HermesExecutor(
            settings, store if store is not None else self.store, tools=tools,
            worker_module='fake_agent_worker',
            extra_python_paths=(str(self.fake_dir),),
            clock=clock if clock is not None else self.clock,
        )

    def worker_dir(self, settings):
        return Path(settings.path('storage', 'worker_directory'))

    async def test_valid_result_with_budget_accounting(self):
        settings = FakeSettings(agent_data(self.temp))
        executor = self.make_executor(settings)
        task = make_task(self.temp, clock=self.clock)
        result = await executor.run(task)
        self.assertEqual(result.decision, 'eligible')
        self.assertAlmostEqual(result.score, 0.9)
        self.assertEqual(result.evidence_ids, ['1001'])
        self.assertEqual(len(self.store.reservations), 1)
        self.assertEqual(len(self.store.settlements), 1)
        self.assertEqual(self.store.settlements[0][4], 0.008)
        self.assertEqual(self.store.spent, 0.008)
        self.assertEqual(list(self.worker_dir(settings).iterdir()), [])
        self.assertEqual(result.usage['model_calls'], 1)

    async def test_invalid_schema_becomes_needs_review(self):
        settings = FakeSettings(agent_data(self.temp))
        executor = self.make_executor(settings)
        result = await executor.run(make_task(self.temp, clock=self.clock,
                                              preferences={'test': 'invalid_schema'}))
        self.assertEqual(result.decision, 'needs_review')
        self.assertIn('schema_invalid', result.reason)

    async def test_overlimit_tool_calls_denied_and_flagged(self):
        settings = FakeSettings(agent_data(self.temp))
        calls = []
        async def tools(name, args, task):
            calls.append((name, args))
            return {'bundle': {'messages': []}}
        executor = self.make_executor(settings, tools=tools)
        task = make_task(self.temp, clock=self.clock,
                         limits={'model_calls': 8, 'tool_calls': 2, 'output_tokens': 2048,
                                 'input_characters': 64000},
                         preferences={'test': 'tool_overlimit'})
        result = await executor.run(task)
        self.assertEqual(result.decision, 'needs_review')
        self.assertIn('tool_calls_exceeded', result.reason)
        self.assertEqual(len(calls), 2)

    async def test_denied_source_group_rejected_before_spawn(self):
        settings = FakeSettings(agent_data(self.temp))
        executor = self.make_executor(settings)
        task = make_task(self.temp, clock=self.clock, source_group='999')
        result = await executor.run(task)
        self.assertEqual(result.decision, 'needs_review')
        self.assertEqual(result.reason, 'source_not_authorized')
        worker_dir = self.worker_dir(settings)
        self.assertFalse(worker_dir.exists() and list(worker_dir.iterdir()))
        self.assertEqual(self.store.reservations, [])

    async def test_route_not_permitted(self):
        data = agent_data(self.temp)
        data['authorization']['routes'] = []
        settings = FakeSettings(data)
        executor = self.make_executor(settings)
        result = await executor.run(make_task(self.temp, clock=self.clock))
        self.assertEqual(result.decision, 'needs_review')
        self.assertEqual(result.reason, 'route_not_permitted')

    async def test_past_deadline_rejected_without_spawn(self):
        settings = FakeSettings(agent_data(self.temp))
        executor = self.make_executor(settings)
        task = make_task(self.temp, clock=self.clock, deadline=self.clock() - 5)
        result = await executor.run(task)
        self.assertEqual(result.reason, 'deadline_expired')
        worker_dir = self.worker_dir(settings)
        self.assertFalse(worker_dir.exists() and list(worker_dir.iterdir()))

    async def test_result_after_deadline_flagged(self):
        settings = FakeSettings(agent_data(self.temp, timeout_seconds=15))
        executor = self.make_executor(settings)
        task = make_task(self.temp, clock=self.clock, deadline=self.clock() + 10,
                         preferences={'test': 'sleep_then_result', 'sleep': 1.0})
        runner = asyncio.create_task(executor.run(task))

        async def advance():
            await asyncio.sleep(0.6)
            self.clock.advance(1000)

        advancer = asyncio.create_task(advance())
        result = await runner
        await advancer
        self.assertEqual(result.decision, 'needs_review')
        self.assertIn('deadline_expired', result.reason)

    async def test_timeout_kills_process_group_and_cleans_up(self):
        settings = FakeSettings(agent_data(self.temp, timeout_seconds=3))
        executor = self.make_executor(settings)
        task = make_task(self.temp, clock=self.clock, preferences={'test': 'timeout'})
        runner = asyncio.create_task(executor.run(task))
        child_pid = None
        for _ in range(100):
            matches = list(self.worker_dir(settings).glob('task-*/child.pid'))
            if matches:
                child_pid = int(matches[0].read_text())
                break
            await asyncio.sleep(0.05)
        result = await runner
        self.assertEqual(result.reason, 'timeout')
        self.assertEqual(list(self.worker_dir(settings).iterdir()), [])
        if child_pid is not None:
            for _ in range(40):
                try:
                    os.kill(child_pid, 0)
                except ProcessLookupError:
                    break
                await asyncio.sleep(0.05)
            else:
                self.fail('超时后进程组未被终止')

    async def test_real_worker_module_speaks_protocol(self):
        if importlib.util.find_spec('run_agent') is not None:
            self.skipTest('Hermes SDK 已安装：worker 会走真实 SDK 路径')
        home = self.temp / 'hermes-home'
        envelope = {
            'type': 'task', 'version': 1,
            'task': asdict(make_task(self.temp, clock=self.clock)),
            'config': agent_data(self.temp)['agent'],
        }
        env = {**os.environ, 'HERMES_HOME': str(home)}
        process = await asyncio.create_subprocess_exec(
            sys.executable, '-m', 'pullshit.agent.hermes_worker',
            stdin=asyncio.subprocess.PIPE, stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.DEVNULL, cwd=self.temp, env=env)
        process.stdin.write(json.dumps(envelope, ensure_ascii=False).encode() + b'\n')
        await process.stdin.drain()
        reply = json.loads((await process.stdout.readline()).decode())
        await process.wait()
        self.assertEqual(reply, {'type': 'error', 'code': 'hermes_unavailable'})
        self.assertTrue((home / 'plugins' / 'pullshit' / 'plugin.yaml').is_file())
        self.assertTrue((home / 'plugins' / 'pullshit' / '__init__.py').is_file())
        self.assertIn('enabled: [pullshit]', (home / 'config.yaml').read_text())

    async def test_cancellation_kills_child_sentinel_and_cleans_up(self):
        settings = FakeSettings(agent_data(self.temp, timeout_seconds=60))
        executor = self.make_executor(settings)
        task = make_task(self.temp, clock=self.clock, preferences={'test': 'cancel'})
        runner = asyncio.create_task(executor.run(task))
        taskdir = None
        for _ in range(100):
            matches = list(self.worker_dir(settings).glob('task-*/started'))
            if matches:
                taskdir = matches[0].parent
                break
            await asyncio.sleep(0.05)
        self.assertIsNotNone(taskdir, 'worker 未启动')
        child_pid = int((taskdir / 'child.pid').read_text())
        await executor.cancel(task.task_id)
        result = await runner
        self.assertEqual(result.reason, 'cancelled')
        for _ in range(40):
            try:
                os.kill(child_pid, 0)
            except ProcessLookupError:
                break
            await asyncio.sleep(0.05)
        else:
            self.fail('子进程组未被终止')
        self.assertFalse(taskdir.exists())

    async def test_temp_directory_removed_after_run(self):
        settings = FakeSettings(agent_data(self.temp))
        executor = self.make_executor(settings)
        await executor.run(make_task(self.temp, clock=self.clock))
        self.assertEqual(list(self.worker_dir(settings).iterdir()), [])

    def test_stale_worker_cleanup(self):
        root = self.temp / 'workers'
        root.mkdir()
        old, new, live = root / 'task-old', root / 'task-new', root / 'task-live'
        for directory in (old, new, live):
            directory.mkdir()
        marker = root / 'notes.txt'
        marker.write_text('keep')
        other = root / 'other-dir'
        other.mkdir()
        link = root / 'task-link'
        link.symlink_to(old)
        now = time.time()
        os.utime(old, (now - 3 * 86400, now - 3 * 86400))
        os.utime(live, (now - 3 * 86400, now - 3 * 86400))
        self.assertEqual(clean_stale_workers(root, now=now, live={str(live.resolve())}), 1)
        self.assertFalse(old.exists())
        self.assertTrue(new.exists())
        self.assertTrue(live.exists())
        self.assertTrue(marker.exists())
        self.assertTrue(other.exists())
        self.assertTrue(link.is_symlink())
        executor = self.make_executor()
        self.assertEqual(executor.clean_stale_workers(directory=root, now=now), 1)
        self.assertFalse(live.exists())
        self.assertTrue(new.exists())
        (new/'expiry.json').write_text(json.dumps({'expires_at':now-1}))
        self.assertEqual(clean_stale_workers(root,now=now),1)
        self.assertFalse(new.exists())

    async def test_reserved_budget_kept_on_unknown_usage(self):
        settings = FakeSettings(agent_data(self.temp))
        executor = self.make_executor(settings)
        task = make_task(self.temp, clock=self.clock, preferences={'test': 'reserved_unknown'})
        result = await executor.run(task)
        self.assertEqual(result.decision, 'needs_review')
        self.assertEqual(result.reason, 'worker_died')
        self.assertEqual(self.store.settlements, [])
        self.assertEqual(set(self.store.held().values()), {0.01})
        self.assertEqual(self.store.spent, 0.01)

    async def test_incomplete_usage_keeps_reservation(self):
        settings = FakeSettings(agent_data(self.temp))
        executor = self.make_executor(settings)
        task = make_task(self.temp, clock=self.clock, preferences={'test': 'usage_incomplete'})
        result = await executor.run(task)
        self.assertEqual(result.decision, 'needs_review')
        self.assertEqual(self.store.settlements, [])
        self.assertEqual(set(self.store.held().values()), {0.01})

    async def test_budget_denial_blocks_model_request(self):
        settings = FakeSettings(agent_data(self.temp))
        store = FakeStore(daily_cap=0.001)
        executor = self.make_executor(settings, store=store)
        task = make_task(self.temp, clock=self.clock, preferences={'test': 'budget_denied'})
        result = await executor.run(task)
        self.assertEqual(result.decision, 'needs_review')
        self.assertEqual(store.reservations, [])
        self.assertEqual(store.spent, 0.0)

    async def test_concurrent_runs_two(self):
        settings = FakeSettings(agent_data(self.temp, concurrency=2))
        executor = self.make_executor(settings)
        sentinel_a = self.temp / 'sentinel-a'
        sentinel_b = self.temp / 'sentinel-b'
        task_a = make_task(self.temp, clock=self.clock,
                           preferences={'test': 'concurrent', 'my_sentinel': str(sentinel_a),
                                        'peer_sentinel': str(sentinel_b)})
        task_b = make_task(self.temp, clock=self.clock,
                           preferences={'test': 'concurrent', 'my_sentinel': str(sentinel_b),
                                        'peer_sentinel': str(sentinel_a)})
        results = await asyncio.gather(executor.run(task_a), executor.run(task_b))
        self.assertEqual([r.decision for r in results], ['eligible', 'eligible'])

    async def test_worker_error_code_becomes_needs_review(self):
        settings = FakeSettings(agent_data(self.temp))
        executor = self.make_executor(settings)
        task = make_task(self.temp, clock=self.clock, preferences={'test': 'unknown_behavior'})
        result = await executor.run(task)
        self.assertEqual(result.decision, 'needs_review')
        self.assertEqual(result.reason, 'worker_error:unknown_behavior')

    async def test_child_env_isolation(self):
        settings = FakeSettings(agent_data(self.temp))
        executor = self.make_executor(settings)
        task = make_task(self.temp, clock=self.clock, preferences={'test': 'env_check'})
        result = await executor.run(task)
        self.assertEqual(result.decision, 'needs_review')
        self.assertEqual(result.reason, '环境隔离正常')

    async def test_feedback_task_requires_direction(self):
        settings = FakeSettings(agent_data(self.temp))
        executor = self.make_executor(settings)
        missing = make_task(self.temp, kind='feedback', clock=self.clock,
                            preferences={'test': 'feedback'})
        result = await executor.run(missing)
        self.assertEqual(result.decision, 'needs_review')
        self.assertIn('schema_invalid', result.reason)
        with_direction = make_task(self.temp, kind='feedback', clock=self.clock,
                                   preferences={'test': 'feedback', 'direction': 'positive'})
        result = await executor.run(with_direction)
        self.assertEqual(result.decision, 'eligible')
        self.assertEqual(result.direction, 'positive')

    async def test_inspect_image_default_vision_and_scope(self):
        server = http.server.ThreadingHTTPServer(('127.0.0.1', 0), VisionHandler)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        self.addCleanup(server.shutdown)
        self.addCleanup(server.server_close)
        settings = FakeSettings(agent_data(
            self.temp, vision_base_url='http://127.0.0.1:%d/v1' % server.server_address[1]))
        executor = self.make_executor(settings)
        task = make_task(self.temp, clock=self.clock, with_media=True,
                         preferences={'test': 'inspect'})
        result = await executor.run(task)
        self.assertEqual(result.decision, 'eligible')
        self.assertEqual(result.inspected_images, [task.bundle.media[0].reference_id])
        self.assertEqual(len(VisionHandler.requests), 1)
        auth, body = VisionHandler.requests[0]
        self.assertEqual(auth, 'Bearer test-vision-key')
        self.assertEqual(len(self.store.settlements), 1)
        self.assertAlmostEqual(self.store.settlements[0][4], 120e-6 * 0.5 + 40e-6 * 1.5)

    def test_vision_request_identifies_client_and_preserves_task_session(self):
        for host,session in [('opencode.ai','task-conversation'),('other.example','task-conversation')]:
            with self.subTest(host=host), patch('pullshit.agent.executor.urllib.request.urlopen') as send:
                send.return_value.__enter__.return_value.read.return_value=json.dumps({'choices':[{'message':{'content':'red'}}],'usage':{'prompt_tokens':9,'completion_tokens':3}}).encode()
                result=vision_describe_image('https://'+host+'/v1','sample-model','test-key',b'image','image/png','Describe',100,2,session_id=session)
                request=send.call_args.args[0]; headers={key.lower():value for key,value in request.header_items()}
                self.assertEqual(headers['user-agent'],'pullshit/0.2')
                self.assertEqual(headers['authorization'],'Bearer test-key')
                self.assertEqual(headers.get('x-opencode-session'),session if host=='opencode.ai' else None)
                self.assertEqual(result['input_tokens'],9)

    async def test_unverified_image_claim_flagged(self):
        settings = FakeSettings(agent_data(self.temp))
        executor = self.make_executor(settings)
        task = make_task(self.temp, clock=self.clock, with_media=True,
                         preferences={'test': 'unverified_claim'})
        result = await executor.run(task)
        self.assertEqual(result.decision, 'needs_review')
        self.assertIn('inspected_image_unverified', result.reason)

    async def test_vision_unavailable_reported_to_worker(self):
        settings = FakeSettings(agent_data(self.temp, vision_base_url=''))
        executor = self.make_executor(settings)
        task = make_task(self.temp, clock=self.clock, with_media=True,
                         preferences={'test': 'inspect'})
        result = await executor.run(task)
        self.assertEqual(result.decision, 'needs_review')
        self.assertEqual(result.reason, 'worker_error:vision_unavailable')

    def test_repair_cap_pipeline(self):
        kind = 'selection'
        limits = {'model_calls': 4}
        good = {'decision': 'eligible', 'score': 0.9, 'tags': [], 'risks': [],
                'evidence_ids': [], 'reason': '好', 'direction': None}
        bad_text = {'completed': True, 'final_response': '这不是 JSON'}
        turn_bad = (bad_text, 3, False)
        turn_good = ({'completed': True, 'final_response': json.dumps(good, ensure_ascii=False)}, 4, False)
        self.assertTrue(worker.plan_repair(None, ['schema_object'], 3, 4, False))
        self.assertFalse(worker.plan_repair(None, ['schema_object'], 4, 4, False))
        fields = worker.decide([turn_bad, turn_bad], kind, limits)
        self.assertEqual(fields['decision'], 'needs_review')
        self.assertIn('schema_invalid', fields['reason'])
        fields = worker.decide([turn_bad, turn_good], kind, limits)
        self.assertEqual(fields['decision'], 'eligible')
        exhausted = ({'completed': False, 'failed': False, 'final_response': ''}, 8, False)
        fields = worker.decide([exhausted], kind, {'model_calls': 8})
        self.assertEqual(fields['decision'], 'needs_review')
        self.assertIn('model_failed', fields['reason'])
        failed = ({'completed': True, 'failed': True,
                   'final_response': json.dumps(good, ensure_ascii=False)}, 4, True)
        fields = worker.decide([failed], kind, limits)
        self.assertIn('model_failed', fields['reason'])

    def test_parse_and_normalize_helpers(self):
        self.assertIsNone(worker.parse_analysis_json('完全没有 JSON'))
        fenced = '结论如下：\n```json\n{"decision": "rejected", "score": 0.1, "reason": "低质"}\n```'
        self.assertEqual(worker.parse_analysis_json(fenced)['decision'], 'rejected')
        from pullshit.agent import protocol
        analysis, errors = protocol.normalize_analysis(
            {'decision': 'eligible', 'score': 0.8, 'reason': '好', 'direction': None}, 'selection')
        self.assertEqual(analysis['decision'], 'eligible')
        self.assertEqual(errors, [])
        analysis, errors = protocol.normalize_analysis(
            {'decision': 'eligible', 'score': 0.8, 'reason': '好'}, 'feedback')
        self.assertIsNone(analysis)
        self.assertIn('schema_direction', errors)

    def test_instruction_budget_truncation(self):
        task = make_task(self.temp, clock=self.clock)
        task.bundle.messages[0].segments = [{'type': 'text', 'data': 'x' * 5000}]
        limits = {'model_calls': 8, 'tool_calls': 8, 'output_tokens': 2048,
                  'input_characters': 2000}
        instruction, chars = worker.build_instruction(task, limits)
        self.assertLessEqual(chars, 2000)
        self.assertIn('x' * 50, instruction)
        self.assertIn('…', instruction)
        self.assertIn('submit_analysis', instruction)
        with self.assertRaisesRegex(ValueError,'input_character_limit'):
            worker.build_instruction(task,limits,allow_truncation=False)

    def test_publication_instructions_are_scoped_to_selection(self):
        task=make_task(self.temp,clock=self.clock)
        selection,_=worker.build_instruction(task,{'input_characters':64000})
        task.kind='feedback'
        feedback,_=worker.build_instruction(task,{'input_characters':64000})
        self.assertIn('整个内容包会按顺序原样转发',selection)
        self.assertNotIn('整个内容包会按顺序原样转发',feedback)
        self.assertIn('反馈信号',feedback)

    async def test_submit_checks_evidence_before_acceptance(self):
        executor=self.make_executor(); task=make_task(self.temp,clock=self.clock)
        run=SimpleNamespace(task=task,submitted=None)
        analysis={'decision':'eligible','score':.9,'reason':'verified','evidence_ids':['999999']}
        denied=await executor._invoke_tool(run,'submit_analysis',{'analysis':analysis})
        self.assertFalse(denied['accepted']); self.assertIn('evidence_out_of_scope',denied['errors']); self.assertIsNone(run.submitted)
        analysis['evidence_ids']=[task.bundle.messages[0].ref.key]
        accepted=await executor._invoke_tool(run,'submit_analysis',{'analysis':analysis})
        self.assertTrue(accepted['accepted']); self.assertEqual(run.submitted['evidence_ids'],analysis['evidence_ids'])

    async def test_conflicting_verdict_is_rejected_and_can_be_repaired(self):
        executor=self.make_executor(); task=make_task(self.temp,clock=self.clock)
        run=SimpleNamespace(task=task,submitted=None)
        analysis={'decision':'eligible','score':.9,'reason':'Whole bundle needs editing',
                  'risks':['A record must be removed'],'evidence_ids':[task.bundle.messages[0].ref.key]}
        denied=await executor._invoke_tool(run,'submit_analysis',{'analysis':analysis})
        self.assertFalse(denied['accepted'])
        self.assertIn('eligible_with_blocking_risks',denied['errors'])
        self.assertIsNone(run.submitted)
        analysis['decision']='needs_review'
        accepted=await executor._invoke_tool(run,'submit_analysis',{'analysis':analysis})
        self.assertTrue(accepted['accepted']); self.assertEqual(run.submitted['risks'],analysis['risks'])
        analysis.update(decision='eligible',risks=[],reason='Old meme is independently understandable')
        accepted=await executor._invoke_tool(run,'submit_analysis',{'analysis':analysis})
        self.assertTrue(accepted['accepted'])

    async def test_final_response_cannot_bypass_blocking_risk_check(self):
        executor=self.make_executor()
        task=make_task(self.temp,clock=self.clock,preferences={'test':'conflicting_verdict'})
        result=await executor.run(task)
        self.assertEqual(result.decision,'needs_review')
        self.assertIn('eligible_with_blocking_risks',result.reason)

    def test_context_character_cap_returns_valid_failure(self):
        executor=self.make_executor(); task=make_task(self.temp,clock=self.clock)
        result=executor._default_get_context(task,{'max_chars':1})
        self.assertEqual(result,{'error':'context_character_limit'})

    def test_plugin_registration_against_fake_context(self):
        registrations = []
        class FakeContext:
            def register_tool(self, **kwargs):
                registrations.append(('tool', kwargs['name'], kwargs['toolset']))
            def register_hook(self, kind, callback):
                registrations.append(('hook', kind))
            def register_middleware(self, kind, callback):
                registrations.append(('middleware', kind))
        plugin.register(FakeContext())
        names = [entry[1] for entry in registrations if entry[0] == 'tool']
        self.assertEqual(names, ['get_context', 'inspect_image', 'get_group_preferences',
                                 'submit_analysis'])
        for entry in registrations:
            if entry[0] == 'tool':
                self.assertEqual(entry[2], 'pullshit')
        kinds = [entry[1] for entry in registrations if entry[0] == 'middleware']
        self.assertEqual(kinds, ['llm_execution', 'llm_request'])


if __name__ == '__main__':
    unittest.main()
