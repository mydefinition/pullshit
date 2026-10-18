"""Exercise SystemOne through the real worker, parent budgets and local HTTP."""
import asyncio
from dataclasses import asdict
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import os
from pathlib import Path
import sys
import threading
import time
from unittest.mock import patch

from pullshit.agent.executor import HermesExecutor
from pullshit.config import agent_config, ConfigError, load_settings
from pullshit.contracts import AgentTask, ChatMessage, ContentBundle, MediaRef, MessageRef, digest
from pullshit.content import selection
from tests.support import Workspace


class DecisionAgentTests(Workspace):
    def setUp(self):
        super().setUp()
        self.requests = []
        self.transform = lambda body, response: response
        self.http_status = 200
        self.mapping = {'U001': 'U001', 'U002': 'discard', 'U003': 'U003'}
        self.chance = .98
        owner = self
        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *args): pass
            def do_POST(self):
                body = json.loads(self.rfile.read(int(self.headers['Content-Length'])))
                owner.requests.append((self.path, body, self.headers.get('Authorization')))
                if self.path == '/chat/completions':
                    response = {'choices': [{'message': {'content': '一张包含独立幽默反转的梗图'}}],
                                'usage': {'prompt_tokens': 50, 'completion_tokens': 20}}
                else:
                    answers = {}
                    for key, question in body['questions'].items():
                        if question['type'] == 'choice':
                            choice = owner.mapping.get(key, key)
                            answers[key] = {'type': 'choice', 'choice': choice, 'confidence': .98,
                                'probabilities': {option: 1.0 if option == choice else 0.0 for option in question['criteria']}}
                        else:
                            answers[key] = {'type': 'noul', 'noul': owner.chance}
                    response = owner.transform(body, {'answers': answers, 'model': 'local-decision',
                                                      'usage': {'input_tokens': 100, 'output_tokens': 0}})
                raw = json.dumps(response).encode()
                self.send_response(owner.http_status); self.send_header('Content-Length', str(len(raw)))
                self.end_headers()
                try: self.wfile.write(raw)
                except (BrokenPipeError, ConnectionResetError): pass
        server = ThreadingHTTPServer(('127.0.0.1', 0), Handler)
        thread = threading.Thread(target=server.serve_forever, daemon=True); thread.start()
        self.addCleanup(lambda: (server.shutdown(), server.server_close(), thread.join()))
        self.base = f'http://127.0.0.1:{server.server_port}'
        self.local['agent'] = {'python': '/nonexistent-hermes-python', 'provider': 'custom',
            'model': 'feedback-llm', 'base_url': 'http://127.0.0.1:1', 'api_key_env': 'TEST_LLM_KEY',
            'daily_budget_usd': 1., 'task_budget_usd': .05, 'input_usd_per_million': .3,
            'output_usd_per_million': 1.2,
            'selection': {'api_format': 'systemone', 'model': 'local-decision', 'base_url': self.base,
                          'api_key_env': 'TEST_DECISION_KEY', 'free': True},
            'decision': {'think_mode': 'never'}}
        self.write_config(); self.settings = load_settings(self.root, 'development').with_account('900001')
        self.addCleanup(patch.stopall)
        patch.dict(os.environ, {'TEST_LLM_KEY': 'llm-secret', 'TEST_DECISION_KEY': 'decision-secret'}).start()

    def task(self, texts=None):
        now = time.time()
        texts = texts or ['独立笑话：健身卡办成年卡，唯一变瘦的是钱包。', '@小王 你说的是谁？']
        messages = [ChatMessage(MessageRef('900001', '900101', str(i+1)), '900401', '合成用户', now+i/10,
                               [{'type': 'text', 'data': {'text': text}}]) for i, text in enumerate(texts)]
        bundle = ContentBundle(digest(texts), digest(texts), messages, now, now+3600)
        return AgentTask('decision-test', 'selection', '900201', now+10, bundle,
                         limits={'model_calls': 8, 'tool_calls': 8, 'output_tokens': 2048, 'input_characters': 64000})

    def execute(self, task=None, **kwargs):
        return asyncio.run(HermesExecutor(self.settings, self.store(), **kwargs).run(task or self.task()))

    def test_real_worker_selects_disjoint_groups_and_settles_free_usage(self):
        self.mapping = {'U001': 'U001', 'U002': 'U001', 'U003': 'discard', 'U004': 'U004'}
        task = self.task(['段子前半句', '段子后半句', '@小王？', '另一份独立内容'])
        result = self.execute(task)
        self.assertEqual(result.decision, 'eligible', result.reason)
        self.assertEqual([item['publish_ids'] for item in result.selections], [['U001', 'U002'], ['U004']])
        self.assertEqual(result.usage['cost_usd'], 0)
        self.assertTrue(result.usage['complete'])
        self.assertEqual(result.usage['model_calls'], 2)
        self.assertEqual(result.usage['output_tokens'], 0)
        self.assertIsNone(selection.validate(task, result, []))
        self.assertEqual(len(selection.publications(task, result)), 2)
        self.assertTrue(all(request[2] == 'Bearer decision-secret' for request in self.requests))
        self.assertNotIn('messages', self.requests[0][1])
        self.assertEqual(list(self.settings.path('storage', 'worker_directory').iterdir()), [])

    def test_all_chatter_rejected_and_uncertain_value_requires_review(self):
        self.mapping = {'U001': 'discard', 'U002': 'discard'}
        result = self.execute(); self.assertEqual(result.decision, 'rejected')
        self.assertEqual(len(self.requests), 1)
        self.mapping['U001'] = 'U001'; self.chance = .5
        result = self.execute(); self.assertEqual(result.decision, 'needs_review')
        self.assertFalse(result.selections)

    def test_uncertain_member_withholds_its_dependants_and_preserves_independent_group(self):
        self.mapping = {'U001': 'U001', 'U002': 'U003', 'U003': 'U003'}
        def uncertain(body, response):
            if 'U003' in body['questions']:
                answer = response['answers']['U003']
                answer['confidence'] = .3
                count = len(answer['probabilities'])
                answer['probabilities'] = {key: .5 if key == 'U003' else .5/(count-1) for key in answer['probabilities']}
            return response
        self.transform = uncertain
        task = self.task(['完整段子', '依赖不确定内容的前半段', '不确定的后半段'])
        result = self.execute(task)
        self.assertEqual(result.decision, 'eligible', result.reason)
        self.assertEqual([item['publish_ids'] for item in result.selections], [['U001']])
        self.assertIsNone(selection.validate(task, result, []))

    def test_bad_or_missing_answers_usage_and_choices_fail_closed(self):
        changes = [
            lambda b, r: {**r, 'answers': {}},
            lambda b, r: {**r, 'usage': {}},
            lambda b, r: {**r, 'usage': {'input_tokens': True, 'output_tokens': 0}},
            lambda b, r: {**r, 'answers': {**r['answers'], 'U001': {**r['answers']['U001'], 'choice': None}}},
            lambda b, r: {**r, 'answers': {**r['answers'], 'U001': {**r['answers']['U001'], 'confidence': float('nan')}}},
            lambda b, r: {**r, 'answers': {**r['answers'], 'U001': {**r['answers']['U001'], 'none_probability': .8}}},
        ]
        for transform in changes:
            with self.subTest(transform=transform):
                self.transform = transform
                result = self.execute()
                self.assertEqual(result.decision, 'needs_review', asdict(result))
                self.assertFalse(result.selections)

    def test_http_failures_and_limits_stop_without_extra_calls(self):
        for code in (401, 422, 429, 502):
            self.http_status = code
            result = self.execute()
            self.assertIn('decision_http_'+str(code), result.reason)
        self.http_status = 200
        task = self.task(); task.limits['model_calls'] = 1
        before = len(self.requests); result = self.execute(task)
        self.assertEqual(len(self.requests), before+1)
        self.assertIn('model_call_limit', result.reason)
        task = self.task(); task.limits['input_characters'] = 100
        before = len(self.requests); result = self.execute(task)
        self.assertEqual(len(self.requests), before)
        self.assertIn('decision_input_character_limit', result.reason)

    def test_paid_budget_denial_occurs_before_http(self):
        self.settings.data['agent']['selection']['free'] = False
        self.settings.data['agent']['task_budget_usd'] = .000001
        result = self.execute()
        self.assertEqual(len(self.requests), 0)
        self.assertIn('budget_denied', result.reason)

    def test_images_use_llm_vision_and_remain_verified_for_publication(self):
        import hashlib
        task = self.task(['图片正文'])
        path = self.root/'picture.png'; path.write_bytes(b'synthetic-image')
        ref = MediaRef('image1', task.bundle.messages[0].ref, str(path), hashlib.sha256(path.read_bytes()).hexdigest(),
                       path.stat().st_size, 'image/png', time.time()+1000)
        task.bundle.media.append(ref)
        task.bundle.messages[0].segments = [{'type': 'image', 'data': {'file': str(path)}}]
        self.settings.data['agent'].update(vision_model='vision-llm', vision_base_url=self.base,
            vision_api_key_env='TEST_LLM_KEY', vision_input_usd_per_million=.3, vision_output_usd_per_million=1.2)
        result = self.execute(task)
        self.assertEqual(result.decision, 'eligible', result.reason)
        self.assertEqual(result.inspected_images, ['image1'])
        self.assertIsNone(selection.validate(task, result, result.inspected_images))
        self.assertEqual(self.requests[0][0], '/chat/completions')
        self.assertEqual(self.requests[0][2], 'Bearer llm-secret')
        self.assertIn('image_description', json.dumps(self.requests[1][1]))
        self.assertGreater(result.usage['cost_usd'], 0)
        self.assertTrue(result.usage['complete'])

    def test_reply_expansion_keeps_authoritative_snapshot(self):
        task = self.task(['后半段包袱'])
        task.bundle.messages[0].segments.insert(0, {'type': 'reply', 'data': {'id': '99'}})
        store = self.store()
        old = ChatMessage(MessageRef('900001', '900101', '99'), '900402', '前文', time.time()-1,
                          [{'type': 'text', 'data': {'text': '前半段铺垫'}}], expires_at=time.time()+1000)
        store.put_message(old)
        def respond(body, response):
            if 'U002' in body['questions'] and len(body['state']['units']) == 2:
                # U001 is the reply segment; U002 is its dependent text.
                response['answers']['U002'].update(choice='needs_context', probabilities={k: float(k=='needs_context') for k in body['questions']['U002']['criteria']})
            return response
        self.mapping = {'U002': 'U002', 'U003': 'U002'}
        self.transform = respond
        result = asyncio.run(HermesExecutor(self.settings, store).run(task))
        self.assertEqual(result.decision, 'eligible', result.reason)
        self.assertEqual(result.snapshot_revision, 1)
        self.assertEqual(result.selections[0]['publish_ids'], ['U002', 'U003'])
        self.assertIsNone(selection.validate(task, result, []))

    def test_feedback_routes_to_llm_with_independent_connector(self):
        module = self.root/'llm_probe.py'
        module.write_text('import sys,json\ne=json.loads(sys.stdin.readline())\nprint(json.dumps({"type":"result","result":{"decision":"needs_review","direction":"unknown","inspected_images":[],"reason":e["config"]["model"]}}))\n')
        self.settings.data['agent']['python'] = sys.executable
        task = self.task(); task.kind = 'feedback'
        result = self.execute(task, worker_module='llm_probe', extra_python_paths=[self.root])
        self.assertEqual(result.reason, 'feedback-llm')
        self.assertEqual(self.requests, [])
        self.settings.data['agent']['feedback']['api_format'] = 'systemone'
        result = self.execute(task)
        self.assertEqual(result.reason, 'feedback_requires_llm')

    def test_decision_requests_are_serialized_and_waiting_task_checks_deadline(self):
        started, release = threading.Event(), threading.Event()
        def hold_first(body, response):
            if not started.is_set():
                started.set(); release.wait(3)
            return response
        self.transform = hold_first
        async def scenario():
            executor = HermesExecutor(self.settings, self.store())
            first = self.task(); second = self.task()
            first.task_id = 'first'; second.task_id = 'second'
            running = asyncio.create_task(executor.run(first))
            try:
                self.assertTrue(await asyncio.to_thread(started.wait, 3))
                second.deadline = time.time() + .05
                waiting = asyncio.create_task(executor.run(second))
                await asyncio.sleep(.1)
                self.assertEqual(len(self.requests), 1)
            finally:
                release.set()
            result, expired = await asyncio.gather(running, waiting)
            self.assertEqual(result.decision, 'eligible', result.reason)
            self.assertEqual(expired.reason, 'deadline_expired')
            self.assertEqual(len(self.requests), 2)
        asyncio.run(scenario())

    def test_cancel_interrupts_http_worker_and_cleans_task_directory(self):
        started, release = threading.Event(), threading.Event()
        def hold(body, response):
            started.set(); release.wait(3)
            return response
        self.transform = hold
        async def scenario():
            executor = HermesExecutor(self.settings, self.store())
            task = self.task()
            running = asyncio.create_task(executor.run(task))
            try:
                self.assertTrue(await asyncio.to_thread(started.wait, 3))
                await executor.cancel(task.task_id)
                result = await asyncio.wait_for(running, 2)
                self.assertEqual(result.reason, 'cancelled')
                self.assertFalse(executor._runs)
                self.assertEqual(list(self.settings.path('storage', 'worker_directory').iterdir()), [])
            finally:
                release.set()
        asyncio.run(scenario())

    def test_role_configuration_inheritance_and_feedback_restriction(self):
        self.assertEqual(agent_config(self.settings.data, 'selection')['model'], 'local-decision')
        self.assertEqual(agent_config(self.settings.data, 'feedback')['model'], 'feedback-llm')
        self.local['agent']['feedback'] = {'api_format': 'systemone'}
        self.write_config()
        with self.assertRaises(ConfigError): load_settings(self.root, 'development')
