import asyncio
import json
from pathlib import Path
import threading
import urllib.error
import urllib.request

from pullshit.contracts import ChatMessage, OutcomeUnknown, PlatformRejected
from pullshit.entrypoints.simulation_api import LocalServer, SimulationAPI, handler_for
from pullshit.storage.simulation_receipts import SimulationReceipts
from tests.support import Workspace

TOKEN = 'simulation-test-token-not-a-real-secret'


class Gateway:
    account_id = '900001'
    sent = []
    error = None
    returned_qq = None
    readback_error = False

    def __init__(self, settings): pass
    async def __aenter__(self): return self
    async def __aexit__(self, *args): pass

    async def send_forward(self, group, nodes, *, before_send=None):
        before_send()
        self.sent.append((group, nodes))
        if self.error: raise self.error
        return '12345'

    async def fetch_message(self, ref):
        if self.readback_error: raise OutcomeUnknown('test readback failed')
        return ChatMessage(ref, self.account_id, 'bot', 1,
                           [{'type': 'forward', 'data': {'id': 'resource'}}], own=True)

    async def fetch_forward(self, ref, resource):
        result = []
        for i, node in enumerate(self.sent[-1][1]):
            data = node['data']
            qq = self.returned_qq if i == 1 and self.returned_qq else data['user_id']
            result.append({'sender': {'user_id': qq}, 'content': data['content']})
        return result


class SimulationApiTests(Workspace):
    def setUp(self):
        super().setUp()
        self.store()
        Gateway.sent = []; Gateway.error = None; Gateway.returned_qq = None
        Gateway.readback_error = False; Gateway.account_id = '900001'
        self.path = self.root / 'data/development/simulation-receipts.json'
        self.receipts = SimulationReceipts(self.path, clock=lambda: self.current)
        self.api = self.make_api()
        self.body = {'qq': '900401', 'content': '我要好好学习', 'group_id': '900201'}

    def make_api(self, receipts=None):
        return SimulationAPI(lambda: self.settings, '900001', TOKEN,
                             receipts or self.receipts, gateway_factory=Gateway,
                             clock=lambda: self.current)

    def post(self, key='test-request-01', **changes):
        return self.api.post(key, json.dumps({**self.body, **changes}).encode())

    def test_disclosure_is_present_and_literal_text_is_preserved(self):
        content = '[CQ:node,id=123] neutral test'
        code, result = self.post(content=content)
        self.assertEqual(code, 201)
        self.assertTrue(result['identity_verified']); self.assertTrue(result['content_verified'])
        self.assertTrue(result['disclosure_verified'])
        first, second = Gateway.sent[0][1]
        self.assertEqual(first['data']['user_id'], '900001')
        self.assertIn('非本人发言', first['data']['content'][0]['data']['text'])
        self.assertEqual(second['data']['user_id'], '900401')
        self.assertEqual(second['data']['content'], [{'type': 'text', 'data': {'text': content}}])
        self.assertIn('模拟测试', second['data']['nickname'])
        self.assertNotIn(content, self.path.read_text())
        self.assertEqual(self.path.stat().st_mode & 0o777, 0o600)

    def test_replay_after_restart_is_idempotent_and_conflicting_body_is_rejected(self):
        code, result = self.post()
        receipts = SimulationReceipts(self.path, clock=lambda: self.current)
        self.api = self.make_api(receipts)
        code2, replayed = self.post()
        self.assertEqual(code2, 200); self.assertEqual(result, replayed)
        self.assertEqual(len(Gateway.sent), 1)
        self.assertEqual(self.post(content='different')[0], 409)
        self.assertEqual(len(Gateway.sent), 1)

    def test_unknown_receipt_is_not_resent_after_restart(self):
        Gateway.error = OutcomeUnknown('not a receipt')
        self.assertEqual(self.post()[0], 202)
        self.api = self.make_api(SimulationReceipts(self.path, clock=lambda: self.current))
        self.assertEqual(self.post()[0], 202)
        self.assertEqual(len(Gateway.sent), 1)

    def test_crash_intent_prevents_resending(self):
        from pullshit.contracts import digest
        request = self.body
        self.receipts.start('test-request-01', digest(['900001', request]),
                            {'status': 'unknown', 'request_id': 'test-request-01'})
        self.api = self.make_api(SimulationReceipts(self.path, clock=lambda: self.current))
        self.assertEqual(self.post()[0], 202)
        self.assertEqual(Gateway.sent, [])

    def test_account_mismatch_and_unauthorized_target_never_send(self):
        self.assertEqual(self.post(group_id='999999')[0], 403)
        Gateway.account_id = '900002'
        self.assertEqual(self.post()[0], 502)
        self.assertEqual(Gateway.sent, [])

    def test_disabled_and_paused_sending_never_send(self):
        self.settings.data['sending']['enabled'] = False
        self.assertEqual(self.post()[0], 403)
        self.settings.data['sending']['enabled'] = True
        self.store().set_paused('900001', '900201', True)
        self.assertEqual(self.post()[0], 403)
        self.assertEqual(Gateway.sent, [])

    def test_pause_is_rechecked_at_the_send_boundary(self):
        store = self.store()
        class PausingGateway(Gateway):
            async def __aenter__(self):
                store.set_paused('900001', '900201', True)
                return self
        self.api.gateway_factory = PausingGateway
        code, result = self.post()
        self.assertEqual(code, 502)
        self.assertEqual(result['error'], 'rules_changed')
        self.assertEqual(Gateway.sent, [])

    def test_acknowledgement_survives_gateway_shutdown_failure(self):
        class FailingCloseGateway(Gateway):
            async def __aexit__(self, *args):
                raise OutcomeUnknown('test close failed after acknowledgement')
        self.api.gateway_factory = FailingCloseGateway
        code, result = self.post()
        self.assertEqual(code, 201)
        self.assertEqual(result['message_id'], '12345')
        self.api = self.make_api(SimulationReceipts(self.path, clock=lambda: self.current))
        self.assertEqual(self.post()[0], 200)
        self.assertEqual(len(Gateway.sent), 1)

    def test_full_receipt_file_preserves_unexpired_idempotency_records(self):
        self.receipts.capacity = 1
        self.assertEqual(self.post()[0], 201)
        self.assertEqual(self.post(key='test-request-02')[0], 503)
        self.assertEqual(self.post()[0], 200)
        self.assertEqual(len(Gateway.sent), 1)

    def test_identity_mismatch_is_reported_separately_from_sent(self):
        Gateway.returned_qq = '900999'
        code, result = self.post()
        self.assertEqual(code, 201); self.assertEqual(result['status'], 'sent')
        self.assertFalse(result['identity_verified']); self.assertEqual(result['observed_qq'], '900999')
        self.assertTrue(result['content_verified'])

    def test_readback_failure_preserves_send_receipt(self):
        Gateway.readback_error = True
        code, result = self.post()
        self.assertEqual(code, 201); self.assertEqual(result['message_id'], '12345')
        self.assertIsNone(result['identity_verified'])
        self.assertEqual(result['verification_error'], 'readback_unavailable')

    def test_explicit_platform_failure_is_cached(self):
        Gateway.error = PlatformRejected('test rejected')
        self.assertEqual(self.post()[0], 502); self.assertEqual(self.post()[0], 502)
        self.assertEqual(len(Gateway.sent), 1)

    def test_validation_rate_limit_and_expiry(self):
        self.assertEqual(self.post(qq=True)[0], 400)
        self.assertEqual(self.post(content=' ')[0], 400)
        self.assertEqual(self.post(content='x' * 2001)[0], 400)
        self.assertEqual(self.post(disclosure=False)[0], 400)
        for i in range(5): self.assertEqual(self.post(key=f'test-request-{i:02}')[0], 201)
        self.assertEqual(self.post(key='test-request-99')[0], 429)
        self.current += self.receipts.lifetime + 1
        self.receipts.purge()
        self.assertEqual(json.loads(self.path.read_text()), {})

    def test_real_http_authentication_routes_and_body_limits(self):
        server = LocalServer(('127.0.0.1', 0), handler_for(self.api))
        self.addCleanup(server.server_close); self.addCleanup(server.shutdown)
        thread = threading.Thread(target=server.serve_forever, daemon=True); thread.start()
        base = f'http://127.0.0.1:{server.server_port}'
        def request(path, body=None, **headers):
            r = urllib.request.Request(base + path, data=body, headers=headers)
            try: response = urllib.request.urlopen(r, timeout=5)
            except urllib.error.HTTPError as error: response = error
            with response: return response.status, json.load(response)
        self.assertEqual(request('/health')[0], 401)
        headers = {'Authorization': 'Bearer ' + TOKEN, 'Content-Type': 'application/json',
                   'Idempotency-Key': 'http-request-01'}
        self.assertEqual(request('/health', **headers)[0], 200)
        code, result = request('/v1/simulation-forwards', json.dumps(self.body).encode(), **headers)
        self.assertEqual(code, 201)
        self.assertEqual(request('/v1/simulation-forwards/http-request-01', **headers)[1], result)
        self.assertEqual(request('/v1/simulation-forwards', b'x' * 16385, **headers)[0], 413)
        self.assertEqual(len(Gateway.sent), 1)
