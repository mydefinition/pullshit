"""Tests for ingestion event normalization and the NapCat gateway."""
from __future__ import annotations

import asyncio
import json
import os
import unittest
from pathlib import Path

import websockets

from pullshit.config import Settings
from pullshit.contracts import DAY, ChatEvent, MessageRef, OutcomeUnknown, PlatformRejected
from pullshit.ingestion.events import normalize
from pullshit.ingestion.napcat import NapCatGateway, PayloadMismatch

FIXTURES = Path(__file__).resolve().parent / 'fixtures'


def load_fixture(name):
    return [json.loads(line) for line in (FIXTURES / name).read_text().splitlines() if line.strip()]


def gateway_settings(endpoint: str, timeout: float = 2.0) -> Settings:
    data = {
        'napcat': {'endpoint': endpoint, 'access_token_env': 'PULLSHIT_NAPCAT_ACCESS_TOKEN',
                   'request_timeout_seconds': timeout, 'reconnect_initial_seconds': 1,
                   'reconnect_max_seconds': 30},
        'authorization': {'source_group_ids': ['900101', '900102'], 'target_group_ids': ['900201'],
                          'operator_qq_ids': []},
        'retention': {'message_days': 3},
    }
    return Settings(Path.cwd(), 'development', data)


def message_payload(group_id=900102, message_id=101, user_id=900401, time=1791590791,
                    segments=None, sender=None):
    return {'time': time, 'message_type': 'group', 'message_id': message_id, 'group_id': group_id,
            'user_id': user_id,
            'sender': sender or {'user_id': user_id, 'nickname': 'Alice', 'role': 'member'},
            'message': segments or [{'type': 'text', 'data': {'text': 'hello'}}]}


class NormalizeTests(unittest.TestCase):
    def test_sanitized_fixture(self):
        events = [normalize(raw, '900001', {'900101', '900102'}, {'900101'})
                  for raw in load_fixture('napcat-sanitized-events.jsonl')]
        events = [event for event in events if event is not None]
        self.assertEqual([event.kind for event in events],
                         ['message', 'message', 'message', 'reaction', 'message'])
        messages = [event.message for event in events if event.kind == 'message']
        self.assertEqual([message.ref.message_id for message in messages], ['100', '101', '102', '103'])
        self.assertTrue(messages[0].own)
        self.assertTrue(all(not message.own for message in messages[1:]))
        self.assertEqual([segment['type'] for segment in messages[1].segments], ['at', 'text'])
        self.assertEqual(messages[3].ref.group_id, '900101')
        reaction = [event for event in events if event.kind == 'reaction'][0]
        self.assertEqual(reaction.ref.message_id, '100')
        self.assertEqual(reaction.reaction, '66')
        self.assertIs(reaction.added, True)
        self.assertEqual(reaction.user_id, '900402')

    def test_synthetic_fixture(self):
        events = [normalize(raw, '900001', {'900101'}, {'900201'})
                  for raw in load_fixture('synthetic-events.jsonl')]
        events = [event for event in events if event is not None]
        self.assertEqual(len(events), 8)
        kinds = [event.kind for event in events]
        self.assertEqual(kinds.count('message'), 6)
        self.assertEqual(kinds.count('recall'), 1)
        self.assertEqual(kinds.count('reaction'), 1)
        recall = [event for event in events if event.kind == 'recall'][0]
        self.assertEqual(recall.ref.message_id, '1')
        self.assertEqual(recall.user_id, '900401')
        own = [event for event in events if event.kind == 'message' and event.message.own]
        self.assertEqual([event.ref.message_id for event in own], ['77'])

    def test_signed_message_ids(self):
        raw = {'post_type': 'message', 'message_type': 'group', 'time': 1000, 'self_id': 900001,
               'group_id': 900101, 'message_id': -123, 'user_id': 900401,
               'message': [{'type': 'text', 'data': {'text': 'x'}}]}
        event = normalize(raw, '900001', {'900101'}, set())
        self.assertEqual(event.ref.message_id, '-123')
        self.assertEqual(event.event_id, event.ref.key)
        for bad in ('abc', '1.5', True, None):
            raw['message_id'] = bad
            self.assertIsNone(normalize(raw, '900001', {'900101'}, set()), f'message_id={bad}')

    def test_ignored_events(self):
        base = {'time': 1000, 'self_id': 900001, 'group_id': 900101}
        cases = [
            {'post_type': 'meta_event', 'meta_event_type': 'heartbeat'},
            {'post_type': 'message', 'message_type': 'private', 'message_id': 1,
             'user_id': 900401, 'message': []},
            {'post_type': 'notice', 'notice_type': 'group_increase'},
            {'post_type': 'request'},
            {'post_type': 'message', 'message_type': 'group', 'message_id': 1,
             'user_id': 900401, 'message': [], 'self_id': 999999},
            {'post_type': 'message', 'message_type': 'group', 'message_id': 1,
             'user_id': 900401, 'message': [], 'group_id': 900999},
            {'post_type': 'message', 'message_type': 'group', 'message_id': 1,
             'user_id': 900401, 'message': []},
        ]
        for case in cases:
            raw = {**base, **case}
            self.assertIsNone(normalize(raw, '900001', {'900101'}, set()), str(case))

    def test_message_days_expiry(self):
        raw = {'post_type': 'message', 'message_type': 'group', 'time': 1000, 'self_id': 900001,
               'group_id': 900101, 'message_id': 1, 'user_id': 900401,
               'message': [{'type': 'text', 'data': {'text': 'x'}}]}
        event = normalize(raw, '900001', {'900101'}, set(), message_days=5)
        self.assertEqual(event.message.expires_at, 1000 + 5 * DAY)

    def test_recall_of_unknown_message_normalizes(self):
        raw = {'post_type': 'notice', 'notice_type': 'group_recall', 'time': 1000,
               'self_id': 900001, 'group_id': 900101, 'message_id': 424242,
               'operator_id': 900401}
        event = normalize(raw, '900001', {'900101'}, set())
        self.assertEqual(event.kind, 'recall')
        self.assertEqual(event.ref.message_id, '424242')
        self.assertIsNone(event.message)

    def test_reaction_without_is_add(self):
        raw = {'post_type': 'notice', 'notice_type': 'group_msg_emoji_like', 'time': 1000,
               'self_id': 900001, 'group_id': 900101, 'message_id': 5, 'user_id': 900402,
               'likes': [{'emoji_id': '128077', 'count': 1}]}
        event = normalize(raw, '900001', {'900101'}, set())
        self.assertIsNone(event.added)
        self.assertEqual(event.reaction, '128077')

    def test_reaction_cancel_added_false(self):
        raw = {'post_type': 'notice', 'notice_type': 'group_msg_emoji_like', 'time': 1000,
               'self_id': 900001, 'group_id': 900101, 'message_id': 5, 'user_id': 900402,
               'is_add': False, 'likes': []}
        event = normalize(raw, '900001', {'900101'}, set())
        self.assertIs(event.added, False)
        self.assertIsNone(event.reaction)

    def test_reaction_event_id_fingerprints_payload(self):
        raw = {'post_type': 'notice', 'notice_type': 'group_msg_emoji_like', 'time': 1000,
               'self_id': 900001, 'group_id': 900101, 'message_id': 5, 'user_id': 900402,
               'likes': [{'emoji_id': '1', 'count': 1}]}
        first = normalize(raw, '900001', {'900101'}, set())
        second = normalize(dict(raw), '900001', {'900101'}, set())
        self.assertEqual(first.event_id, second.event_id)
        raw['likes'] = [{'emoji_id': '1', 'count': 2}]
        self.assertNotEqual(first.event_id, normalize(raw, '900001', {'900101'}, set()).event_id)

    def test_segments_keep_order_and_are_sanitized(self):
        raw = {'post_type': 'message', 'message_type': 'group', 'time': 1000, 'self_id': 900001,
               'group_id': 900101, 'message_id': 1, 'user_id': 900401,
               'message': [{'type': 'text', 'data': {'text': 'a', 'nested': {'k': ['v']}}},
                           {'type': 'image', 'data': {'url': 'https://x/y.png'}},
                           {'type': 'text', 'data': {'text': 'c'}},
                           'not-a-segment', {'data': {'no-type': 1}}]}
        event = normalize(raw, '900001', {'900101'}, set())
        self.assertEqual([segment['type'] for segment in event.message.segments],
                         ['text', 'image', 'text'])
        self.assertEqual(event.message.segments[0]['data']['text'], 'a')
        self.assertEqual(event.message.segments[0]['data']['nested'], {'k': ['v']})
        self.assertNotIn('no-type', event.message.segments)

    def test_message_sent_with_sender_nickname(self):
        raw = {'post_type': 'message_sent', 'message_type': 'group', 'time': 1000,
               'self_id': 900001, 'group_id': 900101, 'message_id': 7, 'user_id': 900001,
               'sender': {'user_id': 900001, 'nickname': 'Bot'},
               'message': [{'type': 'text', 'data': {'text': 'self'}}]}
        event = normalize(raw, '900001', {'900101'}, set())
        self.assertTrue(event.message.own)
        self.assertEqual(event.message.nickname, 'Bot')


class FakeNapCat:
    """In-process OneBot 11 server: joint API receipts and pushed events."""

    def __init__(self, login_user='900001', actions=None, close_after_events=False):
        self.login_user = login_user
        self.actions = actions or {}
        self.events = []
        self.close_after_events = close_after_events
        self.received = []
        self.headers = {}
        self.push_task = None
        self.server = None
        self.port = None

    async def start(self):
        self.server = await websockets.serve(self.handler, '127.0.0.1', 0)
        self.port = self.server.sockets[0].getsockname()[1]

    async def close(self):
        if self.push_task is not None:
            self.push_task.cancel()
        self.server.close()
        await self.server.wait_closed()

    async def handler(self, ws):
        self.headers = ws.request.headers
        if self.events:
            self.push_task = asyncio.create_task(self._push(ws))
        try:
            async for raw in ws:
                message = json.loads(raw)
                if not isinstance(message, dict) or 'action' not in message:
                    continue
                self.received.append(message)
                if message['action'] == 'get_login_info':
                    await self._respond(ws, message, {'status': 'ok', 'retcode': 0,
                                                      'data': {'user_id': int(self.login_user)}})
                    continue
                respond = self.actions.get(message['action'])
                if respond is None:
                    await self._respond(ws, message, {'status': 'failed', 'retcode': 1403,
                                                      'message': 'unsupported action'})
                    continue
                response = respond(message)
                if response is not None:
                    await self._respond(ws, message, response)
        except asyncio.CancelledError:
            raise
        except Exception:
            pass

    @staticmethod
    async def _respond(ws, message, response):
        await ws.send(json.dumps({**response, 'echo': message['echo']},
                                 ensure_ascii=False, separators=(',', ':')))

    async def _push(self, ws):
        while not any(item.get('action') == 'get_login_info' for item in self.received):
            await asyncio.sleep(0.005)
        for event in self.events:
            await ws.send(json.dumps(event, ensure_ascii=False, separators=(',', ':')))
        if self.close_after_events:
            await ws.close()


class GatewayTests(unittest.IsolatedAsyncioTestCase):
    async def test_forward_original_uses_native_api_without_constructing_nodes(self):
        self.napcat.actions['get_msg'] = lambda request: {'status': 'ok', 'retcode': 0, 'data':
            message_payload(group_id=900101, message_id=-123,
                            segments=[{'type': 'forward', 'data': {'id': 'original-resource'}}])}
        self.napcat.actions['forward_group_single_msg'] = lambda request: {
            'status': 'ok', 'retcode': 0, 'data': {'message_id': -456}}
        await self.gateway.__aenter__()
        checks = []
        result = await self.gateway.forward_message(MessageRef('900001', '900101', '-123'), '900201',
                                                     before_send=lambda: checks.append(True))
        self.assertEqual(result, '-456')
        self.assertEqual(checks, [True])
        sends = [r for r in self.napcat.received if r['action'].startswith(('send_', 'forward_'))]
        self.assertEqual([(r['action'], r['params']) for r in sends],
                         [('forward_group_single_msg', {'group_id': '900201', 'message_id': -123})])

    async def test_native_forward_rejects_unavailable_or_misidentified_source(self):
        self.napcat.actions['get_msg'] = lambda request: {'status': 'ok', 'retcode': 0, 'data':
            message_payload(group_id=900102, message_id=123,
                            segments=[{'type': 'forward', 'data': {'id': 'resource'}}])}
        await self.gateway.__aenter__()
        with self.assertRaises(PlatformRejected):
            await self.gateway.forward_message(MessageRef('900001', '900101', '123'), '900201')
        self.assertFalse(any(r['action']=='forward_group_single_msg' for r in self.napcat.received))

    async def test_native_forward_with_missing_receipt_is_unknown(self):
        self.napcat.actions['get_msg'] = lambda request: {'status': 'ok', 'retcode': 0, 'data':
            message_payload(group_id=900101, message_id=123,
                            segments=[{'type': 'forward', 'data': {'id': 'resource'}}])}
        self.napcat.actions['forward_group_single_msg'] = lambda request: {'status': 'ok', 'retcode': 0, 'data': None}
        await self.gateway.__aenter__()
        with self.assertRaises(OutcomeUnknown):
            await self.gateway.forward_message(MessageRef('900001', '900101', '123'), '900201')

    async def asyncSetUp(self):
        self.token_env = os.environ.get('PULLSHIT_NAPCAT_ACCESS_TOKEN')
        os.environ['PULLSHIT_NAPCAT_ACCESS_TOKEN'] = 'test-token'
        self.napcat = FakeNapCat()
        await self.napcat.start()
        self.gateway = NapCatGateway(gateway_settings(f'ws://127.0.0.1:{self.napcat.port}/'))

    async def asyncTearDown(self):
        await self.gateway.__aexit__(None, None, None)
        await self.napcat.close()
        if self.token_env is None:
            os.environ.pop('PULLSHIT_NAPCAT_ACCESS_TOKEN', None)
        else:
            os.environ['PULLSHIT_NAPCAT_ACCESS_TOKEN'] = self.token_env

    async def collect_until(self, gateway, count, timeout=5):
        collected = []

        async def consume():
            async for event in gateway.events():
                collected.append(event)

        task = asyncio.create_task(consume())
        deadline = asyncio.get_running_loop().time() + timeout
        while len(collected) < count:
            if asyncio.get_running_loop().time() > deadline:
                task.cancel()
                self.fail('timed out waiting for events')
            await asyncio.sleep(0.01)
        task.cancel()
        return collected

    async def test_connect_resolves_account_and_auth(self):
        self.assertIs(await self.gateway.__aenter__(), self.gateway)
        self.assertEqual(self.gateway.account_id, '900001')
        login = [item for item in self.napcat.received if item['action'] == 'get_login_info']
        self.assertEqual(len(login), 1)
        self.assertEqual(self.napcat.headers.get('Authorization'),
                         'Bearer test-token')

    async def test_events_stream_normalized_only(self):
        base = {'time': 1791590000, 'self_id': 900001}
        self.napcat.events = [
            {**base, 'post_type': 'meta_event', 'meta_event_type': 'heartbeat', 'group_id': 900101},
            {**base, 'post_type': 'message', 'message_type': 'group', 'group_id': 900101,
             'message_id': 1, 'user_id': 900401, 'message': [{'type': 'text', 'data': {'text': 'a'}}]},
            {**base, 'post_type': 'message', 'message_type': 'private', 'group_id': 900101,
             'message_id': 2, 'user_id': 900401, 'message': []},
            {**base, 'post_type': 'message', 'message_type': 'group', 'group_id': 900999,
             'message_id': 3, 'user_id': 900401, 'message': [{'type': 'text', 'data': {'text': 'x'}}]},
            {**base, 'post_type': 'notice', 'notice_type': 'group_recall', 'group_id': 900101,
             'message_id': 1, 'operator_id': 900401},
            {**base, 'post_type': 'notice', 'notice_type': 'group_msg_emoji_like', 'group_id': 900101,
             'message_id': 1, 'user_id': 900402, 'likes': [{'emoji_id': '1', 'count': 1}]},
        ]
        await self.gateway.__aenter__()
        collected = await self.collect_until(self.gateway, 3)
        self.assertEqual([event.kind for event in collected], ['message', 'recall', 'reaction'])

    async def test_fetch_message_ok(self):
        def get_msg(_request):
            return {'status': 'ok', 'retcode': 0,
                    'data': message_payload(group_id=900102, message_id=101,
                                            segments=[{'type': 'text', 'data': {'text': 'hi'}},
                                                      {'type': 'image', 'data': {'file': 'x.png'}}])}
        self.napcat.actions = {'get_msg': get_msg}
        await self.gateway.__aenter__()
        message = await self.gateway.fetch_message(MessageRef('900001', '900102', '101'))
        self.assertEqual(message.ref.key, MessageRef('900001', '900102', '101').key)
        self.assertEqual([segment['type'] for segment in message.segments], ['text', 'image'])
        self.assertEqual(message.nickname, 'Alice')
        self.assertFalse(message.own)

    async def test_fetch_message_explicit_missing(self):
        self.napcat.actions = {'get_msg': lambda _request: {
            'status': 'failed', 'retcode': 1200, 'message': '消息不存在或已被撤回'}}
        await self.gateway.__aenter__()
        self.assertIsNone(await self.gateway.fetch_message(MessageRef('900001', '900102', '101')))
        self.napcat.actions = {'get_msg': lambda _request: {
            'status': 'failed', 'retcode': 1404, 'message': 'resource missing'}}
        self.assertIsNone(await self.gateway.fetch_message(MessageRef('900001', '900102', '101')))

    async def test_fetch_message_identity_mismatch(self):
        self.napcat.actions = {'get_msg': lambda _request: {
            'status': 'ok', 'retcode': 0, 'data': message_payload(group_id=999999, message_id=101)}}
        await self.gateway.__aenter__()
        with self.assertRaises(PayloadMismatch):
            await self.gateway.fetch_message(MessageRef('900001', '900102', '101'))
        self.napcat.actions = {'get_msg': lambda _request: {
            'status': 'ok', 'retcode': 0, 'data': message_payload(group_id=900102, message_id=999)}}
        with self.assertRaises(PayloadMismatch):
            await self.gateway.fetch_message(MessageRef('900001', '900102', '101'))

    async def test_fetch_message_account_mismatch(self):
        await self.gateway.__aenter__()
        with self.assertRaises(PlatformRejected):
            await self.gateway.fetch_message(MessageRef('900002', '900102', '101'))
        self.assertFalse(any(item['action'] == 'get_msg' for item in self.napcat.received))

    async def test_fetch_message_cq_string_payload(self):
        self.napcat.actions = {'get_msg': lambda _request: {
            'status': 'ok', 'retcode': 0,
            'data': {**message_payload(), 'message': '[CQ:text,text=x]'}}}
        await self.gateway.__aenter__()
        message = await self.gateway.fetch_message(MessageRef('900001', '900102', '101'))
        self.assertEqual(message.segments, [{'type': 'text', 'data': {'text': '[CQ:text,text=x]'}}])

    async def test_fetch_forward_preserves_hierarchy(self):
        nodes = [{'type': 'node', 'data': {'uin': '900401', 'name': 'A', 'content': [
                    {'type': 'text', 'data': {'text': 'top'}},
                    {'type': 'node', 'data': {'uin': '900402', 'name': 'B', 'content': [
                        {'type': 'text', 'data': {'text': 'nested'}}]}}]}}]
        self.napcat.actions = {'get_forward_msg': lambda _request: {
            'status': 'ok', 'retcode': 0, 'data': {'messages': nodes}}}
        await self.gateway.__aenter__()
        result = await self.gateway.fetch_forward(MessageRef('900001', '900102', '101'), 'fwd-1')
        self.assertEqual(result, nodes)
        request = [item for item in self.napcat.received if item['action'] == 'get_forward_msg'][0]
        self.assertEqual(request['params'], {'message_id': 'fwd-1'})

    async def test_reply_in_authorized_source_group_is_bound_to_message(self):
        self.napcat.actions={'send_group_msg':lambda _request:{'status':'ok','retcode':0,'data':{'message_id':901}}}
        await self.gateway.__aenter__()
        receipt=await self.gateway.reply(MessageRef('900001','900101','321'),'收到')
        self.assertEqual(receipt,'901')
        request=[r for r in self.napcat.received if r['action']=='send_group_msg'][0]
        self.assertEqual(request['params']['message'][0],{'type':'reply','data':{'id':'321'}})
        with self.assertRaises(PlatformRejected):
            await self.gateway.reply(MessageRef('900001','999999','321'),'收到')

    async def test_send_message_uses_plain_group_api_and_preserves_segments(self):
        self.napcat.actions = {'send_group_msg': lambda _request: {
            'status': 'ok', 'retcode': 0, 'data': {'message_id': -555}}}
        await self.gateway.__aenter__()
        segments = [{'type':'text','data':{'text':'ordinary message'}},
                    {'type':'image','data':{'file':'base64://YWJj'}}]
        result = await self.gateway.send_message('900201', segments)
        self.assertEqual(result, '-555')
        requests = [r for r in self.napcat.received if r['action'].startswith('send_')]
        self.assertEqual([r['action'] for r in requests], ['send_group_msg'])
        self.assertEqual(requests[0]['params'], {'group_id':'900201','message':segments})

    async def test_send_message_revalidates_account_and_callback(self):
        await self.gateway.__aenter__()
        def blocked():
            raise PlatformRejected('paused')
        with self.assertRaises(PlatformRejected):
            await self.gateway.send_message('900201',[{'type':'text','data':{'text':'x'}}],before_send=blocked)
        self.assertFalse(any(r['action']=='send_group_msg' for r in self.napcat.received))
        with self.assertRaises(ValueError):
            await self.gateway.send_message('900201',[{'type':'node','data':{}}])

    async def test_send_message_missing_receipt_is_unknown(self):
        self.napcat.actions = {'send_group_msg': lambda _request: {
            'status':'ok','retcode':0,'data':{}}}
        await self.gateway.__aenter__()
        with self.assertRaises(OutcomeUnknown):
            await self.gateway.send_message('900201',[{'type':'text','data':{'text':'x'}}])

    async def test_send_forward_ok(self):
        self.napcat.actions = {'send_group_forward_msg': lambda _request: {
            'status': 'ok', 'retcode': 0, 'data': {'message_id': 555}}}
        await self.gateway.__aenter__()
        nodes = [{'type': 'node', 'data': {'uin': '900401', 'name': 'A',
                                           'content': [{'type': 'text', 'data': {'text': 'x'}}]}}]
        result = await self.gateway.send_forward('900201', nodes)
        self.assertEqual(result, '555')
        request = [item for item in self.napcat.received
                   if item['action'] == 'send_group_forward_msg'][0]
        self.assertEqual(request['params']['group_id'], '900201')
        self.assertEqual(request['params']['messages'], nodes)

    async def test_send_forward_rejected(self):
        self.napcat.actions = {'send_group_forward_msg': lambda _request: {
            'status': 'failed', 'retcode': 1200, 'message': '发送合并转发消息失败'}}
        await self.gateway.__aenter__()
        with self.assertRaises(PlatformRejected):
            await self.gateway.send_forward('900201', [{'type': 'node', 'data': {}}])

    async def test_send_forward_uncertain_outcomes(self):
        self.napcat.actions = {'send_group_forward_msg': lambda _request: None}
        timeout_gateway = NapCatGateway(gateway_settings(f'ws://127.0.0.1:{self.napcat.port}/', timeout=0.2))
        await timeout_gateway.__aenter__()
        with self.assertRaises(OutcomeUnknown):
            await timeout_gateway.send_forward('900201', [{'type': 'node', 'data': {}}])
        await timeout_gateway.__aexit__(None, None, None)
        self.napcat.actions = {'send_group_forward_msg': lambda _request: {
            'status': 'ok', 'retcode': 0, 'data': {'no_message_id': True}}}
        await self.gateway.__aenter__()
        with self.assertRaises(OutcomeUnknown):
            await self.gateway.send_forward('900201', [{'type': 'node', 'data': {}}])

    async def test_send_forward_node_count_bounds(self):
        self.napcat.actions = {'send_group_forward_msg': lambda _request: {
            'status': 'ok', 'retcode': 0, 'data': {'message_id': 556}}}
        await self.gateway.__aenter__()
        node = {'type': 'node', 'data': {}}
        with self.assertRaises(ValueError):
            await self.gateway.send_forward('900201', [])
        with self.assertRaises(ValueError):
            await self.gateway.send_forward('900201', [node] * 101)
        result = await self.gateway.send_forward('900201', [node] * 100)
        self.assertEqual(result, '556')

    async def test_transport_error_sanitized(self):
        class ClosingServer(FakeNapCat):
            async def handler(self, ws):
                await ws.close(code=1008, reason='policy violation')

        server = ClosingServer()
        await server.start()
        gateway = NapCatGateway(gateway_settings(f'ws://127.0.0.1:{server.port}/'))
        with self.assertRaises(OutcomeUnknown) as caught:
            await gateway.__aenter__()
        self.assertNotIn('ws://', str(caught.exception))
        self.assertNotIn('test-token', str(caught.exception))
        await gateway.__aexit__(None, None, None)
        await server.close()

    async def test_invalid_endpoint(self):
        gateway = NapCatGateway(gateway_settings('http://example.com/'))
        with self.assertRaises(OutcomeUnknown):
            await gateway.__aenter__()

    async def test_queue_bounded_with_eviction(self):
        base = {'time': 1791590000, 'self_id': 900001, 'post_type': 'message',
                'message_type': 'group', 'group_id': 900101, 'user_id': 900401,
                'message': [{'type': 'text', 'data': {'text': 'x'}}]}
        self.napcat.events = [{**base, 'message_id': index} for index in range(1010)]
        self.napcat.close_after_events = True
        await self.gateway.__aenter__()
        loop = asyncio.get_running_loop()
        deadline = loop.time() + 10
        while self.gateway.stats['dropped_events'] < 10:
            if loop.time() > deadline:
                self.fail('no queue eviction observed')
            await asyncio.sleep(0.02)
        collected = []
        async for event in self.gateway.events():
            collected.append(event)
        self.assertEqual(len(collected) + self.gateway.stats['dropped_events'], 1010)
        self.assertLessEqual(len(collected), 1000)
        self.assertGreaterEqual(self.gateway.stats['dropped_events'], 10)


if __name__ == '__main__':
    unittest.main()
