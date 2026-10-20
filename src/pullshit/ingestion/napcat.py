"""NapCat OneBot 11 gateway over a single joint WebSocket.

One reader task receives both API receipts (correlated by echo) and pushed
events (normalized into ChatEvents). The gateway owns connection lifetime but
no reconnection; the runtime lead decides when to reconnect. The event queue
is bounded at 1000 entries (oldest evicted when the consumer stalls), frames
are capped at 2 MiB, and transport failures surface as sanitized
PlatformRejected/OutcomeUnknown errors that never carry endpoints or tokens.

API shape reference (NapCat 4.18.x, OneBot 11):
- get_login_info -> {user_id}; get_msg -> full group message payload, missing
  messages fail with wording such as "消息不存在或已被撤回" (websocket retcode
  1200) or resource-missing retcode 1404;
- get_forward_msg -> {messages: [node, ...]} preserving hierarchy; nodes carry
  data.content as segment list, CQ string or inline nested nodes;
- send_group_forward_msg {group_id, messages: 1..100 nodes} -> {message_id}.
"""
from __future__ import annotations

import asyncio
import json
import logging
from urllib.parse import urlsplit

import websockets
from websockets.exceptions import ConnectionClosed

from pullshit.contracts import (ChatEvent, ChatMessage, MessageRef, OutcomeUnknown,
                                PlatformRejected, platform_id)
from pullshit.ingestion.events import message_of, normalize

QUEUE_BOUND = 1000
MAX_FRAME_BYTES = 2 * 1024 * 1024
_MISSING_WORDINGS = ('不存在', '撤回', '为空', '过期')


class PayloadMismatch(PlatformRejected):
    """The platform payload contradicts the verified identity of the request."""


class NapCatGateway:
    def __init__(self, settings):
        napcat = settings.data['napcat']
        self.settings = settings
        self.endpoint = napcat['endpoint']
        self.token = settings.token()
        self.request_timeout = float(napcat['request_timeout_seconds'])
        self.account_id = ''
        self.stats = {'dropped_events': 0}
        self._ws = None
        self._reader = None
        self._echo = 0
        self._pending: dict[str, asyncio.Future] = {}
        self._queue: asyncio.Queue = asyncio.Queue(maxsize=QUEUE_BOUND)

    async def __aenter__(self) -> NapCatGateway:
        parts = urlsplit(self.endpoint or '')
        if parts.scheme not in ('ws', 'wss') or not parts.hostname or parts.username \
                or parts.password or parts.query or parts.fragment:
            raise OutcomeUnknown('invalid napcat endpoint')
        headers = {}
        if self.token:
            headers['Authorization'] = f'Bearer {self.token}'
        try:
            self._ws = await websockets.connect(self.endpoint, additional_headers=headers,
                                                proxy=None, max_size=MAX_FRAME_BYTES,
                                                open_timeout=self.request_timeout, close_timeout=2)
        except Exception:
            raise OutcomeUnknown('napcat connect failed') from None
        self._reader = asyncio.create_task(self._read_loop())
        try:
            info = await self._call('get_login_info', {})
            self.account_id = platform_id(str(info['user_id']))
        except (KeyError, TypeError, ValueError, PlatformRejected, OutcomeUnknown):
            await self.__aexit__(None, None, None)
            raise OutcomeUnknown('napcat login info unavailable') from None
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        reader, ws = self._reader, self._ws
        self._reader = None
        self._ws = None
        if reader is not None:
            reader.cancel()
            try:
                await reader
            except (asyncio.CancelledError, Exception):
                pass
        for future in self._pending.values():
            if not future.done():
                future.set_exception(OutcomeUnknown('connection closed'))
                future.exception()
        self._pending.clear()
        if ws is not None:
            try:
                await ws.close()
            except Exception:
                pass
        self._put_sentinel()

    async def events(self):
        """Yield normalized ChatEvents until the gateway closes."""
        while True:
            item = await self._queue.get()
            if item is None:
                return
            event = normalize(item, self.account_id, self.settings.sources,
                              self.settings.targets,
                              self.settings.data['retention']['message_days'])
            if event is not None:
                yield event

    async def fetch_message(self, ref: MessageRef) -> ChatMessage | None:
        """Fetch one message after verifying account/group/message identity.

        Returns None when the platform reports the message as missing or
        recalled; raises PayloadMismatch on identity mismatch and
        PlatformRejected on other explicit rejections.
        """
        if not self.account_id or ref.account_id != self.account_id:
            raise PlatformRejected('account mismatch')
        if ref.group_id not in self.settings.sources | self.settings.targets:
            raise PlatformRejected('group not authorized')
        await self._verify_account()
        try:
            message_id = int(ref.message_id)
        except ValueError:
            raise PlatformRejected('invalid message id') from None
        try:
            data = await self._call('get_msg', {'message_id': message_id})
        except PlatformRejected as exc:
            if self._explicit_missing(exc):
                return None
            raise
        if not isinstance(data, dict):
            return None
        payload = data.get('message')
        if isinstance(payload, str):
            if not payload:
                return None
            data = {**data, 'message': [{'type': 'text', 'data': {'text': payload}}]}
        elif not isinstance(payload, list) or not payload:
            return None
        if str(data.get('group_id') or '') != ref.group_id:
            raise PayloadMismatch('message identity mismatch')
        try:
            same_id = int(str(data.get('message_id'))) == int(ref.message_id)
        except (TypeError, ValueError):
            same_id = False
        if not same_id:
            raise PayloadMismatch('message identity mismatch')
        message = message_of(data, self.account_id, self.settings.data['retention']['message_days'])
        if message is None or message.ref.key != ref.key:
            raise PayloadMismatch('message identity mismatch')
        return message

    async def fetch_forward(self, ref: MessageRef, forward_id: str) -> list[dict]:
        """Fetch raw forward nodes preserving hierarchy; the caller verified
        that the outer message claims this forward id."""
        if not self.account_id or ref.account_id != self.account_id:
            raise PlatformRejected('account mismatch')
        if ref.group_id not in self.settings.sources | self.settings.targets:
            raise PlatformRejected('group not authorized')
        await self._verify_account()
        data = await self._call('get_forward_msg', {'message_id': str(forward_id)})
        if not isinstance(data, dict) or not isinstance(data.get('messages'), list):
            raise PayloadMismatch('forward payload mismatch')
        return data['messages']

    async def reply(self, ref: MessageRef, text: str, *, before_send=None) -> str:
        """Reply to a real event within an authorized group."""
        if ref.account_id!=self.account_id or ref.group_id not in self.settings.sources|self.settings.targets:
            raise PlatformRejected('reply not authorized')
        await self._verify_account()
        if before_send is not None: before_send()
        data=await self._call('send_group_msg',{'group_id':ref.group_id,'message':[
            {'type':'reply','data':{'id':platform_id(ref.message_id,True)}},
            {'type':'text','data':{'text':str(text)}}]})
        if not isinstance(data,dict) or data.get('message_id') is None:
            raise OutcomeUnknown('invalid reply ack')
        try: return platform_id(str(data['message_id']),True)
        except ValueError: raise OutcomeUnknown('invalid reply ack') from None

    async def send_message(self, group_id: str, segments: list[dict], *, before_send=None) -> str:
        """Send an ordinary text/image message and return its platform receipt."""
        if not self.account_id:
            raise OutcomeUnknown('gateway not ready')
        group_id = platform_id(str(group_id))
        if not isinstance(segments, list) or not 1 <= len(segments) <= 100:
            raise ValueError('message segment count must be 1..100')
        if any(not isinstance(segment, dict) or segment.get('type') not in {'text', 'image'}
               or not isinstance(segment.get('data'), dict) for segment in segments):
            raise ValueError('message requires text/image segments')
        if group_id not in self.settings.targets:
            raise PlatformRejected('target not authorized')
        await self._verify_account()
        if before_send is not None:
            before_send()
        data = await self._call('send_group_msg', {'group_id': group_id, 'message': segments})
        if not isinstance(data, dict) or data.get('message_id') is None:
            raise OutcomeUnknown('invalid send ack')
        try:
            return platform_id(str(data['message_id']), signed=True)
        except ValueError:
            raise OutcomeUnknown('invalid send ack') from None

    async def send_forward(self, group_id: str, nodes: list[dict], *, before_send=None) -> str:
        """Send 1..100 forward nodes and return the new message id."""
        if not self.account_id:
            raise OutcomeUnknown('gateway not ready')
        group_id = platform_id(str(group_id))
        if not isinstance(nodes, list) or not 1 <= len(nodes) <= 100:
            raise ValueError('forward node count must be 1..100')
        if any(not isinstance(node, dict) for node in nodes):
            raise ValueError('forward nodes must be objects')
        if group_id not in self.settings.targets:
            raise PlatformRejected('target not authorized')
        await self._verify_account()
        if before_send is not None:
            before_send()
        data = await self._call('send_group_forward_msg', {'group_id': group_id, 'messages': nodes})
        if not isinstance(data, dict) or data.get('message_id') is None:
            raise OutcomeUnknown('invalid send ack')
        try:
            return platform_id(str(data['message_id']), signed=True)
        except ValueError:
            raise OutcomeUnknown('invalid send ack') from None

    async def forward_message(self, ref: MessageRef, group_id: str, *, before_send=None) -> str:
        """Copy an existing merged-forward message without rebuilding its nodes."""
        group_id = platform_id(str(group_id))
        if ref.account_id != self.account_id or not self.settings.permits(ref.group_id, group_id):
            raise PlatformRejected('forward not authorized')
        message = await self.fetch_message(ref)
        if (message is None or message.own or message.recalled
                or not any(s.get('type') in {'forward', 'node'} for s in message.segments)
                or any(s.get('type') not in {'forward', 'node', 'text', 'image'} for s in message.segments)):
            raise PlatformRejected('forward source unavailable')
        await self._verify_account()
        if before_send is not None:
            before_send()
        data = await self._call('forward_group_single_msg',
                                {'group_id': group_id, 'message_id': int(ref.message_id)})
        if not isinstance(data, dict) or data.get('message_id') is None:
            raise OutcomeUnknown('invalid forward ack')
        try:
            return platform_id(str(data['message_id']), signed=True)
        except ValueError:
            raise OutcomeUnknown('invalid forward ack') from None

    async def _verify_account(self):
        login=await self._call('get_login_info',{})
        if platform_id(login.get('user_id'))!=self.account_id:
            raise OutcomeUnknown('platform account changed')

    def _explicit_missing(self, exc: PlatformRejected) -> bool:
        wording = str(exc)
        return 'retcode=1404' in wording or any(part in wording for part in _MISSING_WORDINGS)

    async def _read_loop(self) -> None:
        try:
            async for raw in self._ws:
                try:
                    message = json.loads(raw)
                except (ValueError, TypeError):
                    continue
                if not isinstance(message, dict):
                    continue
                if 'echo' in message:
                    future = self._pending.pop(str(message['echo']), None)
                    if future is not None and not future.done():
                        future.set_result(message)
                    continue
                if message.get('post_type'):
                    self._enqueue_event(message)
        except (ConnectionClosed, asyncio.CancelledError):
            pass
        except Exception:
            pass
        finally:
            for future in self._pending.values():
                if not future.done():
                    future.set_exception(OutcomeUnknown('connection closed'))
                    future.exception()
            self._pending.clear()
            self._put_sentinel()

    def _enqueue_event(self, raw: dict) -> None:
        try:
            self._queue.put_nowait(raw)
        except asyncio.QueueFull:
            self._queue.get_nowait()
            self.stats['dropped_events'] += 1
            logging.getLogger('pullshit').warning('gateway event queue full; oldest event discarded')
            self._queue.put_nowait(raw)

    def _put_sentinel(self) -> None:
        try:
            self._queue.put_nowait(None)
        except asyncio.QueueFull:
            try:
                self._queue.get_nowait()
            except asyncio.QueueEmpty:
                pass
            self.stats['dropped_events'] += 1
            self._queue.put_nowait(None)

    async def _call(self, action: str, params: dict):
        ws = self._ws
        if ws is None:
            raise OutcomeUnknown('gateway not connected')
        echo = str(self._echo)
        self._echo += 1
        future = asyncio.get_running_loop().create_future()
        self._pending[echo] = future
        payload = json.dumps({'action': action, 'params': params, 'echo': echo},
                             ensure_ascii=False, separators=(',', ':'))
        try:
            await ws.send(payload)
        except Exception:
            self._pending.pop(echo, None)
            raise OutcomeUnknown('api send failed') from None
        try:
            response = await asyncio.wait_for(future, self.request_timeout)
        except asyncio.TimeoutError:
            raise OutcomeUnknown('api timeout') from None
        finally:
            self._pending.pop(echo, None)
        status = response.get('status')
        if status == 'ok':
            return response.get('data')
        if status == 'failed':
            wording = response.get('message') or ''
            raise PlatformRejected(f'retcode={response.get("retcode")} {wording}'.strip())
        raise OutcomeUnknown('invalid api ack')
