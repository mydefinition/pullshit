"""Content bundle assembly from local evidence and bounded platform fetch.

build() resolves each requested message locally first, then fetches at most
max_fetch_calls records through the gateway, expands strong replies and
forward nodes within depth/node/deadline budgets, caches images and flattens
everything into ChatMessage records plus a fingerprint and a content+source
digest. Records older than content_days are never included and fetching never
extends their expiry; fetched messages are stored with their original age.
"""
from __future__ import annotations

import asyncio

from pullshit.contracts import (DAY, ChatMessage, ContentBundle, MediaRef, MessageRef,
                                OutcomeUnknown, PlatformRejected, digest, now, platform_id)
from pullshit.content.media import MediaError, ContentWindow
from pullshit.ingestion.napcat import PayloadMismatch


def _signed_id(raw) -> str:
    try:
        return platform_id(str(raw), signed=True)
    except ValueError:
        return ''


class _BuildState:
    def __init__(self, max_fetch_calls: int, max_nodes: int, reply_depth: int, forward_depth: int):
        self.max_fetch_calls = max_fetch_calls
        self.max_nodes = max_nodes
        self.reply_depth = reply_depth
        self.forward_depth = forward_depth
        self.fetch_calls = 0
        self.nodes = 0
        self.node_limit_issued = False
        self.messages: list[ChatMessage] = []
        self.outer_keys: set[str] = set()
        self.issues: list[str] = []
        self.media: list[MediaRef] = []
        self.media_hashes: dict[tuple, str] = {}
        self.seen_replies: set[str] = set()
        self.origins: dict[str, str] = {}


class ContentService:
    def __init__(self, settings, store, gateway=None, media_cache=None, *, clock=now):
        context = settings.data['context']
        retention = settings.data['retention']
        self.settings = settings
        self.clock = clock
        self.store = store
        self.gateway = gateway
        self.media_cache = media_cache
        self.reply_depth = int(context['reply_depth'])
        self.forward_depth = int(context['forward_depth'])
        self.max_nodes = int(context['max_nodes'])
        self.max_fetch_calls = int(context['max_fetch_calls'])
        self.timeout_seconds = float(context['timeout_seconds'])
        self.message_days = float(retention['message_days'])
        self.content_days = float(retention['content_days'])

    async def build(self, refs: MessageRef | list[MessageRef], *, media_limit=None) -> ContentBundle:
        if isinstance(refs, MessageRef):
            refs = [refs]
        started = self.clock()
        state = _BuildState(self.max_fetch_calls, self.max_nodes,
                            self.reply_depth, self.forward_depth)
        try:
            async with asyncio.timeout(self.timeout_seconds):
                for ref in refs:
                    await self._resolve(ref, 0, state)
                await self._cache_media(state,media_limit)
        except TimeoutError:
            state.issues.append('timeout')
        state.issues = list(dict.fromkeys(state.issues))
        bundle = self._bundle(state, started)
        bundle.trigger_refs = list(dict.fromkeys(ref.key for ref in refs))
        bundle.origins = state.origins
        return bundle

    async def _resolve(self, ref: MessageRef, reply_level: int, state: _BuildState, path=()) -> ChatMessage | None:
        if ref.group_id not in self.settings.sources | self.settings.targets or (self.settings.account_id and ref.account_id != self.settings.account_id):
            state.issues.append('reference_not_authorized')
            return None
        if ref.key in path:
            state.issues.append('reply_cycle')
            return None
        if ref.key in state.seen_replies:
            return None
        if state.nodes >= state.max_nodes:
            if not state.node_limit_issued:
                state.issues.append('node_limit')
                state.node_limit_issued = True
            return None
        state.seen_replies.add(ref.key)
        current = self.clock()
        message = self.store.get_message(ref, now=current)
        if message is None:
            message = await self._fetch(ref, state)
        if message is None:
            return None
        if message.occurred_at + self.content_days * DAY <= current:
            state.issues.append('expired')
            return None
        state.nodes += 1
        state.outer_keys.add(ref.key)
        if message.recalled:
            state.issues.append('recalled')
            state.messages.append(message)
            return message
        for segment in message.segments:
            if segment.get('type') == 'reply':
                reply_id = _signed_id(segment.get('data', {}).get('id'))
                if not reply_id:
                    state.issues.append('invalid_reply')
                elif reply_level >= self.reply_depth:
                    state.issues.append('reply_depth_limit')
                else:
                    await self._resolve(MessageRef(ref.account_id, ref.group_id, reply_id), reply_level+1, state, path+(ref.key,))
        state.messages.append(message)
        for segment in message.segments:
            kind = segment.get('type'); data = segment.get('data', {})
            if kind == 'text' and '[CQ:' in str(data.get('text', '')):
                state.issues.append('unsupported_cq')
            if kind == 'forward':
                forward_id = str(data.get('id', ''))
                if forward_id:
                    await self._expand_forward(ref, message, forward_id, state)
                else:
                    state.issues.append('missing_forward_id')
            elif kind == 'node':
                self._flatten_nodes(ref, message.occurred_at, [segment], [], 'embedded:'+digest(ref.key)[:24], set(), 1, state)
        return message

    async def _fetch(self, ref: MessageRef, state: _BuildState) -> ChatMessage | None:
        if self.gateway is None:
            state.issues.append(f'missing_offline:{ref.key}')
            return None
        if state.fetch_calls >= state.max_fetch_calls:
            state.issues.append(f'fetch_limit:{ref.key}')
            return None
        state.fetch_calls += 1
        try:
            message = await self.gateway.fetch_message(ref)
        except PayloadMismatch:
            state.issues.append(f'invalid_or_mismatched_payload:{ref.key}')
            return None
        except PlatformRejected:
            state.issues.append(f'api_rejected:{ref.key}')
            return None
        except OutcomeUnknown:
            state.issues.append(f'transport_error:{ref.key}')
            return None
        if message is None:
            state.issues.append(f'missing:{ref.key}')
            return None
        if message.ref.key != ref.key:
            state.issues.append(f'invalid_or_mismatched_payload:{ref.key}')
            return None
        if message.occurred_at < self.clock() - self.content_days * DAY:
            state.issues.append(f'expired:{ref.key}')
            return None
        message.expires_at = message.occurred_at + self.message_days * DAY
        self.store.put_message(message)
        return message

    async def _expand_forward(self, ref: MessageRef, message: ChatMessage,
                              forward_id: str, state: _BuildState) -> None:
        if self.gateway is None:
            state.issues.append(f'forward_unavailable:{ref.key}')
            return
        if state.fetch_calls >= state.max_fetch_calls:
            state.issues.append(f'fetch_limit:{ref.key}')
            return
        state.fetch_calls += 1
        try:
            nodes = await self.gateway.fetch_forward(ref, forward_id)
        except PlatformRejected:
            state.issues.append(f'api_rejected:{ref.key}')
            return
        except OutcomeUnknown:
            state.issues.append(f'transport_error:{ref.key}')
            return
        if not isinstance(nodes, list):
            state.issues.append(f'invalid_or_mismatched_payload:{ref.key}')
            return
        prefix = f'{ref.message_id}.f{digest(forward_id)[:8]}'
        self._flatten_nodes(ref, message.occurred_at, nodes, [], prefix,
                            set([forward_id]), 1, state)

    def _flatten_nodes(self, outer_ref: MessageRef, outer_occurred: float, nodes: list,
                       path: list[int], prefix: str, branch: set[str],
                       level: int, state: _BuildState) -> None:
        for index, node in enumerate(nodes):
            if not isinstance(node, dict):
                continue
            if state.nodes >= state.max_nodes:
                if not state.node_limit_issued:
                    state.issues.append('node_limit')
                    state.node_limit_issued = True
                return
            state.nodes += 1
            data = node.get('data') if isinstance(node.get('data'), dict) else node
            sender = data.get('sender', {})
            data = {**data, 'user_id': data.get('user_id', sender.get('user_id')), 'nickname': data.get('nickname', sender.get('nickname')), 'content': data.get('content', data.get('message'))}
            user_id = _signed_id(data.get('user_id')) or _signed_id(data.get('uin')) or outer_ref.account_id
            nickname = data.get('nickname') or data.get('name')
            nickname = nickname if isinstance(nickname, str) else ''
            occurred = outer_occurred
            if isinstance(data.get('time'), (int, float)) and not isinstance(data.get('time'), bool):
                occurred = float(data['time'])
            node_path = path + [index]
            node_ref = MessageRef(outer_ref.account_id, outer_ref.group_id,
                                  'embedded:'+digest([prefix,node_path]))
            if occurred < self.clock() - self.content_days * DAY:
                state.issues.append(f'expired:{node_ref.key}')
                continue
            message = ChatMessage(
                ref=node_ref, user_id=user_id, nickname=nickname, occurred_at=occurred,
                segments=[], own=user_id == outer_ref.account_id, recalled=False,
                expires_at=occurred + self.message_days * DAY)
            state.messages.append(message)
            state.origins[node_ref.key] = state.origins.get(outer_ref.key, outer_ref.key)
            message.segments = self._node_segments(node_ref, occurred, data.get('content'),
                                                  node_path, prefix, branch, level, state)

    def _node_segments(self, node_ref: MessageRef, occurred: float, content, path: list[int], prefix: str,
                       branch: set[str], level: int, state: _BuildState) -> list[dict]:
        if content is None:
            return []
        if isinstance(content, str):
            state.issues.append(f'cq_string:{node_ref.key}')
            return [{'type': 'text', 'data': {'text': content}}]
        if not isinstance(content, list):
            return []
        result = []
        for index, item in enumerate(content):
            if not isinstance(item, dict) or not isinstance(item.get('type'), str):
                continue
            item_data = item.get('data') if isinstance(item.get('data'), dict) else {}
            if item['type'] == 'node':
                result.append({'type': 'node', 'data': dict(item_data)})
                if level < self.forward_depth:
                    self._flatten_nodes(node_ref, occurred, [item], path + [index],
                                        prefix, set(branch), level + 1, state)
                else:
                    state.issues.append(f'forward_depth_limit:{node_ref.key}')
                continue
            if item['type'] == 'forward':
                result.append({'type': 'forward', 'data': dict(item_data)})
                claimed_id = item_data.get('id')
                inline = item_data.get('content')
                if not isinstance(claimed_id, str) or not claimed_id:
                    continue
                if claimed_id in branch:
                    state.issues.append(f'forward_cycle:{node_ref.key}')
                    continue
                if not isinstance(inline, list):
                    state.issues.append(f'embedded_forward_unverified:{node_ref.key}')
                    continue
                if level >= self.forward_depth:
                    state.issues.append(f'forward_depth_limit:{node_ref.key}')
                    continue
                nested = [{'type': 'node', 'data': {'content': inline}}]
                self._flatten_nodes(node_ref, occurred, nested, path + [index],
                                    prefix, branch | {claimed_id}, level + 1, state)
                continue
            result.append({'type': item['type'], 'data': dict(item_data)})
        return result

    async def _cache_media(self, state: _BuildState, media_limit=None) -> None:
        if self.media_cache is None:
            return
        attempts=0
        for message in state.messages:
            if message.recalled:
                continue
            for index, segment in enumerate(message.segments):
                if segment.get('type') != 'image':
                    continue
                if media_limit is not None and attempts>=media_limit:
                    state.issues.append(f'media_limit:{message.ref.key}')
                    continue
                attempts+=1
                data = segment.get('data') if isinstance(segment.get('data'), dict) else {}
                url = data.get('url')
                if not isinstance(url, str) or not url:
                    state.issues.append(f'media_unavailable:{message.ref.key}')
                    continue
                try:
                    media_ref = await self.media_cache.cache(
                        url, message.ref, message.occurred_at + self.content_days * DAY)
                except MediaError as exc:
                    state.issues.append(f'{exc.code}:{message.ref.key}')
                    continue
                state.media.append(media_ref)
                state.media_hashes[(message.ref.key, index)] = media_ref.sha256
                data.pop('url', None)
                data['file'] = media_ref.path
                segment['data'] = data

    def _bundle(self, state: _BuildState, started: float) -> ContentBundle:
        messages = state.messages
        content = []
        for message in messages:
            items = []
            for index, segment in enumerate(message.segments):
                seg_type = segment.get('type')
                data = segment.get('data') if isinstance(segment.get('data'), dict) else {}
                if seg_type == 'text':
                    text = data.get('text')
                    if isinstance(text, str) and text:
                        items.append(['text', text])
                elif seg_type == 'image':
                    sha = state.media_hashes.get((message.ref.key, index))
                    file = data.get('file')
                    items.append(['image', sha or (file if isinstance(file, str) else '')])
            if items:
                content.append(items)
        fingerprint = digest({'content': content})
        bundle_id = digest({'content': content, 'sources': sorted(state.outer_keys)})
        expires_at = started + self.content_days * DAY
        if messages:
            expires_at = min(started,min(item.occurred_at for item in messages)) + self.content_days * DAY
        return ContentBundle(bundle_id=bundle_id, fingerprint=fingerprint, messages=messages,
                             created_at=started, expires_at=expires_at,
                             media=list(state.media), issues=state.issues)
