"""Explicitly labelled operator tests of custom forward nodes."""
from __future__ import annotations

import json
from pullshit.contracts import MessageRef, platform_id


def normalize_request(value, targets):
    if not isinstance(value, dict) or set(value) - {'qq', 'content', 'group_id'}:
        raise ValueError('invalid_fields')
    qq = platform_id(value.get('qq'))
    if int(qq) <= 0 or len(qq) > 20:
        raise ValueError('invalid_qq')
    content = value.get('content')
    if not isinstance(content, str) or not content.strip() or len(content) > 2000:
        raise ValueError('invalid_content')
    if len(content.encode('utf-8')) > 8000:
        raise ValueError('invalid_content')
    group = value.get('group_id')
    if group is None and len(targets) == 1:
        group = next(iter(targets))
    group = platform_id(group)
    if group not in targets:
        raise PermissionError('target_not_authorized')
    return {'qq': qq, 'content': content, 'group_id': group}


async def send_simulation(settings, request, account, gateway_factory, before_send, on_receipt):
    async with gateway_factory(settings) as gateway:
        if gateway.account_id != account:
            raise PermissionError('account_mismatch')
        qq, group = request['qq'], request['group_id']
        nodes = [
            {'type': 'node', 'data': {
                'user_id': account, 'nickname': 'pullshit · 模拟测试说明',
                'content': [{'type': 'text', 'data': {'text':
                    f'【模拟测试｜非本人发言】以下节点由机器人构造，不代表 QQ {qq} 本人说过这些话。'}}]}},
            {'type': 'node', 'data': {
                'user_id': qq, 'nickname': f'QQ {qq}（模拟测试）',
                'content': [{'type': 'text', 'data': {'text': request['content']}}]}},
        ]
        message_id = await gateway.send_forward(group, nodes, before_send=before_send)
        result = {'status': 'sent', 'simulation': True, 'group_id': group,
                  'requested_qq': qq, 'message_id': message_id,
                  'identity_verified': None, 'content_verified': None,
                  'disclosure_verified': None, 'observed_qq': None}
        on_receipt(dict(result))
        try:
            ref = MessageRef(account, group, message_id)
            message = await gateway.fetch_message(ref)
            if message is None or not message.own:
                raise ValueError('message_unavailable')
            resource = None
            for segment in message.segments:
                if segment['type'] == 'forward':
                    resource = segment.get('data', {}).get('id') or resource
                elif segment['type'] == 'json':
                    raw = segment.get('data', {}).get('data')
                    card = json.loads(raw) if isinstance(raw, str) else raw
                    if isinstance(card, dict):
                        resource = card.get('meta', {}).get('detail', {}).get('resid') or resource
            if not resource:
                raise ValueError('forward_unavailable')
            returned = await gateway.fetch_forward(ref, str(resource))
            observed = []
            for node in returned:
                sender = node.get('sender') or {}
                data = node.get('data') or {}
                author = str(sender.get('user_id') or node.get('user_id') or data.get('user_id') or '')
                segments = node.get('content') or node.get('message') or data.get('content') or []
                text = ''.join(str(s.get('data', {}).get('text', '')) for s in segments
                               if isinstance(s, dict) and s.get('type') == 'text') if isinstance(segments, list) else str(segments)
                observed.append((author, text))
            result['content_verified'] = len(observed) == 2 and observed[1][1] == request['content']
            result['observed_qq'] = observed[1][0] if len(observed) == 2 else None
            result['identity_verified'] = result['observed_qq'] == qq
            result['disclosure_verified'] = bool(observed and observed[0][0] == account
                and '模拟测试' in observed[0][1] and '非本人发言' in observed[0][1])
        except Exception:
            result['verification_error'] = 'readback_unavailable'
        return result
