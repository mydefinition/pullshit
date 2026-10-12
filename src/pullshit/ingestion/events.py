"""Normalize OneBot 11 event payloads into platform contracts.

Only authorized group messages, recalls and emoji changes are normalized;
heartbeats, private traffic, other accounts and unknown payloads are ignored.
Message segments keep platform order; message IDs may be signed. Dedupe
identity for messages is the account/group/message key, other events use the
full payload fingerprint. A recall of a message that was never seen still
normalizes, so the store can record a tombstone later.
"""
from __future__ import annotations

from pullshit.contracts import DAY, ChatEvent, ChatMessage, MessageRef, digest, platform_id


def _decimal(raw, signed: bool = False) -> str:
    if raw is None or isinstance(raw, bool):
        return ''
    try:
        return platform_id(str(raw), signed)
    except ValueError:
        return ''


def _float_time(raw) -> float | None:
    if isinstance(raw, bool) or not isinstance(raw, (int, float)):
        return None
    return float(raw)


def _safe_data(data, depth: int = 0) -> dict:
    """Keep a JSON-safe, bounded copy of segment payload data."""
    if not isinstance(data, dict) or depth > 10:
        return {}
    result = {}
    for key, value in data.items():
        if not isinstance(key, str):
            continue
        if value is None or isinstance(value, (str, int, float, bool)):
            result[key] = value
        elif isinstance(value, list) and depth < 10:
            kept = []
            for item in value:
                if item is None or isinstance(item, (str, int, float, bool)):
                    kept.append(item)
                elif isinstance(item, dict):
                    kept.append(_safe_data(item, depth + 1))
            result[key] = kept
        elif isinstance(value, dict):
            result[key] = _safe_data(value, depth + 1)
    return result


def segments_of(raw) -> list[dict] | None:
    """Ordered sanitized message segments, or None when the payload has none."""
    if not isinstance(raw, dict) or not isinstance(raw.get('message'), list) or not raw['message']:
        return None
    result = []
    for segment in raw['message']:
        if not isinstance(segment, dict) or not isinstance(segment.get('type'), str):
            continue
        result.append({'type': segment['type'], 'data': _safe_data(segment.get('data'))})
    return result


def message_of(raw, account_id: str, message_days: float = 3) -> ChatMessage | None:
    """Build a ChatMessage from an event-like payload (push event or get_msg data)."""
    if not isinstance(raw, dict):
        return None
    message_id = _decimal(raw.get('message_id'), signed=True)
    group_id = _decimal(raw.get('group_id'))
    occurred_at = _float_time(raw.get('time'))
    if not message_id or not group_id or occurred_at is None:
        return None
    sender = raw.get('sender') if isinstance(raw.get('sender'), dict) else {}
    user_id = _decimal(raw.get('user_id')) or _decimal(sender.get('user_id'))
    nickname = sender.get('nickname')
    nickname = nickname if isinstance(nickname, str) else ''
    segments = segments_of(raw)
    if segments is None:
        return None
    ref = MessageRef(account_id, group_id, message_id)
    return ChatMessage(ref=ref, user_id=user_id, nickname=nickname, occurred_at=occurred_at,
                       segments=segments, own=user_id == account_id, recalled=False,
                       expires_at=occurred_at + message_days * DAY)


def normalize(raw, account_id: str, sources, targets, message_days: float = 3) -> ChatEvent | None:
    """Normalize one raw event into a ChatEvent; returns None for ignored input."""
    if not isinstance(raw, dict):
        return None
    account_id = platform_id(account_id)
    self_id = raw.get('self_id')
    if self_id is not None:
        reported = _decimal(self_id)
        if not reported or reported != account_id:
            return None
    group_id = _decimal(raw.get('group_id'))
    if not group_id or group_id not in set(sources) | set(targets):
        return None
    occurred_at = _float_time(raw.get('time'))
    if occurred_at is None:
        return None
    post_type = raw.get('post_type')
    if post_type in ('message', 'message_sent'):
        if raw.get('message_type') != 'group':
            return None
        message = message_of(raw, account_id, message_days)
        if message is None:
            return None
        return ChatEvent(kind='message', ref=message.ref, occurred_at=occurred_at,
                         event_id=message.ref.key, user_id=message.user_id, message=message)
    if post_type == 'notice':
        notice_type = raw.get('notice_type')
        if notice_type == 'group_recall':
            message_id = _decimal(raw.get('message_id'), signed=True)
            if not message_id:
                return None
            user_id = _decimal(raw.get('operator_id')) or _decimal(raw.get('user_id'))
            ref = MessageRef(account_id, group_id, message_id)
            return ChatEvent(kind='recall', ref=ref, occurred_at=occurred_at,
                             event_id=digest(raw), user_id=user_id)
        if notice_type == 'group_msg_emoji_like':
            message_id = _decimal(raw.get('message_id'), signed=True)
            if not message_id:
                return None
            reaction = None
            likes = raw.get('likes')
            if isinstance(likes, list) and likes and isinstance(likes[0], dict):
                reaction = _decimal(likes[0].get('emoji_id'))
            added = raw.get('is_add')
            added = added if isinstance(added, bool) else None
            ref = MessageRef(account_id, group_id, message_id)
            return ChatEvent(kind='reaction', ref=ref, occurred_at=occurred_at,
                             event_id=digest(raw), user_id=_decimal(raw.get('user_id')),
                             reaction=reaction, added=added)
    return None
