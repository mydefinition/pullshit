"""OneBot events retain original evidence and use scoped string identifiers."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

from pullshit.config import decimal_id


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="microseconds").replace("+00:00", "Z")


def canonical(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False)


def segments(message: Any, max_depth: int = 3, max_nodes: int = 100) -> tuple[list, list[str]]:
    issues = []
    count = 0

    def walk(items: Any, depth: int) -> list:
        nonlocal count
        if isinstance(items, str):
            if "[CQ:" in items:
                issues.append("opaque_cq_string")
            return [{"type": "text", "data": {"text": items}}]
        if not isinstance(items, list):
            issues.append("invalid_segments")
            return []
        if depth > max_depth:
            issues.append("forward_depth_limit")
            return []
        result = []
        for item in items:
            if count >= max_nodes:
                issues.append("segment_node_limit")
                break
            count += 1
            if not isinstance(item, dict) or not isinstance(item.get("type"), str) or not isinstance(item.get("data"), dict):
                issues.append("invalid_segment")
                continue
            kind, data = item["type"], dict(item["data"])
            if kind == "reply":
                try:
                    data["id"] = decimal_id(data.get("id"), signed=True)
                except ValueError:
                    issues.append("missing_reply_id")
            elif kind == "forward":
                if not isinstance(data.get("id"), (str, int)) or isinstance(data.get("id"), bool):
                    issues.append("missing_forward_id")
                else:
                    data["id"] = str(data["id"])
                    issues.append("forward_not_fetched")
            elif kind == "node":
                for key in ("user_id", "uin"):
                    if key in data:
                        try:
                            data[key] = decimal_id(data[key])
                        except ValueError:
                            issues.append("invalid_node_user")
                data["content"] = walk(data.get("content", []), depth + 1)
            elif kind not in {"text", "image", "at", "json", "xml"}:
                issues.append("unsupported_segment")
            result.append({"type": kind, "data": data})
        return result

    result = walk(message, 0)
    return result, sorted(set(issues))


@dataclass(frozen=True)
class Event:
    account_id: str
    group_id: str
    message_id: str | None
    user_id: str | None
    event_type: str
    occurred_at: int
    received_at: str
    key: str
    own_message: bool
    raw: dict = field(repr=False)
    message: list | None = field(default=None, repr=False)
    issues: list[str] = field(default_factory=list)


def normalize(raw: dict, account: str, sources: set[str], targets: set[str],
              max_depth: int = 3, max_nodes: int = 100) -> Event | None:
    if not isinstance(raw, dict):
        raise ValueError("事件必须是对象")
    if decimal_id(raw.get("self_id")) != account:
        return None
    group = decimal_id(raw["group_id"]) if "group_id" in raw else ""
    if group and group not in sources | targets:
        return None
    post = raw.get("post_type")
    if post not in {"message", "message_sent", "notice", "meta_event"}:
        return None
    if not group and post != "meta_event":
        return None
    timestamp = raw.get("time")
    if type(timestamp) is not int or timestamp < 0:
        raise ValueError("事件缺少有效时间")
    category = raw.get(f"{post}_type")
    if post == "message_sent":
        category = raw.get("message_type")
    if not isinstance(category, str):
        raise ValueError("事件缺少有效类型")
    subtype = raw.get("sub_type")
    if subtype is not None and not isinstance(subtype, str):
        raise ValueError("事件子类型无效")
    kind = ".".join(filter(None, (post, category, subtype)))
    message_id = decimal_id(raw["message_id"], signed=True) if "message_id" in raw else None
    user_id = decimal_id(raw["user_id"]) if "user_id" in raw else None
    message = None
    issues = []
    if post in {"message", "message_sent"}:
        if category != "group" or not message_id or not user_id:
            raise ValueError("群消息缺少 ID")
        message, issues = segments(raw.get("message"), max_depth, max_nodes)
        identity = [account, group, "message", message_id]
    else:
        identity = [account, group, raw]
    return Event(account, group, message_id, user_id, kind, timestamp, utc_now(),
                 hashlib.sha256(canonical(identity).encode()).hexdigest(),
                 post == "message_sent" or user_id == account, raw, message, issues)
