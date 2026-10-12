"""Local operator probes; durable intent precedes a bounded send operation."""

from __future__ import annotations

import asyncio
import base64
import fcntl
import re
from contextlib import contextmanager
from pathlib import Path

from pullshit.config import ConfigError, Settings, decimal_id, load_settings
from pullshit.adapters.qq.napcat import ApiError, TransportError
from pullshit.storage.store import Store, StoreError


@contextmanager
def sender_lock(database: Path):
    database.parent.mkdir(parents=True, exist_ok=True)
    path = database.with_suffix(database.suffix + ".sender.lock")
    with path.open("a") as handle:
        try:
            fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            raise StoreError("同一环境已有发送探测进程") from None
        try:
            yield
        finally:
            fcntl.flock(handle, fcntl.LOCK_UN)


def validate_request(request: dict, settings: Settings) -> dict:
    fields = {"probe_id", "source_group_id", "target_group_id", "operator_id", "messages"}
    if not isinstance(request, dict) or set(request) != fields:
        raise ConfigError("探测请求字段无效")
    if not isinstance(request["probe_id"], str) or not re.fullmatch(r"[A-Za-z0-9_-]{1,80}", request["probe_id"]):
        raise ConfigError("探测 ID 无效")
    auth = settings.data["authorization"]
    for field in ("source_group_id", "target_group_id", "operator_id"):
        if not isinstance(request[field], str):
            raise ConfigError("探测 ID 必须是字符串")
        try:
            decimal_id(request[field])
        except ValueError:
            raise ConfigError("探测范围 ID 无效") from None
    if (request["source_group_id"] not in settings.sources
            or request["target_group_id"] not in settings.targets
            or request["operator_id"] not in auth["operator_qq_ids"]
            or {"source_group_id": request["source_group_id"], "target_group_id": request["target_group_id"]} not in auth["routes"]):
        raise ConfigError("探测请求越过授权范围")
    messages = request["messages"]
    if not isinstance(messages, list) or not messages or len(messages) > settings.data["sending"]["max_probe_messages_per_day"]:
        raise ConfigError("探测消息数量无效")
    for message in messages:
        if not isinstance(message, list) or not message or len(message) > 100:
            raise ConfigError("探测消息段无效")
        for item in message:
            if not isinstance(item, dict) or set(item) != {"type", "data"} or not isinstance(item["data"], dict):
                raise ConfigError("探测消息段无效")
            data = item["data"]
            if item["type"] == "text":
                if set(data) != {"text"} or not isinstance(data["text"], str) or not 0 < len(data["text"]) <= 4000:
                    raise ConfigError("探测文本长度无效")
            elif item["type"] == "image":
                if set(data) != {"file"} or not isinstance(data["file"], str) or not data["file"].startswith("base64://"):
                    raise ConfigError("探测图片使用人工准备的 base64 文件内容")
                try:
                    payload = base64.b64decode(data["file"][9:], validate=True)
                except ValueError:
                    raise ConfigError("探测图片编码无效") from None
                if not payload or len(payload) > settings.data["media"]["max_download_bytes"]:
                    raise ConfigError("探测图片大小无效")
            else:
                raise ConfigError("探测支持文本和图片消息段")
    return request


async def send_probe(settings: Settings, store: Store, client, request: dict,
                     *, reload_settings=None) -> dict:
    # Tests supply a local settings callback; production always re-reads from disk.
    reload_settings = reload_settings or (lambda: load_settings(settings.root, settings.data["profile"]))
    settings.validate_live("probe")
    if not settings.account:
        raise ConfigError("发送探测需要已认证的平台账号")
    validate_request(request, settings)
    probe = request["probe_id"]
    with sender_lock(settings.path("storage", "database_path")):
        store.recover_probe(settings.account, probe)
        if not store.begin_probe(settings.account, request, settings.data["sending"]["max_probe_messages_per_day"]):
            return store.probe(settings.account, probe)
        for index, message in enumerate(request["messages"]):
            try:
                current = reload_settings()
                if current.data["napcat"] != settings.data["napcat"]:
                    raise ConfigError("发送期间连接配置改变")
                current = current.with_account(settings.account)
                current.validate_live("probe")
                validate_request(request, current)
                if store.paused(settings.account):
                    raise ConfigError("发送已暂停")
            except ConfigError:
                for pending in range(index, len(request["messages"])):
                    store.finish_part(settings.account, probe, pending, "cancelled", error_code="control_changed")
                break
            try:
                result = await client.call("send_group_msg", {
                    "group_id": int(request["target_group_id"]), "message": message,
                })
                receipt = decimal_id(result.get("message_id"), signed=True)
            except ApiError:
                store.finish_part(settings.account, probe, index, "failed", error_code="api_rejected")
            except asyncio.CancelledError:
                store.finish_part(settings.account, probe, index, "unknown", error_code="cancelled_inflight")
                raise
            except (TransportError, ValueError):
                store.finish_part(settings.account, probe, index, "unknown", error_code="receipt_unknown")
            else:
                store.finish_part(settings.account, probe, index, "sent", message_id=receipt)
                continue
            for pending in range(index + 1, len(request["messages"])):
                store.finish_part(settings.account, probe, pending, "cancelled", error_code="earlier_part_failed")
            break
        return store.probe(settings.account, probe)
