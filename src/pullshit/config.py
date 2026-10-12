"""Strict, layered JSON configuration; diagnostics never include input values."""

from __future__ import annotations

import copy
import json
import os
import re
from dataclasses import dataclass, field, replace
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit


class ConfigError(ValueError):
    pass


SCHEMA = {
    "schema_version": int,
    "profile": str,
    "runtime": {"mode": str},
    "napcat": {
        "transport": str, "endpoint": str, "access_token_env": str,
        "request_timeout_seconds": int, "reconnect_initial_seconds": int,
        "reconnect_max_seconds": int,
    },
    "authorization": {
        "source_group_ids": [str], "target_group_ids": [str],
        "operator_qq_ids": [str],
        "routes": [{"source_group_id": str, "target_group_id": str}],
    },
    "sending": {"enabled": bool, "paused": bool, "max_probe_messages_per_day": int},
    "storage": {
        "database_path": str, "media_directory": str,
        "raw_event_retention_days": int, "media_retention_days": int,
        "cleanup_interval_seconds": int,
    },
    "messages": {"forward_max_depth": int, "forward_max_nodes": int},
    "context": {
        "max_reply_depth": int, "max_fetch_calls": int, "total_timeout_seconds": int,
        "nearby_messages_each_side": int, "nearby_window_seconds": int,
        "failure_cache_seconds": int, "max_media_per_query": int, "max_text_characters": int,
    },
    "media": {
        "max_download_bytes": int, "download_timeout_seconds": int,
        "allowed_protocols": [str], "allow_private_network": bool, "max_redirects": int,
    },
    "feedback": {"polling_enabled": bool, "max_queries_per_probe": int},
    "logging": {"level": str, "directory": str, "include_raw_payload": bool, "retention_days": int},
}


def _object(pairs: list[tuple[str, Any]]) -> dict:
    result = {}
    for key, value in pairs:
        if key in result:
            raise ConfigError("JSON 含重复字段")
        result[key] = value
    return result


def read_json(path: Path) -> dict:
    try:
        value = json.loads(path.read_text(), object_pairs_hook=_object,
                           parse_constant=lambda _: (_ for _ in ()).throw(ConfigError("JSON 含非有限数值")))
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise ConfigError("配置文件缺失、不可读取或 JSON 格式错误") from exc
    if not isinstance(value, dict):
        raise ConfigError("配置根节点必须是对象")
    return value


def _shape(value: Any, schema: Any, path: str = "", partial: bool = False) -> None:
    if isinstance(schema, dict):
        if not isinstance(value, dict):
            raise ConfigError(f"{path or 'root'} 必须是对象")
        if set(value) - set(schema):
            raise ConfigError(f"{path or 'root'} 含未知字段")
        if not partial and set(schema) - set(value):
            raise ConfigError(f"{path or 'root'} 缺少字段")
        for key, item in value.items():
            _shape(item, schema[key], f"{path}.{key}".strip("."), partial)
    elif isinstance(schema, list):
        if not isinstance(value, list):
            raise ConfigError(f"{path} 必须是数组")
        for item in value:
            _shape(item, schema[0], path + "[]", False)
    elif type(value) is not schema:
        raise ConfigError(f"{path} 类型错误")


def merge(base: dict, overlay: dict) -> dict:
    result = copy.deepcopy(base)
    for key, value in overlay.items():
        if isinstance(value, dict) and isinstance(result.get(key), dict):
            result[key] = merge(result[key], value)
        else:
            result[key] = copy.deepcopy(value)
    return result


def decimal_id(value: Any, *, signed: bool = False) -> str:
    if isinstance(value, bool) or not isinstance(value, (str, int)):
        raise ValueError("平台 ID 格式错误")
    text = str(value)
    if not re.fullmatch(r"-?[0-9]+" if signed else r"[1-9][0-9]*", text):
        raise ValueError("平台 ID 格式错误")
    return text


@dataclass(frozen=True)
class Settings:
    root: Path
    data: dict = field(repr=False)
    account_id: str = field(default="", repr=False)

    @property
    def account(self) -> str:
        return self.account_id

    def with_account(self, account: str) -> Settings:
        return replace(self, account_id=decimal_id(account))

    @property
    def sources(self) -> set[str]:
        return set(self.data["authorization"]["source_group_ids"])

    @property
    def targets(self) -> set[str]:
        return set(self.data["authorization"]["target_group_ids"])

    def path(self, section: str, key: str) -> Path:
        return (self.root / self.data[section][key]).resolve()

    def token(self) -> str:
        token = os.environ.get(self.data["napcat"]["access_token_env"], "")
        if not token or any(char in token for char in "\r\n"):
            raise ConfigError("NapCat token 环境变量未配置或格式错误")
        return token

    def validate_live(self, mode: str) -> None:
        if mode not in {"receive", "probe"}:
            raise ConfigError("在线模式错误")
        if not self.sources:
            raise ConfigError("在线接入需要授权源群")
        if self.data["napcat"]["transport"] != "websocket" or not self.data["napcat"]["endpoint"]:
            raise ConfigError("在线接入需要正向 WebSocket 配置")
        self.token()
        if mode == "probe":
            if self.data["runtime"]["mode"] != "probe":
                raise ConfigError("发送探测需要 runtime.mode=probe")
            if len(self.targets) != 1 or not self.data["authorization"]["operator_qq_ids"]:
                raise ConfigError("发送探测需要一个目标群和授权操作者")
            if not self.data["authorization"]["routes"]:
                raise ConfigError("发送探测需要授权路由")
            if not self.data["sending"]["enabled"] or self.data["sending"]["paused"]:
                raise ConfigError("发送尚未启用或已暂停")


def load_settings(root: Path, profile: str) -> Settings:
    root = root.resolve()
    if profile not in {"development", "production"}:
        raise ConfigError("环境必须是 development 或 production")
    defaults = read_json(root / "config/defaults.json")
    environment = read_json(root / f"config/{profile}.json")
    local = read_json(root / f"config/local/{profile}.json")
    if "schema_version" in local or "profile" in local:
        raise ConfigError("本地配置禁止覆盖 schema_version 或 profile")
    for layer in (defaults, environment, local):
        _shape(layer, SCHEMA, partial=True)
    value = merge(merge(defaults, environment), local)
    _shape(value, SCHEMA)
    if value["schema_version"] != 1 or value["profile"] != profile:
        raise ConfigError("配置版本或环境不匹配")
    if value["runtime"]["mode"] not in {"receive", "probe", "replay"}:
        raise ConfigError("runtime.mode 无效")
    try:
        auth = value["authorization"]
        for key in ("source_group_ids", "target_group_ids", "operator_qq_ids"):
            for item in auth[key]:
                decimal_id(item)
            if len(set(auth[key])) != len(auth[key]):
                raise ConfigError(f"authorization.{key} 含重复 ID")
        seen = set()
        for route in auth["routes"]:
            pair = (route["source_group_id"], route["target_group_id"])
            if pair[0] not in auth["source_group_ids"] or pair[1] not in auth["target_group_ids"] or pair in seen:
                raise ConfigError("路由越过白名单或重复")
            seen.add(pair)
    except ValueError as exc:
        if isinstance(exc, ConfigError):
            raise
        raise ConfigError("账号、群或操作者 ID 无效") from exc
    for section, schema in SCHEMA.items():
        if isinstance(schema, dict):
            for key, kind in schema.items():
                if kind is int and value[section][key] <= 0:
                    raise ConfigError(f"{section}.{key} 必须是正整数")
    nc = value["napcat"]
    if nc["reconnect_max_seconds"] < nc["reconnect_initial_seconds"]:
        raise ConfigError("重连最大间隔小于初始间隔")
    if nc["transport"] not in {"", "websocket"}:
        raise ConfigError("当前适配器支持 websocket")
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", nc["access_token_env"]):
        raise ConfigError("token 环境变量名无效")
    if nc["endpoint"]:
        try:
            endpoint = urlsplit(nc["endpoint"])
            port = endpoint.port
            if (endpoint.scheme not in {"ws", "wss"} or not endpoint.hostname
                    or endpoint.username or endpoint.password or endpoint.query or endpoint.fragment
                    or endpoint.path not in {"", "/"}):
                raise ValueError
        except ValueError as exc:
            raise ConfigError("需要提供无凭据和查询参数的联合 WS 根地址") from exc
    for section, key, prefix in (("storage", "database_path", "data"),
                                 ("storage", "media_directory", "data"),
                                 ("logging", "directory", "logs")):
        baseline = environment.get(section, {}).get(key)
        if not baseline or value[section][key] != baseline:
            raise ConfigError(f"{section}.{key} 由环境配置固定")
        resolved = (root / baseline).resolve()
        scope = (root / prefix / profile).resolve()
        if (key == "database_path" and resolved == scope) or not resolved.is_relative_to(scope) or scope != root / prefix / profile:
            raise ConfigError(f"{section}.{key} 必须位于独立环境目录")
    if value["logging"]["level"] not in {"debug", "info", "warning", "error"}:
        raise ConfigError("logging.level 无效")
    if value["logging"]["include_raw_payload"]:
        raise ConfigError("日志禁止记录原始载荷")
    if value["media"]["allow_private_network"] or set(value["media"]["allowed_protocols"]) - {"http", "https"}:
        raise ConfigError("媒体下载需要公共 HTTP(S) 地址")
    if not value["media"]["allowed_protocols"]:
        raise ConfigError("media.allowed_protocols 不能为空")
    if value["feedback"]["polling_enabled"]:
        raise ConfigError("反馈查询协议待联调，当前启用实时事件记录")
    return Settings(root, value)
