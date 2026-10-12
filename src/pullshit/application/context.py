"""Read-only, scoped context assembly. Platform evidence stays separate from events."""

from __future__ import annotations

import asyncio
import hashlib
import json
import re
import sys
from datetime import datetime, timezone
from pathlib import Path

from pullshit.config import ConfigError, decimal_id, load_settings
from pullshit.adapters.qq.events import canonical, segments
from pullshit.media.cache import MediaError, read_cached
from pullshit.adapters.qq.napcat import ApiError, TransportError


async def download_image(url: str, directory: Path, limits: dict, timeout: float) -> dict:
    request = json.dumps({"url": url, "directory": str(directory), "limits": limits}).encode()
    if len(request) > 32768:
        raise MediaError("媒体地址过长")
    process = await asyncio.create_subprocess_exec(
        sys.executable, "-m", "pullshit.media.worker", stdin=asyncio.subprocess.PIPE,
        stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.DEVNULL,
    )
    try:
        async with asyncio.timeout(min(timeout, limits.get("download_timeout_seconds", timeout))):
            output, _ = await process.communicate(request)
        if len(output) > 4096:
            raise MediaError("媒体下载失败")
        result = json.loads(output)
        if not isinstance(result, dict):
            raise ValueError
        if process.returncode:
            code = result.get("error", "")
            if not isinstance(code, str) or not re.fullmatch(r"media_[a-z_]{1,50}", code):
                code = "media_unavailable"
            raise MediaError("媒体下载失败", code)
        return result
    except TimeoutError:
        raise MediaError("媒体下载已超时", "media_timeout") from None
    except MediaError:
        raise
    except (ValueError, UnicodeError):
        raise MediaError("媒体缓存结果无效") from None
    finally:
        if process.returncode is None:
            process.kill()
            await process.wait()


class ContextResolver:
    def __init__(self, settings, store, client=None, *, cache_media=False,
                 downloader=download_image, reload_settings=None, deadline=None):
        if not settings.account:
            raise ConfigError("上下文查询需要明确运行账号")
        if cache_media and client is None:
            raise ConfigError("图片下载需要显式在线查询")
        if client is not None and client.account != settings.account:
            raise ConfigError("上下文连接账号不匹配")
        self.settings, self.store, self.client = settings, store, client
        self.reload = reload_settings or (lambda: load_settings(settings.root, settings.data["profile"]))
        self.downloader, self.cache_media = downloader, cache_media
        self.limits = dict(settings.data["context"])
        self.forward_depth = settings.data["messages"]["forward_max_depth"]
        self.max_nodes = settings.data["messages"]["forward_max_nodes"]
        self.deadline = deadline
        self.group = ""
        self.issues = set()
        self.stats = {"fetch_calls": 0, "identity_checks": 0, "nodes": 0,
                      "media_downloads": 0, "media_cache_hits": 0}
        self.text_used = 0
        self.result = None
        self.renewed_messages = {}

    def _permissions(self):
        current = self.reload().with_account(self.settings.account)
        if self.group not in current.sources | current.targets:
            raise ConfigError("上下文查询超出当前群授权")
        if self.client is not None and current.data["napcat"] != self.settings.data["napcat"]:
            raise ConfigError("上下文查询期间连接配置改变")
        for key in self.limits:
            self.limits[key] = min(self.limits[key], current.data["context"][key])
        self.max_nodes = min(self.max_nodes, current.data["messages"]["forward_max_nodes"])
        self.forward_depth = min(self.forward_depth, current.data["messages"]["forward_max_depth"])
        return current

    def _check(self):
        current = self._permissions()
        if asyncio.get_running_loop().time() >= self.deadline:
            raise TimeoutError
        return current

    def _issue(self, code):
        self.issues.add(code)
        return code

    def _take_node(self):
        if self.stats["nodes"] >= self.max_nodes:
            self._issue("node_limit")
            return False
        self.stats["nodes"] += 1
        return True

    def _cached(self, kind, reference):
        try:
            cached = self.store.context_cached(self.settings.account, self.group, kind, reference)
            if cached is None:
                return None
            if cached["status"] == "found" and not isinstance(cached["payload"], dict):
                raise ValueError
            age = (datetime.now(timezone.utc) - datetime.fromisoformat(cached["checked_at"])).total_seconds()
            ttl = (self.settings.data["storage"]["raw_event_retention_days"] * 86400
                   if cached["status"] == "found" else self.limits["failure_cache_seconds"])
            return cached if 0 <= age < ttl else None
        except (ValueError, TypeError):
            self._issue("invalid_cache")
            return None

    async def _reference(self, kind, reference, *, refresh=False):
        self._check()
        cached = self._cached(kind, reference) if not refresh else None
        if cached is not None:
            if cached["status"] == "found":
                return cached["payload"], "cache", None
            return None, "cache", cached["error_code"] or "unavailable"
        if self.client is None:
            return None, "local", "missing_offline"
        if self.stats["fetch_calls"] >= self.limits["max_fetch_calls"]:
            return None, "platform", "fetch_limit"
        try:
            # Forward responses have no account identifier, so confirm identity
            # before every content fetch, including get_msg.
            self.stats["identity_checks"] += 1
            login = await self.client.call("get_login_info")
            if decimal_id(login.get("user_id")) != self.settings.account:
                raise ConfigError("平台登录账号发生变化，停止上下文回取")
            self._check()
            self.stats["fetch_calls"] += 1
            action = "get_msg" if kind == "message" else "get_forward_msg"
            params = {"message_id": int(reference)} if kind == "message" else {"id": reference}
            payload = await self.client.call(action, params)
            self._check()
            if kind == "message":
                self._validate_message(payload, reference)
            elif not isinstance(payload, dict) or not isinstance(payload.get("messages", payload.get("message")), list):
                raise ValueError
            if len(canonical(payload).encode()) > 2 * 1024 * 1024:
                raise ValueError
        except ConfigError:
            raise
        except ApiError:
            error = "api_rejected"
        except TransportError:
            error = "transport_error"
        except (ValueError, TypeError, KeyError, RecursionError):
            error = "invalid_or_mismatched_payload"
        else:
            self.store.cache_context(self.settings.account, self.group, kind, reference, "found", payload)
            return payload, "platform", None
        self._check()
        self.store.cache_context(self.settings.account, self.group, kind, reference,
                                 "unavailable", error_code=error)
        return None, "platform", error

    def _validate_message(self, raw, reference):
        if not isinstance(raw, dict) or raw.get("message_type") != "group":
            raise ValueError
        if decimal_id(raw.get("group_id")) != self.group:
            raise ValueError
        if decimal_id(raw.get("message_id"), signed=True) != reference:
            raise ValueError
        if "self_id" in raw and decimal_id(raw["self_id"]) != self.settings.account:
            raise ValueError
        sender = raw.get("sender") if isinstance(raw.get("sender"), dict) else {}
        decimal_id(raw.get("user_id", sender.get("user_id")))
        if type(raw.get("time")) is not int or raw["time"] < 0 or not isinstance(raw.get("message"), (list, str)):
            raise ValueError

    def _header(self, raw, reference, source, recalled=False):
        sender = raw.get("sender") if isinstance(raw.get("sender"), dict) else {}
        user = decimal_id(raw.get("user_id", sender.get("user_id")))
        return {"message_id": reference, "user_id": user, "time": raw["time"],
                "source": source, "status": "recalled" if recalled else "found",
                "own_message": user == self.settings.account or raw.get("post_type") == "message_sent",
                "recalled": recalled, "segments": [], "issues": []}

    async def _message(self, reference, depth, path, destination=None):
        self._check()
        key = ("message", reference)
        if key in path:
            return {"message_id": reference, "status": self._issue("reply_cycle")}
        if depth > self.limits["max_reply_depth"]:
            return {"message_id": reference, "status": self._issue("reply_depth_limit")}
        local = self.store.context_message(self.settings.account, self.group, reference)
        if local is not None:
            raw, source, error = local["payload"], "local", None
        else:
            raw, source, error = await self._reference("message", reference)
        if error:
            return {"message_id": reference, "source": source, "status": self._issue(error)}
        try:
            self._validate_message(raw, reference)
        except (ValueError, TypeError):
            return {"message_id": reference, "status": self._issue("invalid_or_mismatched_payload")}
        recalled = bool(local and local["recalled"]) or self.store.context_recalled(
            self.settings.account, self.group, reference)
        result = self._header(raw, reference, source, recalled)
        if destination is not None:
            destination["context"] = result
        if depth == 0:
            self.result["message"] = result
        if recalled:
            self._issue("recalled")
            result["segments"] = self._plain_segments(raw["message"])
            return result
        await self._segments(raw["message"], result["segments"], depth, 0, path | {key}, False, reference)
        if self.store.context_recalled(self.settings.account, self.group, reference):
            result.update({"status": "recalled", "recalled": True})
            self._issue("recalled")
        return result

    def _plain_data(self, kind, data):
        if kind == "text":
            text = data.get("text", "")
            if not isinstance(text, str):
                self._issue("invalid_text")
                return {"text": ""}
            remaining = max(0, self.limits["max_text_characters"] - self.text_used)
            self.text_used += min(len(text), remaining)
            if len(text) > remaining:
                self._issue("text_limit")
            return {"text": text[:remaining], **({"truncated": True} if len(text) > remaining else {})}
        if kind == "image":
            # Access URLs and inline base64 belong to private evidence, not to
            # exported context JSON or diagnostics.
            return {key: value for key, value in data.items()
                    if key in {"summary", "sub_type", "file_size"} and isinstance(value, (str, int))}
        if kind in {"node", "forward"}:
            return {key: value for key, value in data.items()
                    if key in {"id", "user_id", "uin", "nickname", "name", "time"}
                    and isinstance(value, (str, int))}
        return {key: value for key, value in data.items()
                if key not in {"url", "file", "raw"} and isinstance(value, (str, int, bool))}

    def _plain_segments(self, body):
        normalized, issues = segments(body, self.forward_depth, self.max_nodes)
        self.issues.update(issues)
        result = []
        for segment in normalized:
            if not self._take_node():
                break
            result.append({"type": segment["type"], "data": self._plain_data(segment["type"], segment["data"])})
        return result

    async def _segments(self, body, destination, reply_depth, forward_depth, path, embedded, message_reference=None):
        normalized, issues = segments(body, self.forward_depth, self.max_nodes)
        # The resolver replaces the normalizer's 'not fetched' marker with the
        # actual expansion result. All other evidence-quality markers survive.
        self.issues.update(code for code in issues if code != "forward_not_fetched")
        for segment in normalized:
            self._check()
            if message_reference and self.store.context_recalled(self.settings.account, self.group, message_reference):
                self._issue("recalled")
                break
            if not self._take_node():
                break
            kind, data = segment["type"], segment["data"]
            item = {"type": kind, "data": self._plain_data(kind, data)}
            destination.append(item)
            if kind == "reply":
                try:
                    reference = decimal_id(data.get("id"), signed=True)
                except ValueError:
                    item["context"] = {"status": self._issue("missing_reply_id")}
                else:
                    if embedded:
                        item["context"] = {"message_id": reference, "status": self._issue("embedded_reply_source_unverified")}
                    else:
                        item["context"] = await self._message(reference, reply_depth + 1, path, item)
            elif kind == "forward":
                reference = data.get("id")
                if not isinstance(reference, (str, int)) or isinstance(reference, bool):
                    reference = None
                else:
                    reference = str(reference)
                inline = data.get("content")
                item["context"] = await self._forward(reference, inline, forward_depth + 1,
                                                      reply_depth, path, item)
            elif kind == "node":
                item["context"] = {"status": "inline", "origin_group_verified": False, "segments": []}
                if forward_depth >= self.forward_depth:
                    item["context"]["status"] = self._issue("forward_depth_limit")
                else:
                    await self._segments(data.get("content", []), item["context"]["segments"],
                                         reply_depth, forward_depth + 1, path, True)
            elif kind == "image":
                item["media"] = await self._image(data, message_reference)

    async def _forward(self, reference, inline, depth, reply_depth, path, destination=None):
        self._check()
        result = {"reference_id": reference, "nodes": []}
        if destination is not None:
            destination["context"] = result
        if depth > self.forward_depth:
            result["status"] = self._issue("forward_depth_limit")
            return result
        key = ("forward", reference)
        if reference is not None and key in path:
            result["status"] = self._issue("forward_cycle")
            return result
        if isinstance(inline, list):
            nodes, source, error = inline, "inline", None
        elif reference and len(reference) <= 256 and not any(ord(c) < 32 for c in reference):
            payload, source, error = await self._reference("forward", reference)
            nodes = payload.get("messages", payload.get("message")) if payload else []
        else:
            nodes, source, error = [], "local", "missing_forward_id"
        result.update({"source": source, "status": "found"})
        if error:
            result["status"] = self._issue(error)
            return result
        for raw in nodes:
            self._check()
            if not self._take_node():
                result["status"] = "node_limit"
                break
            if not isinstance(raw, dict):
                self._issue("invalid_forward_node")
                continue
            data = raw.get("data", {}) if raw.get("type") == "node" else raw
            if not isinstance(data, dict):
                self._issue("invalid_forward_node")
                continue
            sender = data.get("sender") if isinstance(data.get("sender"), dict) else {}
            node = {"origin_group_verified": False, "segments": [],
                    "user_id": data.get("user_id", data.get("uin", sender.get("user_id"))),
                    "nickname": data.get("nickname", data.get("name", sender.get("nickname"))),
                    "time": data.get("time")}
            for field in ("user_id", "nickname", "time"):
                if not isinstance(node[field], (str, int)) or isinstance(node[field], bool):
                    node[field] = None
            result["nodes"].append(node)
            body = data.get("content")
            if not body and isinstance(data.get("message"), (list, str)):
                body = data["message"]
            await self._segments(body if body is not None else [], node["segments"],
                                 reply_depth, depth, path | {key}, True)
        return result

    async def _renew_image_url(self, data, message_reference, previous_url):
        if message_reference is None:
            return None
        if message_reference not in self.renewed_messages:
            payload, _, error = await self._reference("message", message_reference, refresh=True)
            if error:
                self._issue(error)
            self.renewed_messages[message_reference] = payload
        payload = self.renewed_messages[message_reference]
        if not payload or not isinstance(payload.get("message"), list):
            return None
        # Only match a stable, unique file identifier in a verified message.
        # Forward nodes do not carry a verified source message for this lookup.
        identity = data.get("file")
        if not isinstance(identity, str) or not identity:
            return None
        matches = [v["data"] for v in payload["message"][:self.max_nodes]
                   if isinstance(v, dict) and v.get("type") == "image" and isinstance(v.get("data"), dict)
                   and v["data"].get("file") == identity]
        if len(matches) != 1:
            return None
        fresh = matches[0].get("url")
        return fresh if isinstance(fresh, str) and fresh != previous_url and fresh.startswith(("http://", "https://")) else None

    async def _image(self, data, message_reference=None):
        self._check()
        identity = data.get("file") or data.get("url")
        if not isinstance(identity, str) or not identity:
            return {"status": self._issue("missing_image_reference")}
        reference = hashlib.sha256(identity.encode()).hexdigest()
        cached = self._cached("image", reference)
        if cached is not None and cached["status"] == "found":
            try:
                return self._cached_image(cached["payload"], "cache")
            except (MediaError, ValueError, KeyError, TypeError) as exc:
                self._issue("invalid_media_cache")
                if isinstance(exc, MediaError):
                    self._issue(exc.code)
        elif cached is not None:
            return {"reference_id": reference, "status": self._issue(cached["error_code"] or "media_unavailable")}
        if not self.cache_media:
            return {"reference_id": reference, "status": self._issue("media_not_cached")}
        if self.stats["media_downloads"] >= self.limits["max_media_per_query"]:
            return {"reference_id": reference, "status": self._issue("media_limit")}
        url = data.get("url") or data.get("file")
        if not isinstance(url, str) or not url.startswith(("http://", "https://")):
            return {"reference_id": reference, "status": self._issue("image_url_unavailable")}
        for attempt in range(2):
            self.stats["media_downloads"] += 1
            current = self._check()
            limits = dict(current.data["media"])
            remaining = self.deadline - asyncio.get_running_loop().time()
            limits["download_timeout_seconds"] = min(limits["download_timeout_seconds"], remaining)
            try:
                metadata = await self.downloader(url, current.path("storage", "media_directory"), limits, remaining)
                self._check()
                result = self._cached_image(metadata, "download")
            except (MediaError, ValueError, KeyError, TypeError) as exc:
                self._check()
                error = exc.code if isinstance(exc, MediaError) else "media_cache_invalid"
                if (attempt == 0 and error in {"media_unavailable", "media_http_rejected", "media_timeout", "media_incomplete"}
                        and self.stats["media_downloads"] < self.limits["max_media_per_query"]):
                    fresh = await self._renew_image_url(data, message_reference, url)
                    if fresh:
                        url = fresh
                        continue
                self.store.cache_context(self.settings.account, self.group, "image", reference,
                                         "unavailable", error_code=error)
                return {"reference_id": reference, "status": self._issue(error)}
            self.store.cache_context(self.settings.account, self.group, "image", reference, "found", metadata)
            if attempt:
                result["url_refreshed"] = True
            return result

    def _cached_image(self, metadata, source):
        if not isinstance(metadata, dict) or not re.fullmatch(r"[0-9a-f]{64}", metadata.get("sha256", "")):
            raise ValueError
        payload = read_cached(self.settings.path("storage", "media_directory"), metadata["sha256"],
                              self.settings.data["storage"]["media_retention_days"],
                              self.settings.data["media"]["max_download_bytes"])
        if metadata.get("bytes") != len(payload):
            raise ValueError
        if source == "cache":
            self.stats["media_cache_hits"] += 1
        return {"status": "cached", "source": source, "sha256": metadata["sha256"],
                "bytes": len(payload), "type": metadata.get("type"),
                "path": str(self.settings.path("storage", "media_directory") / metadata["sha256"])}

    async def resolve(self, group: str, message: str):
        self.group = decimal_id(group)
        message = decimal_id(message, signed=True)
        if self.deadline is None:
            self.deadline = asyncio.get_running_loop().time() + self.limits["total_timeout_seconds"]
        self.result = {"group_id": group, "message_id": message, "online": self.client is not None,
                       "message": None, "nearby": {"relation": "time_neighbor", "before": [], "after": []}}
        try:
            self._check()
            async with asyncio.timeout_at(self.deadline):
                self.result["message"] = await self._message(message, 0, set())
                root = self.result["message"]
                if root.get("time") is not None:
                    nearby = self.store.nearby_messages(self.settings.account, self.group, message, root["time"],
                                                       self.limits["nearby_messages_each_side"],
                                                       self.limits["nearby_window_seconds"])
                    for side, values in nearby.items():
                        for value in values:
                            self._check()
                            raw = value["payload"]
                            item = self._header(raw, str(raw["message_id"]), "local", value["recalled"])
                            item["segments"] = self._plain_segments(raw["message"])
                            self.result["nearby"][side].append(item)
        except TimeoutError:
            self._issue("timeout")
        self._permissions()  # Discard the result if authorization changed at the deadline.
        self.result.update({"status": "partial" if self.issues else "complete",
                            "issues": sorted(self.issues), "stats": dict(self.stats)})
        return self.result
