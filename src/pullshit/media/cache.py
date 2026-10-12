"""Bounded image cache with public-address checks and DNS-pinned connections."""

from __future__ import annotations

import hashlib
import http.client
import ipaddress
import json
import os
import socket
import ssl
import tempfile
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from urllib.parse import urljoin, urlsplit


class MediaError(ValueError):
    def __init__(self, message: str, code: str = "media_unavailable"):
        super().__init__(message)
        self.code = code


def validate_url(url: str, protocols: list[str]) -> tuple:
    try:
        parsed = urlsplit(url)
        if (parsed.scheme not in protocols or not parsed.hostname or parsed.username or parsed.password
                or any(char.isspace() or ord(char) < 32 for char in url)):
            raise ValueError
        port = parsed.port or (443 if parsed.scheme == "https" else 80)
        addresses = socket.getaddrinfo(parsed.hostname, port, type=socket.SOCK_STREAM)
        ips = sorted({item[4][0] for item in addresses})
        if not ips or any(not ipaddress.ip_address(ip).is_global for ip in ips):
            raise ValueError
        return parsed, ips[0], port
    except (ValueError, OSError):
        raise MediaError("媒体地址必须是公共 HTTP(S) 地址", "media_address_rejected") from None


class _PinnedHTTP(http.client.HTTPConnection):
    def __init__(self, host: str, port: int, address: str, timeout: float):
        super().__init__(host, port, timeout=timeout)
        self.address = address

    def connect(self):
        self.sock = socket.create_connection((self.address, self.port), self.timeout)


class _PinnedHTTPS(_PinnedHTTP):
    def connect(self):
        super().connect()
        try:
            self.sock = ssl.create_default_context().wrap_socket(self.sock, server_hostname=self.host)
        except BaseException:
            self.sock.close()
            raise


def _image_type(payload: bytes) -> str:
    if payload.startswith(b"\x89PNG\r\n\x1a\n"):
        return "png"
    if payload.startswith(b"\xff\xd8\xff"):
        return "jpeg"
    if payload.startswith((b"GIF87a", b"GIF89a")):
        return "gif"
    if payload.startswith(b"RIFF") and payload[8:12] == b"WEBP":
        return "webp"
    raise MediaError("下载内容没有支持的图片文件头", "media_type_unsupported")


def cache_image(url: str, directory: Path, limits: dict, *, resolve=validate_url,
                connection_factory=None) -> dict:
    deadline = time.monotonic() + limits["download_timeout_seconds"]
    payload = bytearray()
    for redirect in range(limits["max_redirects"] + 1):
        parsed, address, port = resolve(url, limits["allowed_protocols"])
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise MediaError("媒体下载已超时", "media_timeout")
        factory = connection_factory or (_PinnedHTTPS if parsed.scheme == "https" else _PinnedHTTP)
        connection = factory(parsed.hostname, port, address, remaining)
        try:
            path = parsed.path or "/"
            if parsed.query:
                path += "?" + parsed.query
            connection.request("GET", path, headers={"Accept": "image/*", "User-Agent": "pullshit/0.1"})
            response = connection.getresponse()
            if response.status in {301, 302, 303, 307, 308}:
                location = response.getheader("Location")
                if not location or redirect == limits["max_redirects"]:
                    raise MediaError("媒体重定向超限或缺少目标", "media_redirect_limit")
                next_url = urljoin(url, location)
                if parsed.scheme == "https" and urlsplit(next_url).scheme != "https":
                    raise MediaError("媒体重定向降低传输保护", "media_address_rejected")
                url = next_url
                continue
            if response.status != 200:
                raise MediaError("媒体请求失败", "media_http_rejected")
            size = response.getheader("Content-Length")
            if size is not None and (not size.isdigit() or int(size) > limits["max_download_bytes"]):
                raise MediaError("媒体声明大小无效或超限", "media_size_limit")
            while True:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise MediaError("媒体下载已超时", "media_timeout")
                if connection.sock:
                    connection.sock.settimeout(remaining)
                chunk = response.read1(min(65536, limits["max_download_bytes"] + 1 - len(payload)))
                if not chunk:
                    break
                payload.extend(chunk)
                if len(payload) > limits["max_download_bytes"]:
                    raise MediaError("媒体实际大小超限", "media_size_limit")
            if size is not None and len(payload) != int(size):
                raise MediaError("媒体响应不完整", "media_incomplete")
            break
        except (OSError, http.client.HTTPException, ValueError) as exc:
            if isinstance(exc, MediaError):
                raise
            raise MediaError("媒体下载失败") from None
        finally:
            connection.close()
    image_type = _image_type(payload)
    digest = hashlib.sha256(payload).hexdigest()
    directory.mkdir(parents=True, exist_ok=True, mode=0o700)
    os.chmod(directory, 0o700)
    content = directory / digest
    with tempfile.NamedTemporaryFile(dir=directory, delete=False) as stream:
        temporary = Path(stream.name)
        try:
            stream.write(payload)
            stream.flush()
            os.fsync(stream.fileno())
            os.replace(temporary, content)
        finally:
            temporary.unlink(missing_ok=True)
    metadata = {"sha256": digest, "bytes": len(payload), "type": image_type,
                "cached_at": datetime.now(timezone.utc).isoformat()}
    # Store no access URL or query credentials in sidecar metadata.
    (directory / f"{digest}.json").write_text(json.dumps(metadata) + "\n")
    return metadata


def read_cached(directory: Path, digest: str, retention_days: int, max_bytes: int) -> bytes:
    if len(digest) != 64 or any(c not in "0123456789abcdef" for c in digest):
        raise MediaError("媒体校验值无效", "media_cache_invalid")
    try:
        metadata = json.loads((directory / f"{digest}.json").read_text())
        cached = datetime.fromisoformat(metadata["cached_at"])
        if cached < datetime.now(timezone.utc) - timedelta(days=retention_days):
            raise MediaError("媒体缓存已过期", "media_cache_expired")
        with (directory / digest).open("rb") as stream:
            payload = stream.read(max_bytes + 1)
        if len(payload) > max_bytes or hashlib.sha256(payload).hexdigest() != digest:
            raise MediaError("媒体缓存损坏或超限", "media_cache_invalid")
        return payload
    except (OSError, ValueError, KeyError, TypeError) as exc:
        if isinstance(exc, MediaError):
            raise
        raise MediaError("媒体缓存不可读取", "media_cache_unreadable") from None


def prune_media(directory: Path, retention_days: int) -> int:
    before = datetime.now(timezone.utc) - timedelta(days=retention_days)
    removed = 0
    for path in directory.glob("*.json"):
        if len(path.stem) != 64 or any(c not in "0123456789abcdef" for c in path.stem):
            continue
        try:
            cached = datetime.fromisoformat(json.loads(path.read_text())["cached_at"])
            if cached < before:
                (directory / path.stem).unlink(missing_ok=True)
                path.unlink()
                removed += 1
        except (OSError, ValueError, KeyError, TypeError):
            continue
    return removed
