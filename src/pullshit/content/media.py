"""Bounded image downloads, content-addressed cache and candidate windows.

Downloads run in a disposable subprocess so DNS and socket waits can be cut
off: the parent enforces a wall-clock budget and kills the whole process
group. Every hop (initial URL and each redirect) must be a public HTTP(S)
target; the resolved address is pinned for the connection and TLS hostname
validation always uses the original host. Bytes are capped per response and
per day through the store budget; content is stored by SHA-256 with JSON
manifest metadata, the first recorded expiry is never renewed, and admission
discards expired and unreferenced files within the configured cache size.

ContentWindow groups messages of one author into bounded quiet windows.
"""
from __future__ import annotations

import asyncio
import base64
import hashlib
import ipaddress
import json
import os
import signal
import socket
import http.client
import ssl
import sys
import time
from pathlib import Path
from urllib.parse import urlsplit, urljoin

from pullshit.contracts import MediaRef, MessageRef, digest, now as epoch_now

READ_CHUNK = 65536
HEADER_LIMIT = 65536
TMP_GRACE_SECONDS = 3600
WORKER_GRACE_SECONDS = 5


class MediaError(Exception):
    code = 'media_download_error'
    actual_bytes = 0


class MediaAddressRejected(MediaError):
    code = 'media_address_rejected'


class MediaHttpRejected(MediaError):
    code = 'media_http_rejected'


class MediaSizeLimit(MediaError):
    code = 'media_size_limit'


class MediaTypeUnsupported(MediaError):
    code = 'media_type_unsupported'


class MediaTimeout(MediaError):
    code = 'media_timeout'


class MediaBudgetExceeded(MediaError):
    code = 'media_budget_exceeded'


class MediaCacheFull(MediaError):
    code = 'media_cache_full'


_ERRORS = {cls.code: cls for cls in (MediaAddressRejected, MediaHttpRejected, MediaSizeLimit,
                                     MediaTypeUnsupported, MediaTimeout, MediaBudgetExceeded,
                                     MediaCacheFull)}


def _signature(data: bytes) -> str | None:
    if data[:3] == b'\xff\xd8\xff':
        return 'image/jpeg'
    if data[:8] == b'\x89PNG\r\n\x1a\n':
        return 'image/png'
    if data[:6] in (b'GIF87a', b'GIF89a'):
        return 'image/gif'
    if len(data) >= 12 and data[:4] == b'RIFF' and data[8:12] == b'WEBP':
        return 'image/webp'
    if data[:2] == b'BM':
        return 'image/bmp'
    return None


def _error_from_code(code, actual_bytes: int = 0) -> MediaError:
    error = _ERRORS.get(code, MediaError)()
    error.actual_bytes = int(actual_bytes or 0)
    return error


def _resolve_target(url: str, allow_private: bool) -> tuple[urlsplit.SplitResult, str, int, str]:
    """Validate scheme/credentials and pin the first resolved public address."""
    parts = urlsplit(url)
    if parts.scheme not in ('http', 'https') or not parts.hostname \
            or parts.username or parts.password or parts.fragment:
        raise MediaAddressRejected('unsupported media URL')
    host = parts.hostname
    port = parts.port or (443 if parts.scheme == 'https' else 80)
    infos = socket.getaddrinfo(host, port, type=socket.SOCK_STREAM)
    if not infos:
        raise MediaAddressRejected('unresolvable media host')
    address = infos[0][4][0].split('%')[0]
    ip = ipaddress.ip_address(address)
    if not allow_private and not ip.is_global:
        raise MediaAddressRejected('non-public media address')
    return parts, host, port, address


def _request_path(parts: urlsplit.SplitResult) -> str:
    path = parts.path or '/'
    if parts.query:
        path = f'{path}?{parts.query}'
    return path


def _read_response(sock, deadline, counter) -> tuple[bytes, bytes]:
    """Read status line and headers; returns (header_bytes, body_head)."""
    data = b''
    while b'\r\n\r\n' not in data:
        if len(data) > HEADER_LIMIT:
            raise MediaHttpRejected('media response headers too large')
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise MediaTimeout
        sock.settimeout(min(remaining, READ_CHUNK))
        try:
            chunk = sock.recv(READ_CHUNK)
        except (socket.timeout, TimeoutError):
            raise MediaTimeout from None
        if not chunk:
            raise MediaHttpRejected('media connection closed early')
        counter[0] += len(chunk)
        data += chunk
    head, _, body_head = data.partition(b'\r\n\r\n')
    return head, body_head


def _download(url: str, max_bytes: int, max_redirects: int, timeout: float,
              allow_private: bool = False) -> tuple[bytes, int]:
    """Read bounded response bodies from a pinned socket; HTTPResponse decodes chunking."""
    counter = 0
    deadline = time.monotonic() + timeout
    try:
        for _ in range(max_redirects + 1):
            parts, host, port, address = _resolve_target(url, allow_private)
            family = socket.AF_INET6 if ':' in address else socket.AF_INET
            sock = socket.socket(family, socket.SOCK_STREAM)
            response = None
            try:
                remaining = deadline - time.monotonic()
                if remaining <= 0: raise MediaTimeout()
                sock.settimeout(remaining); sock.connect((address, port))
                if parts.scheme == 'https':
                    sock = ssl.create_default_context().wrap_socket(sock, server_hostname=host)
                authority = '[' + host + ']' if ':' in host else host
                if port != (443 if parts.scheme == 'https' else 80): authority += ':' + str(port)
                request = ('GET ' + _request_path(parts) + ' HTTP/1.1\r\nHost: ' + authority +
                           '\r\nUser-Agent: pullshit/0.2\r\nAccept: image/*\r\nConnection: close\r\n\r\n').encode()
                sock.sendall(request)
                response = http.client.HTTPResponse(sock); response.begin()
                if 300 <= response.status < 400:
                    location = response.getheader('Location')
                    if not location: raise MediaHttpRejected('missing redirect')
                    url = urljoin(url, location); continue
                if not 200 <= response.status < 300: raise MediaHttpRejected('media HTTP rejection')
                length = response.getheader('Content-Length')
                if length and int(length) > max_bytes: raise MediaSizeLimit()
                body = bytearray()
                while True:
                    remaining = deadline - time.monotonic()
                    if remaining <= 0: raise MediaTimeout()
                    sock.settimeout(remaining)
                    chunk = response.read(min(READ_CHUNK, max_bytes - len(body) + 1))
                    if not chunk: break
                    counter += len(chunk); body.extend(chunk)
                    if len(body) > max_bytes: raise MediaSizeLimit()
                return bytes(body), counter
            finally:
                if response: response.close()
                sock.close()
        raise MediaHttpRejected('redirect limit')
    except MediaError as error:
        error.actual_bytes = counter
        raise
    except (socket.timeout, TimeoutError):
        error = MediaTimeout(); error.actual_bytes = counter; raise error from None
    except (OSError, ValueError, http.client.HTTPException):
        error = MediaHttpRejected(); error.actual_bytes = counter; raise error from None


async def download(url: str, max_bytes: int, max_redirects: int, timeout: float,
                   allow_private: bool = False, wall_timeout: float | None = None) -> tuple[bytes, int]:
    """Download in a disposable subprocess; a wall-clock kill guarantees the
    process group (and its descendants) cannot outlive the budget."""
    wall = timeout + WORKER_GRACE_SECONDS if wall_timeout is None else wall_timeout
    command = [sys.executable, '-m', 'pullshit.content.media_worker', url,
               '--max-bytes', str(int(max_bytes)),
               '--max-redirects', str(int(max_redirects)),
               '--timeout', str(float(timeout))]
    if allow_private:
        command.append('--allow-private')
    process = await asyncio.create_subprocess_exec(*command, stdout=asyncio.subprocess.PIPE,
                                                   stderr=asyncio.subprocess.PIPE,
                                                   preexec_fn=os.setsid)
    try:
        stdout, _ = await asyncio.wait_for(process.communicate(), wall)
    except (asyncio.TimeoutError, TimeoutError):
        _kill_process_group(process)
        await process.wait()
        raise MediaTimeout from None
    except asyncio.CancelledError:
        _kill_process_group(process)
        await process.wait()
        raise
    try:
        result = json.loads(stdout)
    except (ValueError, TypeError):
        raise MediaError('media worker produced no result') from None
    if not isinstance(result, dict):
        raise MediaError('media worker produced no result')
    if result.get('ok'):
        return base64.b64decode(result['data']), int(result.get('actual_bytes', 0))
    raise _error_from_code(result.get('error'), result.get('actual_bytes', 0))


def _kill_process_group(process) -> None:
    try:
        os.killpg(process.pid, signal.SIGKILL)
    except (ProcessLookupError, PermissionError, OSError):
        try:
            process.kill()
        except OSError:
            pass


class MediaCache:
    """Content-addressed image cache with per-day global download budget."""

    def __init__(self, settings, store):
        media = settings.data['media']
        self.settings = settings
        self.store = store
        self.directory: Path = settings.path('storage', 'media_directory')
        self.max_bytes = int(media['max_image_bytes'])
        self.timeout = float(media['download_timeout_seconds'])
        self.max_redirects = int(media['max_redirects'])
        self.cache_max_bytes = int(media['cache_max_bytes'])
        self.daily_limit = float(settings.data['limits']['daily_download_bytes'])
        self._lock = asyncio.Lock()
        self._dirs = (self.directory / 'files', self.directory / 'manifests',
                      self.directory / 'tmp', self.directory / 'index')
        for directory in self._dirs:
            directory.mkdir(parents=True, exist_ok=True, mode=0o700)

    async def cache(self, url: str, source: MessageRef, expires_at: float) -> MediaRef:
        """Download and cache an image, or reuse the existing cache entry.

        Raises MediaError subclasses on refusal, limits and transport
        failures; the store budget is reserved before any download attempt and
        settled with the actually received bytes (including failed reads).
        """
        entry = self._index_entry(url)
        if entry:
            sha = entry.get('sha256')
            manifest = self._read_manifest(sha) if isinstance(sha, str) else None
            if manifest and self._file(sha).is_file():
                expires = min(float(manifest['expires_at']), float(entry.get('expires_at', 0) or manifest['expires_at']), float(expires_at))
                payload = self._file(sha).read_bytes()
                if len(payload) != manifest['size'] or hashlib.sha256(payload).hexdigest() != sha:
                    self._file(sha).unlink(missing_ok=True); (self._dirs[1]/(sha+'.json')).unlink(missing_ok=True)
                elif expires > epoch_now():
                    return self._ref(sha, source, manifest['size'], manifest.get('mime'), expires)
            self._index_path(url).unlink(missing_ok=True)
        day = time.strftime('%Y-%m-%d', time.gmtime())
        reserved = float(self.max_bytes+1)
        actual = reserved
        if not self.store.reserve_budget(day, 'global', 'download_bytes', reserved, self.daily_limit):
            raise MediaBudgetExceeded
        try:
            try:
                data, actual = await download(url, self.max_bytes, self.max_redirects, self.timeout)
            except MediaError as exc:
                actual = exc.actual_bytes
                raise
            async with self._lock:
                ref = self.ingest(data, source, expires_at)
        finally:
            self.store.settle_budget(day, 'global', 'download_bytes', reserved, float(actual))
        self._write_index(url, ref.sha256, ref.expires_at)
        return ref

    def ingest(self, data: bytes, source: MessageRef, expires_at: float) -> MediaRef:
        """Store validated bytes by content; synchronous file work."""
        if len(data) > self.max_bytes:
            raise MediaSizeLimit
        mime = _signature(data)
        if mime is None:
            raise MediaTypeUnsupported
        sha = hashlib.sha256(data).hexdigest()
        now = epoch_now()
        expires_at = min(expires_at, now + self.settings.data['retention']['content_days']*86400)
        if expires_at <= now: raise MediaError('expired image')
        manifest = self._read_manifest(sha)
        if manifest and self._file(sha).is_file() and manifest['expires_at'] > now and hashlib.sha256(self._file(sha).read_bytes()).hexdigest()==sha:
            expires = min(float(manifest['expires_at']), float(expires_at))
            if expires != manifest['expires_at']:
                manifest['expires_at'] = expires
                self._write_manifest(sha, manifest)
            return self._ref(sha, source, manifest['size'], manifest['mime'], expires)
        self._ensure_capacity(len(data), now)
        for directory in self._dirs:
            directory.mkdir(parents=True, exist_ok=True)
        tmp = self.directory / 'tmp' / f'{os.getpid()}-{hashlib.sha256(os.urandom(8)).hexdigest()}'
        tmp.write_bytes(data)
        os.replace(tmp, self._file(sha))
        self._write_manifest(sha, {'sha256': sha, 'size': len(data), 'mime': mime,
                                   'source': [source.account_id, source.group_id, source.message_id],
                                   'created_at': now, 'expires_at': float(expires_at)})
        return self._ref(sha, source, len(data), mime, float(expires_at))

    def cleanup(self, now: float | None = None) -> dict:
        """Delete expired media, orphan content and temp files; return accounting."""
        now = epoch_now() if now is None else now
        removed_files = removed_bytes = 0
        for manifest_file in self._dirs[1].glob('*.json'):
            entry = self._read_manifest(manifest_file.stem)
            if not entry or float(entry['expires_at']) <= now:
                removed_files, removed_bytes = self._remove_file(
                    self._file(manifest_file.stem), removed_files, removed_bytes)
                removed_files, removed_bytes = self._remove_file(
                    manifest_file, removed_files, removed_bytes)
        for entry_file in self._dirs[3].glob('*.json'):
            entry = self._read_json(entry_file)
            if not entry or float(entry.get('expires_at', 0)) <= now \
                    or not isinstance(entry.get('sha256'), str) \
                    or not self._file(entry['sha256']).is_file():
                removed_files, removed_bytes = self._remove_file(entry_file, removed_files, removed_bytes)
        for path in self._dirs[0].iterdir():
            if path.is_file() and not (self._dirs[1] / f'{path.name}.json').exists() \
                    and now - path.stat().st_mtime > TMP_GRACE_SECONDS:
                removed_files, removed_bytes = self._remove_file(path, removed_files, removed_bytes)
        for path in self._dirs[2].iterdir():
            if path.is_file():
                removed_files, removed_bytes = self._remove_file(path, removed_files, removed_bytes)
        remaining_files = remaining_bytes = 0
        for path in self._dirs[0].iterdir():
            if path.is_file():
                remaining_files += 1
                remaining_bytes += path.stat().st_size
        return {'removed_files': removed_files, 'removed_bytes': removed_bytes,
                'remaining_files': remaining_files, 'remaining_bytes': remaining_bytes}

    def _ref(self, sha: str, source: MessageRef, size: int, mime: str | None,
             expires_at: float) -> MediaRef:
        return MediaRef(reference_id=digest([source.account_id, source.group_id,
                                             source.message_id, sha]),
                        source=source, path=str(self._file(sha)), sha256=sha,
                        size=int(size), mime=mime, expires_at=float(expires_at))

    def _file(self, sha: str) -> Path:
        return self._dirs[0] / sha

    def _index_path(self, url: str) -> Path:
        return self._dirs[3] / f'{digest(url)}.json'

    def _index_entry(self, url: str) -> dict | None:
        entry = self._read_json(self._index_path(url))
        return entry if isinstance(entry, dict) and isinstance(entry.get('sha256'), str) else None

    def _write_index(self, url: str, sha: str, expires_at: float) -> None:
        path = self._index_path(url)
        existing = self._read_json(path)
        if isinstance(existing, dict):
            expires_at = min(float(existing.get('expires_at', expires_at)), float(expires_at))
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps({'sha256': sha, 'expires_at': expires_at}, sort_keys=True))

    def _write_manifest(self, sha: str, value: dict) -> None:
        path = self._dirs[1] / (sha + '.json')
        temporary = path.with_suffix('.tmp')
        temporary.write_text(json.dumps(value, ensure_ascii=False))
        os.replace(temporary, path)

    def _read_manifest(self, sha: str) -> dict | None:
        entry = self._read_json(self._dirs[1] / f'{sha}.json')
        return entry if isinstance(entry, dict) and entry.get('sha256') == sha else None

    @staticmethod
    def _read_json(path: Path) -> dict | None:
        try:
            value = json.loads(path.read_text())
        except (OSError, ValueError, TypeError):
            return None
        return value if isinstance(value, dict) else None

    @staticmethod
    def _remove_file(path: Path, files: int, size: int) -> tuple[int, int]:
        try:
            size += path.stat().st_size
        except OSError:
            pass
        try:
            path.unlink()
            files += 1
        except OSError:
            pass
        return files, size

    def _usage(self) -> int:
        total = 0
        for path in self._dirs[0].iterdir():
            if path.is_file():
                try:
                    total += path.stat().st_size
                except OSError:
                    pass
        return total

    def _discard_expired(self, now: float) -> None:
        for manifest_file in self._dirs[1].glob('*.json'):
            entry = self._read_manifest(manifest_file.stem)
            if not entry or float(entry['expires_at']) <= now:
                self._file(manifest_file.stem).unlink(missing_ok=True)
                manifest_file.unlink(missing_ok=True)

    def _ensure_capacity(self, needed: int, now: float) -> None:
        if self._usage() + needed <= self.cache_max_bytes:
            return
        self._discard_expired(now)
        if self._usage() + needed <= self.cache_max_bytes:
            return
        live = set()
        try:
            live = {self._canonical(path) for path in self.store.live_media_paths(now)}
        except Exception:
            pass
        candidates = sorted((path for path in self._dirs[0].iterdir() if path.is_file()
                             and self._canonical(str(path)) not in live),
                            key=lambda path: path.stat().st_mtime)
        for path in candidates:
            if self._usage() + needed <= self.cache_max_bytes:
                return
            (self._dirs[1] / f'{path.name}.json').unlink(missing_ok=True)
            try:
                path.unlink()
            except OSError:
                pass
        if self._usage() + needed > self.cache_max_bytes:
            raise MediaCacheFull

    def _canonical(self, path: str) -> str:
        candidate = Path(path)
        if not candidate.is_absolute():
            candidate = self.directory / candidate
        return str(candidate.resolve())


class ContentWindow:
    """Bounded quiet window over incoming messages, grouped by author.

    The window closes when the newest message is `quiet_seconds` old, when the
    oldest is `max_wait_seconds` old, when `max_messages` accumulate, or on
    force. Flushed groups follow first-arrival order and keep arrival order
    inside each (account, group, user) group.
    """

    def __init__(self, quiet_seconds: float, max_wait_seconds: float, max_messages: int):
        if isinstance(quiet_seconds, bool) or not isinstance(quiet_seconds, (int, float)) or quiet_seconds <= 0:
            raise ValueError('quiet_seconds must be positive')
        if isinstance(max_wait_seconds, bool) or not isinstance(max_wait_seconds, (int, float)) or max_wait_seconds <= 0:
            raise ValueError('max_wait_seconds must be positive')
        if isinstance(max_messages, bool) or not isinstance(max_messages, int) or max_messages <= 0:
            raise ValueError('max_messages must be a positive integer')
        self.quiet_seconds = float(quiet_seconds)
        self.max_wait_seconds = float(max_wait_seconds)
        self.max_messages = int(max_messages)
        self._buffer: list = []
        self._forced = False

    def add(self, message) -> None:
        self._buffer.append(message)
        if len(self._buffer) >= self.max_messages:
            self._forced = True

    def due(self, now: float, force: bool = False) -> list[list[MessageRef]]:
        groups={}
        for message in self._buffer:
            groups.setdefault((message.ref.account_id,message.ref.group_id,message.user_id),[]).append(message)
        result=[]; remaining=[]
        for messages in groups.values():
            if force or len(messages)>=self.max_messages or now-messages[-1].occurred_at>=self.quiet_seconds or now-messages[0].occurred_at>=self.max_wait_seconds:
                result.append([m.ref for m in messages])
            else: remaining.extend(messages)
        self._buffer=remaining; self._forced=False
        return result
