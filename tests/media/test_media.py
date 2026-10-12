import io
import json
import socket
import unittest
from datetime import datetime, timedelta, timezone
from unittest.mock import patch
from urllib.parse import urlsplit

from pullshit.media.cache import MediaError, cache_image, prune_media, read_cached, validate_url
from support import Workspace


class Response:
    def __init__(self, status=200, payload=b"\x89PNG\r\n\x1a\nsynthetic", headers=None):
        self.status = status
        self.stream = io.BytesIO(payload)
        self.headers = headers or {}

    def getheader(self, key):
        return self.headers.get(key)

    def read1(self, size):
        return self.stream.read(size)


class Connection:
    sock = None

    def __init__(self, response):
        self.response = response

    def request(self, *args, **kwargs):
        pass

    def getresponse(self):
        return self.response

    def close(self):
        pass


class MediaTests(Workspace, unittest.TestCase):
    def setUp(self):
        super().setUp()
        self.cfg = self.settings()
        self.directory = self.cfg.path("storage", "media_directory")
        self.limits = self.cfg.data["media"]

    def public(self, url, protocols):
        if urlsplit(url).hostname != "public.example":
            raise MediaError("blocked")
        return urlsplit(url), "8.8.8.8", 443

    def test_private_mixed_dns_and_embedded_credentials_blocked(self):
        for ip in ("127.0.0.1", "10.0.0.1", "169.254.169.254", "::1", "::ffff:127.0.0.1"):
            with patch("socket.getaddrinfo", return_value=[(socket.AF_INET, 0, 0, "", (ip, 80))]):
                with self.assertRaises(MediaError):
                    validate_url("http://example.invalid", ["http", "https"])
        with patch("socket.getaddrinfo", return_value=[(socket.AF_INET, 0, 0, "", ("8.8.8.8", 80)), (socket.AF_INET, 0, 0, "", ("10.0.0.1", 80))]):
            with self.assertRaises(MediaError):
                validate_url("http://example.invalid", ["http", "https"])
        for url in ("file:///etc/passwd", "https://user:secret@public.example/image", "https://public.example/\r\n"):
            with self.assertRaises(MediaError):
                validate_url(url, ["http", "https"])

    def test_cache_survives_restart_and_preserves_checksum(self):
        result = cache_image("https://public.example/image?token=secret", self.directory, self.limits,
                             resolve=self.public, connection_factory=lambda *args: Connection(Response()))
        self.assertEqual(read_cached(self.directory, result["sha256"], 7, 100), b"\x89PNG\r\n\x1a\nsynthetic")
        metadata = self.directory / (result["sha256"] + ".json")
        self.assertNotIn("secret", metadata.read_text())
        (self.directory / result["sha256"]).write_bytes(b"corrupted")
        with self.assertRaises(MediaError):
            read_cached(self.directory, result["sha256"], 7, 100)

    def test_private_redirect_and_redirect_limit(self):
        response = Response(status=302, headers={"Location": "https://127.0.0.1/private"})
        with self.assertRaises(MediaError):
            cache_image("https://public.example/image", self.directory, self.limits,
                        resolve=self.public, connection_factory=lambda *args: Connection(response))
        response = Response(status=302, headers={"Location": "/again"})
        with self.assertRaises(MediaError):
            cache_image("https://public.example/image", self.directory, self.limits,
                        resolve=self.public, connection_factory=lambda *args: Connection(response))

    def test_declared_actual_size_and_non_image_are_rejected(self):
        limits = {**self.limits, "max_download_bytes": 10}
        for response in (Response(headers={"Content-Length": "11"}), Response(), Response(payload=b"html")):
            with self.assertRaises(MediaError):
                cache_image("https://public.example/image", self.directory, limits,
                            resolve=self.public, connection_factory=lambda *args: Connection(response))

    def test_expiration_removes_content_and_metadata(self):
        result = cache_image("https://public.example/image", self.directory, self.limits,
                             resolve=self.public, connection_factory=lambda *args: Connection(Response()))
        path = self.directory / (result["sha256"] + ".json")
        metadata = json.loads(path.read_text())
        metadata["cached_at"] = (datetime.now(timezone.utc) - timedelta(days=8)).isoformat()
        path.write_text(json.dumps(metadata))
        with self.assertRaises(MediaError):
            read_cached(self.directory, result["sha256"], 7, 100)
        self.assertEqual(prune_media(self.directory, 7), 1)
        self.assertFalse(path.exists())
