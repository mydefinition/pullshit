"""Loopback-only authenticated HTTP interface for disclosed simulation cards."""
from __future__ import annotations

import asyncio
from collections import deque
import fcntl
import hmac
from http.server import BaseHTTPRequestHandler, HTTPServer
import json
import os
import re
import time

from pullshit.config import ConfigError, load_settings
from pullshit.contracts import PlatformRejected, digest, platform_id
from pullshit.ingestion.napcat import NapCatGateway
from pullshit.publishing.simulation import normalize_request, send_simulation
from pullshit.storage.simulation_receipts import SimulationReceipts
from pullshit.storage.store import Store

MAX_BODY = 16384
KEY = re.compile(r'[A-Za-z0-9_-]{8,100}')
ROUTE = '/v1/simulation-forwards'


class SimulationAPI:
    def __init__(self, settings_provider, account, token, receipts, *,
                 gateway_factory=NapCatGateway, clock=time.time):
        if not isinstance(token, str) or len(token) < 32:
            raise ConfigError('Simulation API token requires at least 32 characters')
        self.settings_provider, self.account = settings_provider, platform_id(account)
        self.token, self.receipts, self.gateway_factory = token, receipts, gateway_factory
        self.clock, self.recent = clock, deque()

    def authenticated(self, value):
        return hmac.compare_digest(('Bearer ' + self.token).encode(), str(value or '').encode())

    def check_sending(self, group):
        settings = self.settings_provider()
        if group not in settings.targets:
            raise PermissionError('target_not_authorized')
        if not settings.data['sending']['enabled'] or settings.data['runtime']['mode'] != 'auto':
            raise PermissionError('sending_disabled')
        store = Store(settings.path('storage', 'database_path'), settings.data['retention'])
        try:
            if store.paused(self.account, group):
                raise PermissionError('sending_paused')
        finally:
            store.close()
        return settings

    @staticmethod
    def response_code(result, *, cached=False):
        return {'sent': 200 if cached else 201, 'unknown': 202, 'failed': 502}[result['status']]

    def post(self, key, body):
        if not isinstance(key, str) or not KEY.fullmatch(key):
            return 400, {'error': 'invalid_idempotency_key'}
        def pairs(items):
            result = {}
            for name, value in items:
                if name in result: raise ValueError('duplicate_field')
                result[name] = value
            return result
        try:
            raw = json.loads(body, object_pairs_hook=pairs)
            request = normalize_request(raw, self.settings_provider().targets)
        except (ValueError, TypeError, UnicodeError):
            return 400, {'error': 'invalid_request'}
        except PermissionError:
            return 403, {'error': 'target_not_authorized'}
        fingerprint = digest([self.account, request])
        cached = self.receipts.get(key)
        if cached:
            if cached['fingerprint'] != fingerprint:
                return 409, {'error': 'idempotency_conflict'}
            return self.response_code(cached['result'], cached=True), cached['result']
        try:
            settings = self.check_sending(request['group_id'])
        except PermissionError as error:
            return 403, {'error': str(error)}
        self.recent = deque(stamp for stamp in self.recent if stamp > self.clock() - 60)
        if len(self.recent) >= 5:
            return 429, {'error': 'rate_limit'}
        result = {'request_id': key, 'status': 'unknown', 'simulation': True,
                  'group_id': request['group_id'], 'requested_qq': request['qq'],
                  'message_id': None, 'retry_safe': False}
        try:
            self.receipts.start(key, fingerprint, result)
        except ValueError:
            return 503, {'error': 'receipt_capacity'}
        self.recent.append(self.clock())
        def received(value):
            value.update(request_id=key, retry_safe=False)
            self.receipts.finish(key, value)
        try:
            result = asyncio.run(send_simulation(settings, request, self.account, self.gateway_factory,
                lambda: self.check_sending(request['group_id']), received))
            result.update(request_id=key, retry_safe=False)
        except (PermissionError, PlatformRejected) as error:
            result.update(status='failed', error='rules_changed' if isinstance(error, PermissionError) else 'platform_rejected')
        except Exception:
            acknowledged = self.receipts.get(key)['result']
            result = acknowledged if acknowledged['status'] == 'sent' else result
            if result['status'] != 'sent': result['error'] = 'outcome_unknown'
        self.receipts.finish(key, result)
        return self.response_code(result), result


def handler_for(api):
    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *args): pass

        def reply(self, code, value):
            raw = json.dumps(value, ensure_ascii=False).encode()
            self.send_response(code)
            self.send_header('Content-Type', 'application/json; charset=utf-8')
            self.send_header('Content-Length', str(len(raw)))
            self.send_header('Cache-Control', 'no-store')
            self.send_header('Connection', 'close')
            self.end_headers(); self.wfile.write(raw); self.close_connection = True

        def authorize(self):
            if not api.authenticated(self.headers.get('Authorization')):
                self.reply(401, {'error': 'unauthorized'}); return False
            return True

        def do_GET(self):
            if not self.authorize(): return
            if self.path == '/health':
                return self.reply(200, {'service': 'simulation-api', 'status': 'ready'})
            key = self.path.removeprefix(ROUTE + '/')
            if self.path.startswith(ROUTE + '/') and KEY.fullmatch(key):
                record = api.receipts.get(key)
                if record: return self.reply(200, record['result'])
            self.reply(404, {'error': 'not_found'})

        def do_POST(self):
            if not self.authorize(): return
            if self.path != ROUTE: return self.reply(404, {'error': 'not_found'})
            if self.headers.get('Transfer-Encoding') or len(self.headers.get_all('Content-Length', [])) != 1:
                return self.reply(400, {'error': 'invalid_content_length'})
            if self.headers.get('Content-Type', '').split(';')[0].strip().lower() != 'application/json':
                return self.reply(415, {'error': 'json_required'})
            try:
                length = int(self.headers['Content-Length'])
                if length < 1 or length > MAX_BODY:
                    return self.reply(413, {'error': 'body_too_large'})
                body = self.rfile.read(length)
                if len(body) != length: return self.reply(400, {'error': 'incomplete_body'})
                code, result = api.post(self.headers.get('Idempotency-Key'), body)
                self.reply(code, result)
            except Exception:
                self.reply(503, {'error': 'service_unavailable'})
    return Handler


class LocalServer(HTTPServer):
    def get_request(self):
        sock, address = super().get_request()
        sock.settimeout(5)
        return sock, address

    def service_actions(self):
        if time.monotonic() >= getattr(self, '_cleanup_at', 0):
            if getattr(self, 'receipts', None): self.receipts.purge()
            self._cleanup_at = time.monotonic() + 60


def serve(root, profile, account, port, token_env):
    if not 1 <= port <= 65535: raise ConfigError('Invalid API port')
    provider = lambda: load_settings(root, profile)
    directory = provider().path('storage', 'database_path').parent
    directory.mkdir(parents=True, exist_ok=True, mode=0o700)
    with (directory / 'simulation-api.lock').open('a') as lock:
        os.chmod(lock.name, 0o600)
        try: fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError: raise ConfigError('Simulation API already running') from None
        receipts = SimulationReceipts(directory / 'simulation-receipts.json')
        api = SimulationAPI(provider, account, os.environ.get(token_env, ''), receipts)
        with LocalServer(('127.0.0.1', port), handler_for(api)) as server:
            server.receipts = receipts
            server.serve_forever()
