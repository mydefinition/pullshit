"""Bounded operator request receipts; request bodies are represented by hashes."""
from __future__ import annotations

import json
import os
from pathlib import Path
import tempfile
import time


class SimulationReceipts:
    lifetime = 7 * 86400
    capacity = 500

    def __init__(self, path, *, clock=time.time):
        self.path, self.clock = Path(path), clock
        self.path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        if self.path.is_symlink():
            raise ValueError('receipt_path_symlink')
        self.records = json.loads(self.path.read_text()) if self.path.exists() else {}
        if not isinstance(self.records, dict):
            raise ValueError('invalid_receipt_file')
        self.purge()

    def _prune(self):
        previous = len(self.records)
        self.records = {key: item for key, item in self.records.items()
                        if item['created_at'] + self.lifetime > self.clock()}
        return len(self.records) != previous

    def purge(self):
        if self._prune(): self._write()

    def get(self, key):
        self.purge()
        return self.records.get(key)

    def start(self, key, fingerprint, result):
        self.purge()
        if len(self.records) >= self.capacity:
            raise ValueError('receipt_capacity')
        self.records[key] = {'created_at': self.clock(), 'fingerprint': fingerprint, 'result': result}
        self._write()

    def finish(self, key, result):
        self.records[key]['result'] = result
        self._write()

    def _write(self):
        fd, raw = tempfile.mkstemp(prefix='.simulation-receipts-', dir=self.path.parent)
        try:
            with os.fdopen(fd, 'w') as file:
                json.dump(self.records, file, ensure_ascii=False, separators=(',', ':'))
                file.write('\n'); file.flush(); os.fsync(file.fileno())
            os.replace(raw, self.path)
            directory = os.open(self.path.parent, os.O_RDONLY | os.O_DIRECTORY)
            try: os.fsync(directory)
            finally: os.close(directory)
        finally:
            if Path(raw).exists(): Path(raw).unlink()
