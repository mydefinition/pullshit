import asyncio
import base64
from pathlib import Path
import threading
import time
import sys
from http.server import BaseHTTPRequestHandler,ThreadingHTTPServer
from unittest.mock import patch
from pullshit.content.media import MediaCache,MediaAddressRejected,MediaSizeLimit,MediaTimeout,_download,download
from tests.support import Workspace

PNG=base64.b64decode('iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/x8AAwMCAO+jBz8AAAAASUVORK5CYII=')

class MediaTests(Workspace):
    def test_cache_read_hash_expiry_and_real_file_cleanup(self):
        store=self.store(); cache=MediaCache(self.settings,store); source=self.bundle().sources[0]
        expires=time.time()+60; image=cache.ingest(PNG,source,expires)
        self.assertEqual(Path(image.path).read_bytes(),PNG)
        same=cache.ingest(PNG,source,expires+1000); self.assertEqual(same.expires_at,expires)
        report=cache.cleanup(now=expires+1); self.assertGreaterEqual(report['removed_bytes'],len(PNG)); self.assertFalse(Path(image.path).exists())
    def test_private_address_rejection_uses_real_subprocess(self):
        with self.assertRaises(MediaAddressRejected): asyncio.run(download('http://127.0.0.1/private',1024,3,1))
    def test_real_child_timeout_is_bounded(self):
        original=asyncio.create_subprocess_exec
        children=[]
        async def spawn(*args,**kwargs):
            process=await original(sys.executable,'-c','import time; time.sleep(60)',**kwargs)
            children.append(process); return process
        started=time.monotonic()
        with patch('pullshit.content.media.asyncio.create_subprocess_exec',spawn):
            with self.assertRaises(MediaTimeout):
                asyncio.run(download('https://example.invalid/slow',1024,3,.05,wall_timeout=.05))
        self.assertLess(time.monotonic()-started,2)
        self.assertIsNotNone(children[0].returncode)
    def test_http_chunking_and_size_limit(self):
        class Handler(BaseHTTPRequestHandler):
            def log_message(self,*args): pass
            def do_GET(self):
                self.send_response(200); self.send_header('Transfer-Encoding','chunked'); self.end_headers()
                self.wfile.write(('%x\r\n'%len(PNG)).encode()+PNG+b'\r\n0\r\n\r\n')
        server=ThreadingHTTPServer(('127.0.0.1',0),Handler); thread=threading.Thread(target=server.serve_forever,daemon=True); thread.start()
        self.addCleanup(server.server_close); self.addCleanup(server.shutdown)
        url=f'http://127.0.0.1:{server.server_port}/image'
        payload,size=_download(url,1024,3,2,allow_private=True)
        self.assertEqual(payload,PNG); self.assertEqual(size,len(PNG))
        with self.assertRaises(MediaSizeLimit): _download(url,8,3,2,allow_private=True)
    def test_cancelling_download_reaps_real_child(self):
        original=asyncio.create_subprocess_exec; children=[]
        async def spawn(*args,**kwargs):
            process=await original(sys.executable,'-c','import time; time.sleep(60)',**kwargs)
            children.append(process); return process
        async def cancel():
            task=asyncio.create_task(download('https://example.invalid/slow',1024,3,60))
            while not children: await asyncio.sleep(.01)
            task.cancel()
            with self.assertRaises(asyncio.CancelledError): await task
        with patch('pullshit.content.media.asyncio.create_subprocess_exec',spawn): asyncio.run(cancel())
        self.assertIsNotNone(children[0].returncode)
    def test_cancelled_download_conservatively_keeps_budget(self):
        store=self.store(); cache=MediaCache(self.settings,store)
        async def blocked(*args): raise asyncio.CancelledError()
        with patch('pullshit.content.media.download',blocked):
            with self.assertRaises(asyncio.CancelledError): asyncio.run(cache.cache('https://example.invalid/image',self.bundle().sources[0],time.time()+60))
        day=time.strftime('%Y-%m-%d',time.gmtime()); budget=store.budget(day,'global','download_bytes')
        self.assertEqual(budget['reserved'],0); self.assertEqual(budget['used'],cache.max_bytes+1)
