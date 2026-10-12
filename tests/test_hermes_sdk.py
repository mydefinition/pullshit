import asyncio
from dataclasses import asdict
import json
import os
from pathlib import Path
import threading
import time
import unittest
from http.server import BaseHTTPRequestHandler,ThreadingHTTPServer
from unittest.mock import patch
from pullshit.agent.executor import HermesExecutor
from pullshit.contracts import AgentTask
from tests.support import Workspace

SDK_PYTHON=Path(os.environ.get('PULLSHIT_HERMES_PYTHON',str(Path(__file__).resolve().parents[1]/'local/hermes-runtime/.venv/bin/python')))

@unittest.skipUnless(SDK_PYTHON.is_file(),'Hermes worker interpreter is required for SDK integration')
class HermesSdkTests(Workspace):
    def configure(self,deny=False,image=False):
        self.requests=[]; self.user_agents=[]; self.image=image; self.image_ref=None
        owner=self
        class Handler(BaseHTTPRequestHandler):
            def log_message(self,*args): pass
            def do_POST(self):
                body=json.loads(self.rfile.read(int(self.headers['Content-Length'])))
                if self.path!='/v1/chat/completions':
                    raw=b'{"error":"not_found"}'; self.send_response(404); self.send_header('Content-Length',str(len(raw))); self.end_headers(); self.wfile.write(raw); return
                owner.requests.append(body)
                owner.user_agents.append(self.headers.get('User-Agent'))
                if body.get('model')=='mock-vision':
                    raw=json.dumps({'choices':[{'message':{'content':'verified image description'}}],'usage':{'prompt_tokens':20,'completion_tokens':15}}).encode(); self.send_response(200); self.send_header('Content-Type','application/json'); self.send_header('Content-Length',str(len(raw))); self.end_headers(); self.wfile.write(raw); return
                main_requests=[request for request in owner.requests if request['model']=='mock-model']
                if len(main_requests)==1:
                    delta={'role':'assistant','content':None,'tool_calls':[{'index':0,'id':'test-call','type':'function','function':{'name':'get_group_preferences','arguments':'{"group_id":"900201"}'}}]}; finish='tool_calls'
                    if owner.image:
                        delta['tool_calls'][0]['function']={'name':'inspect_image','arguments':json.dumps({'reference_id':owner.image_ref.reference_id})}
                    else:
                        delta['tool_calls'].extend([
                            {'index':1,'id':'context-call','type':'function','function':{'name':'get_context','arguments':'{}'}},
                            {'index':2,'id':'analysis-call','type':'function','function':{'name':'submit_analysis','arguments':json.dumps({'analysis':{'decision':'eligible','score':.95,'tags':['test'],'evidence_ids':['1'],'reason':'mock provider verified'}})}}
                        ])
                else:
                    delta={'role':'assistant','content':json.dumps({'decision':'eligible','score':.95,'tags':['test'],'risks':[],'evidence_ids':['1'],'reason':'mock provider verified'})}; finish='stop'
                chunk={'id':'mock','object':'chat.completion.chunk','created':int(time.time()),'model':'mock-model','choices':[{'index':0,'delta':delta,'finish_reason':finish}]}
                usage={**chunk,'choices':[],'usage':{'prompt_tokens':40,'completion_tokens':60,'total_tokens':100}}
                raw=('data: '+json.dumps(chunk)+'\n\ndata: '+json.dumps(usage)+'\n\ndata: [DONE]\n\n').encode()
                self.send_response(200); self.send_header('Content-Type','text/event-stream'); self.send_header('Content-Length',str(len(raw))); self.end_headers(); self.wfile.write(raw)
        server=ThreadingHTTPServer(('127.0.0.1',0),Handler); threading.Thread(target=server.serve_forever,daemon=True).start()
        self.addCleanup(server.server_close); self.addCleanup(server.shutdown)
        self.settings.data['agent'].update(python=str(SDK_PYTHON),provider='custom',model='mock-model',base_url=f'http://127.0.0.1:{server.server_port}/v1',daily_budget_usd=0 if deny else 1.0,task_budget_usd=.2,input_usd_per_million=1.0,output_usd_per_million=2.0,timeout_seconds=20)
        self.current=time.time(); bundle=self.bundle(); self.store_instance=self.store()
        if image:
            from pullshit.content.media import MediaCache
            from tests.test_media import PNG
            self.image_ref=MediaCache(self.settings,self.store_instance).ingest(PNG,bundle.sources[0],time.time()+60)
            bundle.media=[self.image_ref]
            bundle.messages[0].segments.append({'type':'image','data':{'file':self.image_ref.path}})
            self.settings.data['agent'].update(vision_model='mock-vision',vision_base_url=f'http://127.0.0.1:{server.server_port}/v1',vision_input_usd_per_million=3.0,vision_output_usd_per_million=4.0)
        task=AgentTask('sdk-integration','selection','900201',time.time()+20,bundle)
        return task
    def test_real_sdk_tools_streaming_usage_and_cleanup(self):
        task=self.configure()
        with patch.dict(os.environ,{'PULLSHIT_AGENT_API_KEY':'test-only-token'}):
            result=asyncio.run(HermesExecutor(self.settings,self.store_instance).run(task))
        self.assertEqual(result.decision,'eligible',result.reason)
        self.assertEqual(len(self.requests),2)
        self.assertEqual(self.user_agents,['pullshit/0.2','pullshit/0.2'])
        names={tool['function']['name'] for tool in self.requests[0]['tools']}
        self.assertEqual(names,{'get_context','inspect_image','get_group_preferences','submit_analysis'})
        tool_messages=[v for v in self.requests[1]['messages'] if v['role']=='tool']
        results={message['tool_call_id']:json.loads(message['content']) for message in tool_messages}
        self.assertEqual(results['test-call']['preferences'],{})
        self.assertEqual(results['context-call']['bundle']['messages'][0]['ref']['message_id'],'1')
        self.assertTrue(results['analysis-call']['accepted'])
        self.assertTrue(all('error' not in value for value in results.values()))
        day=time.strftime('%Y-%m-%d',time.gmtime()); budget=self.store_instance.budget(day,'global','model_usd')
        self.assertAlmostEqual(budget['reserved'],0,places=10); self.assertAlmostEqual(budget['used'],.00032)
        self.assertAlmostEqual(result.usage['cost_usd'],.00032)
        self.assertFalse(list(self.settings.path('storage','worker_directory').glob('task-*')))
    def test_budget_denial_prevents_real_provider_request(self):
        task=self.configure(deny=True)
        with patch.dict(os.environ,{'PULLSHIT_AGENT_API_KEY':'test-only-token'}):
            result=asyncio.run(HermesExecutor(self.settings,self.store_instance).run(task))
        self.assertEqual(result.decision,'needs_review'); self.assertEqual(self.requests,[])

    def test_real_vision_tool_is_scoped_metered_and_inspection_verified(self):
        task=self.configure(image=True)
        with patch.dict(os.environ,{'PULLSHIT_AGENT_API_KEY':'test-only-token','PULLSHIT_VISION_API_KEY':'test-vision-token'}):
            result=asyncio.run(HermesExecutor(self.settings,self.store_instance).run(task))
        self.assertEqual(result.decision,'eligible',result.reason)
        self.assertEqual(result.inspected_images,[self.image_ref.reference_id])
        self.assertEqual([v['model'] for v in self.requests],['mock-model','mock-vision','mock-model'])
        messages=[v for v in self.requests[-1]['messages'] if v['role']=='tool']
        inspected=json.loads(messages[0]['content'])
        self.assertTrue(inspected['ok']); self.assertEqual(inspected['reference_id'],self.image_ref.reference_id)
        day=time.strftime('%Y-%m-%d',time.gmtime()); budget=self.store_instance.budget(day,'global','model_usd')
        self.assertAlmostEqual(budget['reserved'],0,places=10); self.assertAlmostEqual(budget['used'],.00044)
    def test_oversized_context_stops_before_provider_and_cleans_worker(self):
        task=self.configure(); task.limits['input_characters']=50
        with patch.dict(os.environ,{'PULLSHIT_AGENT_API_KEY':'test-only-token'}):
            result=asyncio.run(HermesExecutor(self.settings,self.store_instance).run(task))
        self.assertEqual(result.decision,'needs_review'); self.assertIn('input_character_limit',result.reason)
        self.assertFalse(self.requests)
        self.assertFalse(list(self.settings.path('storage','worker_directory').glob('task-*')))
