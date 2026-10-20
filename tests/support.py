from dataclasses import asdict
import json
from pathlib import Path
import tempfile
import time
import unittest
from pullshit.config import load_settings
from pullshit.contracts import AgentResult,AgentTask,ChatMessage,ContentBundle,DAY,MessageRef,digest

class Workspace(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory(); self.addCleanup(self.temp.cleanup); self.root=Path(self.temp.name)
        (self.root/'config/local').mkdir(parents=True)
        repo=Path(__file__).resolve().parents[1]
        (self.root/'config/defaults.json').write_text((repo/'config/defaults.json').read_text())
        self.local={'authorization':{'source_group_ids':['900101'],'target_group_ids':['900201','900202'],
                                    'operator_qq_ids':['900301']},
                    'sending':{'enabled':True},'review':{'enabled':False}}
        self.write_config(); self.settings=load_settings(self.root,'development').with_account('900001'); self.current=time.time()
    def write_config(self):
        (self.root/'config/local/development.json').write_text(json.dumps(self.local))
    def bundle(self,count=1):
        messages=[ChatMessage(MessageRef('900001','900101',str(i+1)),'900401','sample',self.current,
                              [{'type':'text','data':{'text':f'这是一个用于验证完整业务流程的有趣测试内容，记录 {i+1}'}}]) for i in range(count)]
        fingerprint=digest([m.segments for m in messages])
        return ContentBundle(digest(['bundle',fingerprint]),fingerprint,messages,self.current,self.current+7*DAY)
    def store(self):
        from pullshit.storage.store import Store
        store=Store(self.settings.path('storage','database_path')); self.addCleanup(store.close); return store
    def eligible(self): return AgentResult('eligible',.95,['test'],[],[],'worth sharing')

class Gateway:
    account_id='900001'
    def __init__(self): self.sent=[]; self.error=None
    async def reply(self,ref,text,*,before_send=None):
        return await self.send_message(ref.group_id,[{'type':'reply','data':{'id':ref.message_id}},
                    {'type':'text','data':{'text':text}}],before_send=before_send)
    async def send_message(self,group_id,nodes,*,before_send=None):
        if before_send is not None: before_send()
        if self.error: raise self.error
        self.sent.append((group_id,nodes)); return str(1000+len(self.sent))
    async def send_forward(self,group_id,nodes,*,before_send=None):
        return await self.send_message(group_id,nodes,before_send=before_send)


def verdict(task, ids=None, **kwargs):
    from pullshit.content import selection
    selection.prepare(task)
    chosen=ids if ids is not None else [unit['id'] for unit in task.selection['units'] if unit['publishable']]
    return AgentResult('eligible',.95,['test'],reason='selected test',
        snapshot_id=task.selection['snapshot_id'],snapshot_revision=task.selection['revision'],
        selections=[{'publish_ids':chosen,'context_status':'complete','completeness_reason':'self contained test',
                     'reason':'worth sharing','new_value_reason':'new reply supplies substantive context'}],**kwargs)
