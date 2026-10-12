"""Run a deterministic offline workflow with explicitly supplied agent verdicts."""
from __future__ import annotations
from dataclasses import asdict
import json
from pathlib import Path
import time
from pullshit.config import Settings,read_json
from pullshit.contracts import AgentResult,AgentTask,DAY,MessageRef,digest
from pullshit.feedback.service import FeedbackService
from pullshit.publishing.service import Publisher,PublishRejected

class ReplayGateway:
    def __init__(self,account_id): self.account_id=account_id; self.sent=[]
    async def send_forward(self,group_id,nodes,*,before_send=None):
        if before_send is not None: before_send()
        self.sent.append({'group_id':group_id,'nodes':len(nodes)})
        return str(1000+len(self.sent))
    async def fetch_message(self,ref): return None
    async def fetch_forward(self,ref,forward_id): return []

class ReplayAgent:
    def __init__(self,decisions): self.decisions=decisions
    async def run(self,task):
        result=self.decisions.get(task.bundle.fingerprint,self.decisions.get(task.bundle.bundle_id,self.decisions.get('*')))
        if result is None: return AgentResult('needs_review',reason='no_simulated_verdict')
        return AgentResult.from_dict(result)
    async def cancel(self,task_id): pass

async def replay(root,fixture,database,account,sources,targets,decisions=None):
    from pullshit.storage.store import Store
    from pullshit.ingestion.events import normalize
    from pullshit.content.service import ContentService,ContentWindow
    root=Path(root).resolve(); database=Path(database)
    if not database.is_absolute(): database=root/database
    safe=(root/'data/replay').resolve()
    if safe != root/'data/replay' or not database.resolve().is_relative_to(safe): raise ValueError('回放数据库必须位于 data/replay')
    data=read_json(root/'config/defaults.json')
    data['authorization']={'source_group_ids':sources,'target_group_ids':targets,'operator_qq_ids':[],'routes':[{'source_group_id':s,'target_group_id':t} for s in sources for t in targets]}
    data['sending']['enabled']=True; data['runtime']['mode']='auto'
    data['storage']={'database_path':str(database),'media_directory':str(database.parent/'media'),'worker_directory':str(database.parent/'workers')}
    settings=Settings(root,'replay',data,account); store=Store(database)
    gateway=ReplayGateway(account); agent=ReplayAgent(read_json(decisions) if decisions else {})
    current=[time.time()]; clock=lambda:current[0]
    publisher=Publisher(settings,store,gateway,clock=clock); feedback=FeedbackService(settings,store,clock=clock)
    content=ContentService(settings,store,gateway=None,media_cache=None,clock=clock)
    window=ContentWindow(**data['content']); counts={'accepted':0,'duplicates':0,'ignored':0}
    async def collect(refs):
        bundle=await content.build(refs)
        if not bundle.messages: return
        store.put_candidate(bundle)
        analyses={}
        for target in targets:
            result=await agent.run(AgentTask(digest([bundle.bundle_id,target]),'selection',target,current[0]+90,bundle))
            analyses[target]=asdict(result)
        store.set_candidate_status(bundle.bundle_id,'analyzed',analysis=analyses)
        for target in targets:
            try: publisher.enqueue(bundle,target,AgentResult.from_dict(analyses[target]))
            except PublishRejected: pass
    baseline=None; anchor=current[0]
    try:
        store.recover_inflight()
        with Path(fixture).open() as handle:
            for line in handle:
                if not line.strip(): continue
                raw=json.loads(line)
                if baseline is None: baseline=raw.get('time',anchor)
                raw['time']=int(anchor+max(0,raw.get('time',baseline)-baseline))
                current[0]=float(raw['time'])
                event=normalize(raw,account,set(sources),set(targets),data['retention']['message_days'])
                if event is None: counts['ignored']+=1; continue
                if event.kind=='message' and event.message:
                    if not store.put_message(event.message): counts['duplicates']+=1; continue
                    counts['accepted']+=1
                    text=''.join(str(s.get('data',{}).get('text','')) for s in event.message.segments if s.get('type')=='text').strip()
                    refs=[s['data'].get('id') for s in event.message.segments if s.get('type')=='reply']
                    if event.ref.group_id in sources and not event.message.own:
                        if text=='/搬' and len(refs)==1: await collect([MessageRef(account,event.ref.group_id,str(refs[0]))])
                        elif not text.startswith('/'): window.add(event.message)
                elif event.kind=='recall':
                    store.recall_message(event.ref,now=current[0]); counts['accepted']+=1
                    for candidate in store.list_candidates(now=current[0]):
                        if event.ref in candidate['bundle'].sources: store.cancel_candidate(candidate['bundle'].bundle_id,'source_recalled')
                for refs in window.due(current[0]): await collect(refs)
                await publisher.dispatch_once()
                if event.ref.group_id in targets: await feedback.apply(event)
        current[0]+=data['content']['max_wait_seconds']+1
        for refs in window.due(current[0],force=True): await collect(refs)
        for _ in range(data['limits']['daily_batches']):
            await publisher.dispatch_once(); current[0]+=data['limits']['min_interval_seconds']
        for target in targets: feedback.learn(account,target)
        return {'simulation':True,**counts,'publications':gateway.sent,'state':store.summary()}
    finally: store.close()
