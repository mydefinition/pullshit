"""A single service coordinates discovery, agent tasks, publication and feedback."""
from __future__ import annotations
import asyncio
from dataclasses import asdict
import fcntl
import logging
from pathlib import Path
import time
from pullshit.config import ConfigError,load_settings
from pullshit.contracts import AgentTask,ChatEvent,ContentBundle,DAY,MessageRef,digest
from pullshit.feedback.service import FeedbackService
from pullshit.publishing.service import Publisher,PublishRejected
from pullshit.runtime.maintenance import Maintenance

class Service:
    def __init__(self,root,profile,*,gateway_factory=None,agent_factory=None,clock=time.time):
        self.root,self.profile,self.clock=Path(root),profile,clock
        self.settings=load_settings(self.root,profile)
        self.gateway_factory=gateway_factory; self.agent_factory=agent_factory
        self.gateway=None; self.store=None; self.agent=None; self.content=None; self.media=None
        self.failure=asyncio.Event(); self.background_error=None; self.control_replies={};
        self.tasks=set(); self.queue=asyncio.Queue(maxsize=200); self.feedback_queue=asyncio.Queue(maxsize=200)
        self.collecting=set(); self.analyzing=set(); self.stopping=False
        self.logger=logging.getLogger('pullshit')

    def effective(self):
        current=load_settings(self.root,self.profile)
        return current.with_account(self.gateway.account_id) if self.gateway else current

    async def tools(self,name,args,task):
        if name=='get_group_preferences':
            return self.store.group_state(self.gateway.account_id,task.target_group_id)['preferences']
        if name=='get_context' and task.bundle is not None:
            group=args.get('group_id'); message=args.get('message_id')
            if group is None and message is None:
                return {'bundle':asdict(task.bundle)}
            permitted=[ref for ref in task.bundle.sources if (group is None or ref.group_id==group) and (message is None or ref.message_id==message)]
            if len(permitted)!=1 or permitted[0].group_id not in self.effective().sources:
                return {'error':'reference_not_authorized'}
            return {'bundle':asdict(await self.content.build(permitted[0]))}
        if name=='submit_analysis': return {'accepted':True}
        return {'error':'tool_not_available'}

    def spawn(self,coro):
        task=asyncio.create_task(coro); self.tasks.add(task)
        def finished(done):
            self.tasks.discard(done)
            if not done.cancelled() and done.exception() is not None:
                self.background_error=done.exception(); self.failure.set()
                self.logger.error('background workflow failed')
        task.add_done_callback(finished)
        return task

    async def control_reply(self,event):
        settings=self.effective()
        if event.user_id not in settings.operators or event.ref.group_id not in settings.targets:
            return
        if settings.data['runtime']['mode']!='auto' or not settings.data['sending']['enabled']:
            return
        key=event.ref.group_id; current=self.clock()
        recent=[stamp for stamp in self.control_replies.get(key,[]) if stamp>current-60]
        if len(recent)>=5: return
        recent.append(current); self.control_replies[key]=recent
        paused=self.store.paused(event.ref.account_id,key)
        queued=len(self.store.deliveries(status='queued',group_id=key,now=current))
        text=f'pullshit：本群发布暂停={paused}；待发分片={queued}。'
        nodes=[{'type':'node','data':{'user_id':self.gateway.account_id,'nickname':'pullshit','content':[{'type':'text','data':{'text':text}}]}}]
        try: await self.gateway.send_forward(key,nodes)
        except Exception: self.logger.warning('control reply unavailable')

    def schedule(self,refs,nominated=False):
        key=tuple(ref.key for ref in refs)
        if key in self.collecting: return
        try: self.queue.put_nowait((refs,nominated)); self.collecting.add(key)
        except asyncio.QueueFull: self.logger.warning('discovery queue full')

    async def collect_worker(self):
        while True:
            refs,nominated=await self.queue.get(); key=tuple(ref.key for ref in refs)
            try:
                bundle=await self.content.build(refs)
                if bundle.expires_at<=self.clock() or not bundle.messages: continue
                text=''.join(str(s.get('data',{}).get('text','')) for m in bundle.messages for s in m.segments if s.get('type')=='text')
                if not nominated and not bundle.media and len(text.strip())<20 and 'http' not in text: continue
                if len(self.store.list_candidates(limit=201,status='collected',now=self.clock()))>=200:
                    self.logger.warning('candidate queue full'); continue
                if self.store.put_candidate(bundle):
                    self.store.set_candidate_status(bundle.bundle_id,'collected')
            except Exception:
                self.logger.warning('content assembly failed')
            finally:
                self.collecting.discard(key); self.queue.task_done()

    async def analyze(self,candidate):
        bundle=candidate['bundle']; self.analyzing.add(bundle.bundle_id)
        try:
            settings=self.effective(); analyses=dict(candidate['analysis'])
            targets={target for source,target in settings.routes if any(ref.group_id==source for ref in bundle.sources)}
            for target in sorted(targets):
                if not all(settings.permits(ref.group_id,target) for ref in bundle.sources): continue
                task=AgentTask(digest([bundle.bundle_id,target,'selection']),'selection',target,
                               min(bundle.expires_at,self.clock()+settings.data['agent']['timeout_seconds']),bundle,
                               self.store.group_state(self.gateway.account_id,target)['preferences'],
                               limits={'model_calls':settings.data['agent']['model_calls'],'tool_calls':settings.data['agent']['tool_calls'],
                                       'output_tokens':settings.data['agent']['max_output_tokens'],'input_characters':settings.data['agent']['max_input_characters']})
                if Publisher.incomplete_reason(bundle):
                    from pullshit.contracts import AgentResult
                    result=AgentResult('needs_review',reason='context_incomplete')
                else:
                    result=await self.agent.run(task)
                current=self.store.get_candidate(bundle.bundle_id,now=self.clock())
                if current is None or current['status']=='cancelled': return
                analyses[target]=asdict(result)
            status='analyzed' if analyses else 'needs_review'
            self.store.set_candidate_status(bundle.bundle_id,status,analysis=analyses)
            for target,value in analyses.items():
                from pullshit.contracts import AgentResult
                try: self.publisher.enqueue(bundle,target,AgentResult.from_dict(value))
                except PublishRejected as error:
                    self.logger.info('publication withheld: candidate=%s target=%s reason=%s',
                                     bundle.bundle_id[:12],target,str(error))
        except asyncio.CancelledError: raise
        except Exception:
            self.logger.warning('selection task failed')
            current=self.store.get_candidate(bundle.bundle_id,now=self.clock())
            if current and current['status']!='cancelled': self.store.set_candidate_status(bundle.bundle_id,'needs_review')
        finally: self.analyzing.discard(bundle.bundle_id)

    async def feedback_worker(self):
        while True:
            event=await self.feedback_queue.get()
            try: await self.feedback.apply(event)
            except Exception: self.logger.warning('feedback interpretation failed')
            finally: self.feedback_queue.task_done()

    async def handle(self,event):
        settings=self.effective()
        if event.ref.account_id!=self.gateway.account_id or event.ref.group_id not in settings.sources|settings.targets: return
        if event.kind=='recall':
            self.store.recall_message(event.ref,now=self.clock())
            for candidate in self.store.list_candidates(limit=1000,now=self.clock()):
                if event.ref in candidate['bundle'].sources: self.store.cancel_candidate(candidate['bundle'].bundle_id,'source_recalled')
            return
        if event.kind=='message' and event.message:
            message=event.message
            message.expires_at=min(message.expires_at,message.occurred_at+settings.data['retention']['message_days']*DAY)
            if not self.store.put_message(message) or message.own: return
            text=''.join(str(s.get('data',{}).get('text','')) for s in message.segments if s.get('type')=='text').strip()
            replies=[s.get('data',{}).get('id') for s in message.segments if s.get('type')=='reply']
            if text in {'/暂停','/恢复','/状态'} and event.user_id in settings.operators:
                if text!='/状态': self.store.set_paused(event.ref.account_id,event.ref.group_id if event.ref.group_id in settings.targets else '*',text=='/暂停')
                self.spawn(self.control_reply(event))
                self.logger.info('group control applied: %s',text); return
            if text=='/搬' and event.ref.group_id in settings.sources and len(replies)==1:
                self.schedule([MessageRef(event.ref.account_id,event.ref.group_id,str(replies[0]))],True); return
            if event.ref.group_id in settings.sources and not text.startswith('/'):
                self.window.add(message)
        if event.ref.group_id in settings.targets:
            try: self.feedback_queue.put_nowait(event)
            except asyncio.QueueFull: self.logger.warning('feedback queue full')

    async def consume(self,gateway,max_events):
        count=0
        async for event in gateway.events():
            if self.effective().data['napcat']!=self.settings.data['napcat']:
                raise ConnectionError('connection_configuration_changed')
            await self.handle(event); count+=1
            if max_events is not None and count>=max_events:
                return self.store.summary()
        if max_events is None: raise ConnectionError('event_stream_ended')
        return self.store.summary()

    async def tick(self):
        while True:
            for refs in self.window.due(self.clock()): self.schedule(refs)
            capacity=self.settings.data['agent']['concurrency']-len(self.analyzing)
            if capacity>0:
                for candidate in self.store.list_candidates(limit=200,status='collected',now=self.clock()):
                    if candidate['bundle'].bundle_id in self.analyzing: continue
                    self.analyzing.add(candidate['bundle'].bundle_id); self.spawn(self.analyze(candidate)); capacity-=1
                    if capacity<=0: break
            await self.publisher.dispatch_once()
            if self.maintenance.due(): self.maintenance.clean()
            for group in self.effective().targets: self.feedback.learn(self.gateway.account_id,group)
            await asyncio.sleep(self.settings.data['runtime']['tick_seconds'])

    async def run(self,max_events=None):
        from pullshit.storage.store import Store
        from pullshit.content.service import ContentService,ContentWindow
        from pullshit.content.media import MediaCache
        from pullshit.ingestion.napcat import NapCatGateway
        from pullshit.agent.executor import HermesExecutor,clean_stale_workers
        self.settings.validate_live()
        directory=self.settings.path('storage','database_path').parent; directory.mkdir(parents=True,exist_ok=True,mode=0o700)
        lock=(directory/'runner.lock').open('a')
        try:
            try: fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
            except BlockingIOError: raise ConfigError('该环境已有运行实例') from None
            self.store=Store(self.settings.path('storage','database_path'),self.settings.data['retention']); self.store.recover_inflight()
            self.media=MediaCache(self.settings,self.store)
            self.agent=(self.agent_factory or HermesExecutor)(self.settings,self.store,tools=self.tools)
            self.window=ContentWindow(**self.settings.data['content'])
            backoff=self.settings.data['napcat']['reconnect_initial_seconds']; count=0
            while True:
                current=load_settings(self.root,self.profile); current.validate_live()
                try:
                    async with (self.gateway_factory or NapCatGateway)(current) as gateway:
                        self.gateway=gateway; self.settings=current.with_account(gateway.account_id)
                        self.content=ContentService(self.settings,self.store,gateway,self.media,clock=self.clock)
                        self.publisher=Publisher(self.settings,self.store,gateway,settings_provider=self.effective,clock=self.clock)
                        self.feedback=FeedbackService(self.settings,self.store,self.agent,clock=self.clock)
                        self.maintenance=Maintenance(self.settings,self.store,self.media,getattr(self.agent,'clean_stale_workers',clean_stale_workers),clock=self.clock)
                        self.maintenance.clean(compact=True)
                        background=[self.spawn(self.collect_worker()) for _ in range(2)]
                        background += [self.spawn(self.feedback_worker()),self.spawn(self.tick())]
                        self.failure.clear(); self.background_error=None
                        receiver=asyncio.create_task(self.consume(gateway,max_events))
                        failure=asyncio.create_task(self.failure.wait())
                        try:
                            done,_=await asyncio.wait([receiver,failure],return_when=asyncio.FIRST_COMPLETED)
                            if failure in done and self.background_error is not None:
                                raise self.background_error
                            return receiver.result()
                        finally:
                            pending=[receiver,failure,*tuple(self.tasks)]
                            for task in pending: task.cancel()
                            await asyncio.gather(*pending,return_exceptions=True)
                except (ConfigError,KeyboardInterrupt,asyncio.CancelledError): raise
                except Exception:
                    self.logger.warning('platform disconnected; reconnecting')
                    await asyncio.sleep(backoff); backoff=min(backoff*2,current.data['napcat']['reconnect_max_seconds'])
        finally:
            for task in tuple(self.tasks): task.cancel()
            if self.tasks: await asyncio.gather(*tuple(self.tasks),return_exceptions=True)
            if self.store: self.store.close()
            lock.close()
