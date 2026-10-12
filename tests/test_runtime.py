import asyncio
from dataclasses import asdict
import time
from pullshit.contracts import AgentResult,ChatEvent,ChatMessage,MessageRef
from pullshit.content.service import ContentService,ContentWindow
from pullshit.feedback.service import FeedbackService
from pullshit.publishing.service import Publisher
from pullshit.runtime.service import Service
from pullshit.runtime.maintenance import Maintenance
from tests.support import Workspace,Gateway

class RuntimeTests(Workspace):
    def service(self):
        service=Service(self.root,'development',clock=lambda:self.current)
        service.store=self.store(); service.gateway=Gateway(); service.settings=self.settings
        service.window=ContentWindow(**self.settings.data['content'])
        service.content=ContentService(self.settings,service.store,clock=lambda:self.current)
        service.publisher=Publisher(self.settings,service.store,service.gateway,clock=lambda:self.current)
        class Agent:
            calls=0
            async def run(self,task):
                self.calls+=1
                return AgentResult('eligible',.95,['test'],[],[m.ref.message_id for m in task.bundle.messages],'approved test')
        service.agent=Agent(); service.feedback=FeedbackService(self.settings,service.store,service.agent,clock=lambda:self.current)
        return service
    def test_receive_bundle_select_publish_and_feedback(self):
        service=self.service(); messages=self.bundle(2).messages
        async def workflow():
            for i,message in enumerate(messages): await service.handle(ChatEvent('message',message.ref,self.current,str(i),message.user_id,message))
            service.schedule([m.ref for m in messages]); worker=asyncio.create_task(service.collect_worker())
            await asyncio.wait_for(service.queue.join(),1); worker.cancel(); await asyncio.gather(worker,return_exceptions=True)
            candidates=service.store.list_candidates(status='collected',now=self.current)
            self.assertEqual(len(candidates),1)
            await service.analyze(candidates[0]); await service.publisher.dispatch_once(); await service.publisher.dispatch_once()
            ref=MessageRef('900001','900201','5001')
            receipt=next(row['message_id'] for row in service.store.deliveries() if row['group_id']=='900201')
            message=ChatMessage(ref,'900301','member',self.current,[{'type':'reply','data':{'id':receipt}},{'type':'text','data':{'text':'/喜欢'}}])
            await service.feedback.apply(ChatEvent('message',ref,self.current,'feedback','900301',message))
        asyncio.run(workflow())
        self.assertEqual(service.agent.calls,2)
        self.assertEqual(len(service.gateway.sent),2)
        self.assertEqual(len(service.store.feedback_since('900001','900201',0,now=self.current)),1)
    def test_admin_pause_is_persistent_and_nested_command_is_content(self):
        service=self.service()
        async def workflow():
            ref=MessageRef('900001','900201','55')
            message=ChatMessage(ref,'900301','admin',self.current,[{'type':'text','data':{'text':'/暂停'}}])
            await service.handle(ChatEvent('message',ref,self.current,'control','900301',message))
            await asyncio.gather(*tuple(service.tasks),return_exceptions=True)
            self.assertTrue(service.store.paused('900001','900201'))
            nested=ChatMessage(MessageRef('900001','900101','56'),'900301','admin',self.current,[{'type':'node','data':{'content':[{'type':'text','data':{'text':'/恢复'}}]}}])
            await service.handle(ChatEvent('message',nested.ref,self.current,'nested','900301',nested))
            self.assertTrue(service.store.paused('900001','900201'))
        asyncio.run(workflow())
    def test_background_failure_wakes_supervisor(self):
        service=self.service()
        async def workflow():
            async def broken(): raise RuntimeError('test failure')
            service.spawn(broken()); await asyncio.wait_for(service.failure.wait(),1)
            self.assertIsInstance(service.background_error,RuntimeError)
        asyncio.run(workflow())
    def test_incomplete_context_does_not_spend_model_budget(self):
        service=self.service(); bundle=self.bundle(); bundle.issues=['transport_error']; service.store.put_candidate(bundle)
        asyncio.run(service.analyze(service.store.get_candidate(bundle.bundle_id,now=self.current)))
        self.assertEqual(service.agent.calls,0); self.assertEqual(service.store.deliveries(),[])
    def test_global_and_group_scope_cli_pause(self):
        store=self.store(); store.set_paused('*','900201',True)
        self.assertTrue(store.paused('900001','900201')); self.assertFalse(store.paused('900001','900202'))
