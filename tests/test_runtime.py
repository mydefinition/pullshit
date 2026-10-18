import asyncio
from dataclasses import asdict
import time
from pullshit.contracts import AgentResult,ChatEvent,ChatMessage,MessageRef
from pullshit.content.service import ContentService,ContentWindow
from pullshit.feedback.service import FeedbackService
from pullshit.publishing.service import Publisher
from pullshit.runtime.service import Service
from pullshit.runtime.maintenance import Maintenance
from tests.support import Workspace,Gateway,verdict

class RuntimeTests(Workspace):
    def test_recall_of_unselected_context_does_not_cancel_independent_output(self):
        service=self.service();bundle=self.bundle(2)
        for message in bundle.messages:service.store.put_message(message)
        service.store.put_candidate(bundle)
        class Agent:
            async def run(inner,task):
                await service.handle(ChatEvent('recall',bundle.messages[0].ref,self.current,'recall-context'))
                return verdict(task,['U002'])
        service.agent=Agent()
        asyncio.run(service.analyze(service.store.get_candidate(bundle.bundle_id,now=self.current)))
        self.assertEqual(len(service.store.deliveries()),2)
        for row in service.store.deliveries():
            selected=service.store.get_candidate(row['bundle_id'],now=self.current)['bundle']
            self.assertEqual([m.ref.message_id for m in selected.messages],['2'])

    def test_natural_feedback_is_persisted_acknowledged_and_used_by_selection(self):
        service=self.service();original=self.bundle().messages[0];service.store.put_message(original)
        ref=MessageRef('900001','900101','501')
        event=ChatEvent('message',ref,self.current,'natural','900301',
            ChatMessage(ref,'900301','member',self.current,[{'type':'reply','data':{'id':original.ref.message_id}},
                       {'type':'text','data':{'text':'我喜欢这个'}}]))
        tasks=[]
        class Agent:
            async def run(inner,task):
                tasks.append(task)
                return AgentResult('eligible',.99,['test'],reason='明确反馈',direction='positive') if task.kind=='feedback' else verdict(task)
        service.agent=Agent();service.feedback.agent=service.agent
        async def workflow():
            await service.handle(event)
            self.assertEqual(len(service.feedback.pending_events('900001')),1)
            worker=asyncio.create_task(service.feedback_worker())
            await asyncio.wait_for(service.feedback_queue.join(),2)
            worker.cancel();await asyncio.gather(worker,return_exceptions=True)
            self.assertEqual(len(service.gateway.sent),1)
            self.assertEqual(service.gateway.sent[0][1][0],{'type':'reply','data':{'id':'501'}})
            self.assertIn('喜欢',service.gateway.sent[0][1][1]['data']['text'])
            self.assertFalse(service.window.due(self.current,force=True))
            self.assertEqual(service.store.deliveries(),[])
            bundle=self.bundle();service.store.put_candidate(bundle)
            await service.analyze(service.store.get_candidate(bundle.bundle_id,now=self.current))
        asyncio.run(workflow())
        self.assertEqual(tasks[0].kind,'feedback')
        self.assertEqual(tasks[-1].feedback['recent_examples']['900101'][0]['direction'],'positive')

    def test_broadcast_selects_once_and_excludes_all_source_groups(self):
        self.local['authorization']['target_group_ids']=['900101','900201','900202','900203']
        self.write_config()
        from pullshit.config import load_settings
        self.settings=load_settings(self.root,'development').with_account('900001')
        service=self.service(); tasks=[]
        class Agent:
            async def run(inner,task):
                tasks.append(task)
                return verdict(task)
        service.agent=Agent()
        bundle=self.bundle(); service.store.put_candidate(bundle)
        async def workflow():
            await service.analyze(service.store.get_candidate(bundle.bundle_id,now=self.current))
            for _ in range(4): await service.publisher.dispatch_once()
        asyncio.run(workflow())
        self.assertEqual(len(tasks),1)
        self.assertEqual(tasks[0].audience,{'900201','900202','900203'})
        self.assertEqual({group for group,_ in service.gateway.sent},tasks[0].audience)
        self.assertTrue(all(segment['type']=='text' for _,segments in service.gateway.sent for segment in segments))

    def test_no_other_target_skips_model_and_publication(self):
        self.local['authorization']['target_group_ids']=['900101']; self.write_config()
        service=self.service(); bundle=self.bundle(); service.store.put_candidate(bundle)
        asyncio.run(service.analyze(service.store.get_candidate(bundle.bundle_id,now=self.current)))
        self.assertEqual(service.agent.calls,0)
        self.assertEqual(service.store.deliveries(),[])

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
                return verdict(task) if task.kind=='selection' else AgentResult('eligible',.95,['test'],reason='feedback',direction='positive')
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
        self.assertEqual(service.agent.calls,1)
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
    def test_unrelated_context_issue_does_not_block_independent_selection(self):
        service=self.service(); bundle=self.bundle(); bundle.issues=['transport_error']; service.store.put_candidate(bundle)
        asyncio.run(service.analyze(service.store.get_candidate(bundle.bundle_id,now=self.current)))
        self.assertEqual(service.agent.calls,1); self.assertEqual(len(service.store.deliveries()),2)
    def test_global_and_group_scope_cli_pause(self):
        store=self.store(); store.set_paused('*','900201',True)
        self.assertTrue(store.paused('900001','900201')); self.assertFalse(store.paused('900001','900202'))
