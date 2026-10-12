import asyncio
from dataclasses import asdict
from pullshit.contracts import ChatEvent,ChatMessage,MessageRef,DAY
from pullshit.feedback.service import FeedbackService
from pullshit.publishing.service import Publisher
from tests.support import Workspace,Gateway

class FeedbackTests(Workspace):
    def test_explicit_attribution_duplicate_and_group_scope(self):
        store=self.store(); bundle=self.bundle(); store.put_candidate(bundle,{'900201':asdict(self.eligible())}); store.set_candidate_status(bundle.bundle_id,'analyzed')
        publisher=Publisher(self.settings,store,Gateway(),clock=lambda:self.current); publisher.enqueue(bundle,'900201',self.eligible()); asyncio.run(publisher.dispatch_once())
        service=FeedbackService(self.settings,store,clock=lambda:self.current)
        def event(group):
            ref=MessageRef('900001',group,'501')
            return ChatEvent('message',ref,self.current,'feedback','900301',ChatMessage(ref,'900301','member',self.current,[{'type':'reply','data':{'id':'1001'}},{'type':'text','data':{'text':'/喜欢'}}]))
        self.assertIsNotNone(asyncio.run(service.apply(event('900201'))))
        asyncio.run(service.apply(event('900201')))
        self.assertEqual(len(store.feedback_since('900001','900201',0,now=self.current)),1)
        self.assertIsNone(asyncio.run(service.apply(event('900202'))))
    def test_learning_independent_evidence_step_and_rollback(self):
        store=self.store(); service=FeedbackService(self.settings,store,clock=lambda:self.current)
        def signal(i,user): return {'signal_id':str(i),'account_id':'900001','group_id':'900201','bundle_id':str(i),'publication_id':str(i),
                                   'user_id':user,'kind':'explicit','direction':'positive','tags':['fun'],'occurred_at':self.current,'expires_at':self.current+7*DAY,'payload':{}}
        for i in range(3): store.put_feedback(signal(i,'900301'))
        self.assertEqual(service.learn('900001','900201')['preferences'],{})
        self.current+=DAY
        for i in range(3,6): store.put_feedback(signal(i,str(900301+i)))
        state=service.learn('900001','900201'); self.assertEqual(state['preferences']['fun'],.05)
        self.assertEqual(service.learn('900001','900201')['version'],state['version'])
        self.assertEqual(service.rollback('900001','900201')['preferences'],{})
    def test_emoji_is_unknown_and_removal_is_idempotent(self):
        store=self.store(); bundle=self.bundle(); store.put_candidate(bundle,{'900201':asdict(self.eligible())}); store.set_candidate_status(bundle.bundle_id,'analyzed')
        publisher=Publisher(self.settings,store,Gateway(),clock=lambda:self.current); publisher.enqueue(bundle,'900201',self.eligible()); asyncio.run(publisher.dispatch_once())
        service=FeedbackService(self.settings,store,clock=lambda:self.current)
        event=ChatEvent('reaction',MessageRef('900001','900201','1001'),self.current,'add','900301',reaction='265',added=True)
        asyncio.run(service.apply(event)); self.assertEqual(service.learn('900001','900201')['preferences'],{})
        event.added=False; asyncio.run(service.apply(event))
        self.assertEqual(store.feedback_since('900001','900201',0,now=self.current),[])
