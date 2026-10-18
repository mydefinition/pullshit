import asyncio
from dataclasses import asdict
from pullshit.contracts import AgentResult,ChatEvent,ChatMessage,MessageRef,DAY
from pullshit.feedback.service import FeedbackService
from pullshit.publishing.service import Publisher
from tests.support import Workspace,Gateway,verdict

class FeedbackTests(Workspace):
    def publication_bundle(self):
        from pullshit.contracts import AgentTask
        from pullshit.content.selection import publications
        task=AgentTask('feedback-published','selection','900201',self.current+90,self.bundle())
        return publications(task,verdict(task))[0][0]

    def natural_feedback(self,store,text='我喜欢这个',group='900101',message_id='501'):
        original=ChatMessage(MessageRef('900001',group,'41'),'900401','member',self.current,
                             [{'type':'text','data':{'text':'一段独立的程序员笑话，作为被引用的群内原始素材。'}}])
        store.put_message(original)
        ref=MessageRef('900001',group,message_id)
        return ChatEvent('message',ref,self.current,'natural-'+message_id,'900301',
            ChatMessage(ref,'900301','member',self.current,[{'type':'reply','data':{'id':'41'}},
                        {'type':'at','data':{'qq':'900001'}},
                        {'type':'text','data':{'text':text}}]))

    def test_feedback_requires_real_direct_bot_mention_before_capture_or_model(self):
        store=self.store()
        class Agent:
            calls=0
            async def run(inner,task):
                inner.calls+=1
                return AgentResult('eligible',.99,reason='positive',direction='positive')
        agent=Agent();service=FeedbackService(self.settings,store,agent,clock=lambda:self.current)
        variants=[[], [{'type':'at','data':{'qq':'900401'}}],
                  [{'type':'at','data':{'qq':'all'}}],
                  [{'type':'text','data':{'text':'@900001'}}],
                  [{'type':'node','data':{'content':[{'type':'at','data':{'qq':'900001'}}]}}]]
        for index,extra in enumerate(variants):
            with self.subTest(extra=extra):
                event=self.natural_feedback(store,'试试这个',message_id=str(600+index))
                event.message.segments=[segment for segment in event.message.segments if segment['type']!='at']+extra
                self.assertIsNone(asyncio.run(service.apply(event)))
        command=self.natural_feedback(store,'/喜欢',message_id='699')
        command.message.segments=[s for s in command.message.segments if s['type']!='at']
        self.assertIsNone(service.capture(command))
        self.assertEqual(agent.calls,0)
        self.assertEqual(store.feedback_since('900001','900101',0,now=self.current),[])

    def test_numeric_bot_mention_is_accepted(self):
        store=self.store();event=self.natural_feedback(store,'/喜欢')
        event.message.segments[1]['data']['qq']=900001
        service=FeedbackService(self.settings,store,clock=lambda:self.current)
        signal=service.capture(event)
        self.assertTrue(signal['payload']['addressed_to_bot'])

    def test_pending_recovery_retains_verified_mention_without_cached_message(self):
        store=self.store();event=self.natural_feedback(store)
        service=FeedbackService(self.settings,store,clock=lambda:self.current)
        signal=service.capture(event)
        self.assertIsNone(store.get_message(event.ref,now=self.current))
        pending=service.pending_events('900001')
        self.assertEqual(len(pending),1)
        self.assertTrue(service.addressed_to_bot(pending[0].message))
        self.assertEqual(service.capture(pending[0])['signal_id'],signal['signal_id'])

    def test_legacy_pending_without_bot_mention_is_closed_without_model(self):
        store=self.store();event=self.natural_feedback(store)
        service=FeedbackService(self.settings,store,clock=lambda:self.current)
        signal=service.capture(event)
        payload={k:v for k,v in signal['payload'].items() if k!='addressed_to_bot'}
        service.store.complete_feedback(signal['signal_id'],payload=payload,now=self.current)
        self.assertEqual(service.pending_events('900001'),[])
        result=store.get_feedback(signal['signal_id'],now=self.current)
        self.assertEqual(result['direction'],'unknown')
        self.assertEqual(result['payload']['processing'],'done')
        self.assertEqual(result['payload']['reason'],'feedback_requires_bot_mention')
        self.assertEqual(service.pending_events('900001'),[])

    def test_legacy_pending_checks_cached_top_level_mention(self):
        store=self.store();event=self.natural_feedback(store);store.put_message(event.message)
        service=FeedbackService(self.settings,store,clock=lambda:self.current)
        signal=service.capture(event)
        payload={k:v for k,v in signal['payload'].items() if k!='addressed_to_bot'}
        service.store.complete_feedback(signal['signal_id'],payload=payload,now=self.current)
        pending=service.pending_events('900001')
        self.assertEqual(len(pending),1)
        self.assertTrue(service.addressed_to_bot(pending[0].message))
        self.assertTrue(store.get_feedback(signal['signal_id'],now=self.current)['payload']['addressed_to_bot'])

    def test_natural_feedback_on_unpublished_source_message(self):
        store=self.store(); event=self.natural_feedback(store); tasks=[]
        class Agent:
            async def run(inner,task):
                tasks.append(task)
                return AgentResult('eligible',.99,['程序员笑话'],reason='明确表达喜欢引用的笑话',direction='positive')
        service=FeedbackService(self.settings,store,Agent(),clock=lambda:self.current)
        signal=asyncio.run(service.apply(event))
        self.assertEqual(signal['direction'],'positive')
        self.assertEqual(signal['tags'],['程序员笑话'])
        self.assertEqual(tasks[0].kind,'feedback')
        self.assertEqual(tasks[0].feedback['text'],'我喜欢这个')
        self.assertEqual(tasks[0].target_group_id,'900101')
        self.assertEqual(store.deliveries(),[])
        self.assertEqual(store.get_candidate(signal['bundle_id'],now=self.current)['status'],'bundled')
        examples=service.examples('900001','900101')
        self.assertEqual(examples[0]['direction'],'positive')
        self.assertIn('程序员笑话',examples[0]['quoted_summary'])
        self.assertIsNone(asyncio.run(service.apply(event)))
        self.assertEqual(len(tasks),1)

    def test_cancelled_interpretation_is_durable_and_can_resume(self):
        store=self.store(); event=self.natural_feedback(store)
        class CancelledAgent:
            async def run(inner,task): raise asyncio.CancelledError()
        service=FeedbackService(self.settings,store,CancelledAgent(),clock=lambda:self.current)
        with self.assertRaises(asyncio.CancelledError): asyncio.run(service.apply(event))
        pending=service.pending_events('900001')
        self.assertEqual(len(pending),1)
        self.assertEqual(pending[0].ref,event.ref)
        class ResumedAgent:
            async def run(inner,task):
                return AgentResult('eligible',.99,['笑话'],reason='明确正面态度',direction='positive')
        resumed=FeedbackService(self.settings,store,ResumedAgent(),clock=lambda:self.current)
        result=asyncio.run(resumed.apply(pending[0]))
        self.assertEqual(result['direction'],'positive')
        self.assertEqual(resumed.pending_events('900001'),[])
        self.assertEqual(len(store.feedback_since('900001','900101',0,now=self.current)),1)

    def test_ordinary_quoted_chat_is_not_a_preference(self):
        store=self.store();event=self.natural_feedback(store,'你明天几点到？')
        class Agent:
            async def run(inner,task): return AgentResult('rejected',.9,reason='普通聊天',direction='unknown')
        service=FeedbackService(self.settings,store,Agent(),clock=lambda:self.current)
        result=asyncio.run(service.apply(event))
        self.assertEqual(result['direction'],'unknown')
        self.assertTrue(result['payload']['ordinary_chat'])
        self.assertEqual(service.examples('900001','900101'),[])

    def test_negative_natural_feedback_and_external_group_are_distinguished(self):
        store=self.store();event=self.natural_feedback(store,'这类图以后少发点')
        class Agent:
            calls=0
            async def run(inner,task):
                inner.calls+=1
                return AgentResult('eligible',.95,['图片'],reason='要求减少同类内容',direction='negative')
        agent=Agent();service=FeedbackService(self.settings,store,agent,clock=lambda:self.current)
        result=asyncio.run(service.apply(event))
        self.assertEqual(result['direction'],'negative')
        external=self.natural_feedback(store,group='999999',message_id='502')
        self.assertIsNone(asyncio.run(service.apply(external)))
        self.assertEqual(agent.calls,1)

    def test_pending_feedback_survives_database_reopen(self):
        from pullshit.storage.store import Store
        store=self.store();event=self.natural_feedback(store)
        service=FeedbackService(self.settings,store,clock=lambda:self.current)
        signal=service.capture(event)
        reopened=Store(store.path);self.addCleanup(reopened.close)
        recovered=reopened.pending_feedback('900001',self.settings.sources|self.settings.targets,now=self.current)
        self.assertEqual([row['signal_id'] for row in recovered],[signal['signal_id']])

    def test_explicit_attribution_duplicate_and_group_scope(self):
        store=self.store(); bundle=self.publication_bundle(); store.put_candidate(bundle,{'900201':asdict(self.eligible())}); store.set_candidate_status(bundle.bundle_id,'analyzed')
        publisher=Publisher(self.settings,store,Gateway(),clock=lambda:self.current); publisher.enqueue(bundle,'900201',self.eligible()); asyncio.run(publisher.dispatch_once())
        service=FeedbackService(self.settings,store,clock=lambda:self.current)
        def event(group):
            ref=MessageRef('900001',group,'501')
            return ChatEvent('message',ref,self.current,'feedback','900301',ChatMessage(ref,'900301','member',self.current,[{'type':'reply','data':{'id':'1001'}},{'type':'at','data':{'qq':'900001'}},{'type':'text','data':{'text':'/喜欢'}}]))
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
        store=self.store(); bundle=self.publication_bundle(); store.put_candidate(bundle,{'900201':asdict(self.eligible())}); store.set_candidate_status(bundle.bundle_id,'analyzed')
        publisher=Publisher(self.settings,store,Gateway(),clock=lambda:self.current); publisher.enqueue(bundle,'900201',self.eligible()); asyncio.run(publisher.dispatch_once())
        service=FeedbackService(self.settings,store,clock=lambda:self.current)
        event=ChatEvent('reaction',MessageRef('900001','900201','1001'),self.current,'add','900301',reaction='265',added=True)
        asyncio.run(service.apply(event)); self.assertEqual(service.learn('900001','900201')['preferences'],{})
        event.added=False; asyncio.run(service.apply(event))
        self.assertEqual(store.feedback_since('900001','900201',0,now=self.current),[])
