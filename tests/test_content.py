import asyncio
from dataclasses import asdict
from pullshit.content.service import ContentService,ContentWindow
from pullshit.contracts import ChatMessage,MessageRef,DAY
from tests.support import Workspace

class ContentTests(Workspace):
    def test_shared_reply_is_not_a_cycle_and_original_order_is_preserved(self):
        store=self.store(); bundle=self.bundle(12)
        bundle.messages[1].segments.insert(0,{'type':'reply','data':{'id':'1'}})
        for message in bundle.messages: store.put_message(message)
        result=asyncio.run(ContentService(self.settings,store,clock=lambda:self.current).build(bundle.sources))
        self.assertFalse(any('cycle' in i for i in result.issues))
        self.assertEqual([m.ref.message_id for m in result.messages],[str(i) for i in range(1,13)])
    def test_reply_cycle_depth_missing_and_deadline(self):
        store=self.store(); bundle=self.bundle(2)
        bundle.messages[0].segments.append({'type':'reply','data':{'id':'2'}})
        bundle.messages[1].segments.append({'type':'reply','data':{'id':'1'}})
        for message in bundle.messages: store.put_message(message)
        service=ContentService(self.settings,store,clock=lambda:self.current)
        result=asyncio.run(service.build(bundle.sources[0])); self.assertIn('reply_cycle',result.issues)
        class Blocked:
            async def fetch_message(self,ref): await asyncio.sleep(60)
        service=ContentService(self.settings,store,Blocked(),clock=lambda:self.current); service.timeout_seconds=.02
        result=asyncio.run(service.build(MessageRef('900001','900101','999')))
        self.assertIn('timeout',result.issues)
    def test_actual_forward_payload_survives_repository_and_order(self):
        store=self.store(); message=self.bundle().messages[0]
        message.segments=[{'type':'forward','data':{'id':'forward-id'}}]; store.put_message(message)
        current=self.current
        class Platform:
            async def fetch_forward(self,ref,forward_id):
                return [{'sender':{'user_id':'900401','nickname':'sample'},'time':current,'message':[{'type':'text','data':{'text':str(i)}}]} for i in range(12)]
        result=asyncio.run(ContentService(self.settings,store,Platform(),clock=lambda:self.current).build(message.ref))
        self.assertEqual([m.segments[0]['data']['text'] for m in result.messages[1:]],[str(i) for i in range(12)])
        store.put_candidate(result); loaded=store.get_candidate(result.bundle_id,now=self.current)['bundle']
        self.assertEqual(len(loaded.messages),13)
        self.assertTrue(loaded.messages[1].ref.message_id.startswith('embedded:'))
    def test_unauthorized_ref_never_reaches_platform(self):
        class Platform:
            async def fetch_message(self,ref): raise AssertionError('unauthorized fetch')
        result=asyncio.run(ContentService(self.settings,self.store(),Platform(),clock=lambda:self.current).build(MessageRef('900001','999','1')))
        self.assertIn('reference_not_authorized',result.issues)
    def test_nested_forward_keeps_parent_then_child_and_verified_outer_source(self):
        store=self.store(); message=self.bundle().messages[0]
        node=lambda text,children: {'type':'node','data':{'user_id':'900401','content':[{'type':'text','data':{'text':text}},*children]}}
        message.segments=[node('parent',[node('child',[])])]; store.put_message(message)
        bundle=asyncio.run(ContentService(self.settings,store,clock=lambda:self.current).build(message.ref))
        self.assertEqual([m.segments[0]['data']['text'] for m in bundle.messages[1:]],['parent','child'])
        self.assertEqual(bundle.sources,[message.ref]); self.assertFalse(bundle.issues)
        store.put_candidate(bundle)
        self.assertEqual(len(store.get_candidate(bundle.bundle_id,now=self.current)['bundle'].messages),3)
        service=ContentService(self.settings,store,clock=lambda:self.current); service.max_nodes=2
        limited=asyncio.run(service.build(message.ref))
        self.assertEqual(len(limited.messages),2); self.assertIn('node_limit',limited.issues)
    def test_quiet_window_collects_the_group_conversation(self):
        window=ContentWindow(60,300,500); messages=self.bundle(3).messages
        messages[1].user_id='900402'; messages[1].occurred_at=self.current+55
        messages[2].user_id='900402'; messages[2].occurred_at=self.current+100
        for message in messages: window.add(message)
        self.assertEqual(window.due(self.current+101),[])
        self.assertEqual(window.due(self.current+160),[[m.ref for m in messages]])
