import asyncio
from dataclasses import asdict
from pullshit.contracts import OutcomeUnknown,PlatformRejected,DAY
from pullshit.publishing.service import Publisher,PublishRejected
from tests.support import Workspace,Gateway,verdict

class PublishingTests(Workspace):
    def publication_bundle(self,count=1):
        from pullshit.contracts import AgentTask
        from pullshit.content.selection import publications
        bundle=self.bundle(count)
        task=AgentTask('publish-test','selection','900201',self.current+90,bundle)
        return publications(task,verdict(task))[0][0]

    def test_target_removal_does_not_cancel_other_broadcast_recipients(self):
        store,bundle,publisher,gateway,ids=self.prepare(1)
        other=publisher.enqueue(bundle,'900202',self.eligible())
        self.local['authorization']['target_group_ids']=['900202']; self.write_config()
        from pullshit.config import load_settings
        publisher.settings_provider=lambda:load_settings(self.root,'development').with_account('900001')
        asyncio.run(publisher.dispatch_once())
        self.assertEqual(store.get_delivery(ids[0])['status'],'cancelled')
        self.assertEqual(store.get_delivery(other[0])['status'],'sent')
        self.assertEqual([group for group,_ in gateway.sent],['900202'])

    def test_source_cannot_be_a_broadcast_recipient(self):
        self.local['authorization']['target_group_ids'].append('900101'); self.write_config()
        from pullshit.config import load_settings
        self.settings=load_settings(self.root,'development').with_account('900001')
        publisher=Publisher(self.settings,self.store(),Gateway(),clock=lambda:self.current)
        with self.assertRaisesRegex(PublishRejected,'broadcast_not_authorized'):
            publisher.enqueue(self.publication_bundle(),'900101',self.eligible())

    def test_group_taste_does_not_filter_selected_broadcast(self):
        store,bundle,publisher,gateway,ids=self.prepare(1)
        store.set_group_state('900001','900202',{'preferences':{'test':-1}})
        result=self.eligible(); result.score=self.settings.data['agent']['minimum_score']
        self.assertTrue(publisher.enqueue(bundle,'900202',result))

    def test_queued_forward_payload_is_cancelled_without_sending(self):
        store,bundle,publisher,gateway,ids=self.prepare(1)
        import json
        store.db.execute('UPDATE deliveries SET nodes=? WHERE delivery_id=?',
                         (json.dumps([{'type':'node','data':{'content':bundle.messages[0].segments}}]),ids[0]))
        asyncio.run(publisher.dispatch_once())
        self.assertFalse(gateway.sent)
        self.assertEqual(store.get_delivery(ids[0])['status'],'cancelled')

    def test_image_bytes_are_sent_as_an_ordinary_message_segment(self):
        from pathlib import Path
        import hashlib
        from pullshit.contracts import MediaRef
        store=self.store(); bundle=self.publication_bundle(); image=b'synthetic-image-payload'
        path=self.root/'image'; path.write_bytes(image)
        bundle.messages[0].segments.append({'type':'image','data':{'file':str(path)}})
        bundle.media=[MediaRef('image',bundle.sources[0],str(path),hashlib.sha256(image).hexdigest(),len(image),'image/png',bundle.expires_at)]
        from pullshit.contracts import AgentTask
        from pullshit.content.selection import publications
        task=AgentTask('image-test','selection','900201',self.current+90,bundle)
        bundle,result=publications(task,verdict(task,inspected_images=['image']))[0]
        store.put_candidate(bundle,{'900201':asdict(result)})
        gateway=Gateway(); publisher=Publisher(self.settings,store,gateway,clock=lambda:self.current)
        publisher.enqueue(bundle,'900201',result); asyncio.run(publisher.dispatch_once())
        segments=gateway.sent[0][1]
        self.assertEqual([segment['type'] for segment in segments],['text','image'])
        self.assertTrue(segments[1]['data']['file'].startswith('base64://'))

    def prepare(self,count):
        store=self.store(); bundle=self.publication_bundle(count); result=self.eligible(); gateway=Gateway()
        store.put_candidate(bundle,{ '900201':asdict(result),'900202':asdict(result) })
        store.set_candidate_status(bundle.bundle_id,'analyzed')
        publisher=Publisher(self.settings,store,gateway,clock=lambda:self.current)
        ids=publisher.enqueue(bundle,'900201',result)
        return store,bundle,publisher,gateway,ids
    def test_exact_segment_boundaries_and_order(self):
        for count,expected in [(99,[99]),(100,[100]),(101,[100,1]),(200,[100,100]),(201,[100,100,1])]:
            with self.subTest(count=count):
                store,bundle,publisher,gateway,ids=self.prepare(count)
                rows=[store.get_delivery(i) for i in ids]
                self.assertEqual([len(r['nodes']) for r in rows],expected)
                nodes=[node for row in rows for node in row['nodes']]
                self.assertEqual([node['type'] for node in nodes],['node']*count)
                self.assertEqual([node['data']['content'][0]['data']['text'] for node in nodes],
                                 [message.segments[0]['data']['text'] for message in bundle.messages])
                self.assertEqual(publisher.enqueue(bundle,'900201',self.eligible()),ids)

    def test_multiple_chat_records_use_forward_api_and_preserve_identity_and_images(self):
        import hashlib
        from pullshit.contracts import AgentTask,MediaRef
        from pullshit.content.selection import publications
        store=self.store(); bundle=self.bundle(2)
        image=b'checked-image'; path=self.root/'image'; path.write_bytes(image)
        first,second=bundle.messages
        first.user_id='900411';first.nickname='甲';first.occurred_at-=1
        second.user_id='900422';second.nickname='乙'
        first.segments=[{'type':'text','data':{'text':'前文'}},{'type':'image','data':{'file':str(path)}},
                        {'type':'text','data':{'text':'后文'}}]
        second.segments=[{'type':'text','data':{'text':'另一人的回复'}}]
        bundle.media=[MediaRef('image',first.ref,str(path),hashlib.sha256(image).hexdigest(),len(image),'image/png',bundle.expires_at)]
        task=AgentTask('merged-chat','selection','900201',self.current+90,bundle)
        output,result=publications(task,verdict(task,inspected_images=['image']))[0]
        store.put_candidate(output,{'900201':asdict(result),'900202':asdict(result)})
        gateway=Gateway(); calls=[]
        async def forward(group,nodes,*,before_send=None):
            if before_send:before_send()
            calls.append((group,nodes));return '2001'
        gateway.send_forward=forward
        publisher=Publisher(self.settings,store,gateway,clock=lambda:self.current)
        ids=publisher.enqueue(output,'900201',result)
        row=store.get_delivery(ids[0])
        self.assertEqual(row['node_count'],4)
        asyncio.run(publisher.dispatch_once())
        self.assertFalse(gateway.sent)
        self.assertEqual(len(calls),1)
        nodes=calls[0][1]
        self.assertEqual([(n['data']['user_id'],n['data']['nickname'],n['data']['time']) for n in nodes],
                         [(m.user_id,m.nickname,int(m.occurred_at)) for m in output.messages])
        content=nodes[0]['data']['content']
        self.assertEqual([s['type'] for s in content],['text','image','text'])
        self.assertEqual([content[0]['data']['text'],content[2]['data']['text']],['前文','后文'])
        self.assertTrue(content[1]['data']['file'].startswith('base64://'))
        self.assertEqual(nodes[1]['data']['content'][0]['data']['text'],'另一人的回复')
        self.assertEqual(store.get_delivery(ids[0])['message_id'],'2001')
        other=publisher.enqueue(output,'900202',result)
        path.write_bytes(b'modified-image')
        asyncio.run(publisher.dispatch_once())
        self.assertEqual(len(calls),1)
        self.assertEqual(store.get_delivery(other[0])['status'],'cancelled')

    def test_large_record_splits_into_nodes_retaining_sender_and_segment_order(self):
        from pullshit.contracts import AgentTask
        from pullshit.content.selection import publications
        self.settings.data['limits']['max_message_segments']=2
        store=self.store();bundle=self.bundle(2)
        bundle.messages[0].segments=[{'type':'text','data':{'text':text}} for text in ('甲','乙','丙')]
        task=AgentTask('large-record','selection','900201',self.current+90,bundle)
        output,result=publications(task,verdict(task))[0]
        publisher=Publisher(self.settings,store,Gateway(),clock=lambda:self.current)
        rows=[store.get_delivery(i) for i in publisher.enqueue(output,'900201',result)]
        self.assertEqual([r['node_count'] for r in rows],[2,2])
        nodes=[n for row in rows for n in row['nodes']]
        self.assertEqual([len(n['data']['content']) for n in nodes],[2,1,1])
        self.assertEqual([s['data']['text'] for n in nodes for s in n['data']['content']],
                         ['甲','乙','丙',bundle.messages[1].segments[0]['data']['text']])
        self.assertTrue(all(n['data']['user_id']=='900401' for n in nodes))

    def test_forward_cards_pack_complete_records_with_segment_and_media_limits(self):
        import hashlib
        from pullshit.contracts import AgentTask,MediaRef
        from pullshit.content.selection import publications
        self.settings.data['limits']['max_message_segments']=3
        self.settings.data['limits']['max_batch_media_bytes']=8
        store=self.store();bundle=self.bundle(3);media=[]
        for i,message in enumerate(bundle.messages):
            payload=b'12345';path=self.root/str(i);path.write_bytes(payload)
            message.segments.append({'type':'image','data':{'file':str(path)}})
            media.append(MediaRef(str(i),message.ref,str(path),hashlib.sha256(payload).hexdigest(),len(payload),'image/png',bundle.expires_at))
        bundle.media=media;task=AgentTask('limits','selection','900201',self.current+90,bundle)
        output,result=publications(task,verdict(task,inspected_images=['0','1','2']))[0]
        store.put_candidate(output,{'900201':asdict(result)})
        publisher=Publisher(self.settings,store,Gateway(),clock=lambda:self.current)
        rows=[store.get_delivery(i) for i in publisher.enqueue(output,'900201',result)]
        self.assertEqual([r['node_count'] for r in rows],[2,2,2])
        self.assertEqual([len(r['nodes']) for r in rows],[1,1,1])
        self.assertTrue(all(r['nodes'][0]['type']=='node' for r in rows))

    def test_nested_forward_sender_tampering_blocks_send(self):
        store,bundle,publisher,gateway,ids=self.prepare(2)
        import json
        row=store.get_delivery(ids[0]);row['nodes'][0]['data']['nickname']='伪造发送者'
        store.db.execute('UPDATE deliveries SET nodes=? WHERE delivery_id=?',(json.dumps(row['nodes']),ids[0]))
        asyncio.run(publisher.dispatch_once())
        self.assertFalse(gateway.sent)
        self.assertEqual(store.get_delivery(ids[0])['status'],'cancelled')
    def test_spacing_partial_success_pause_and_resume(self):
        store,bundle,publisher,gateway,ids=self.prepare(201)
        self.assertEqual(asyncio.run(publisher.dispatch_once()),ids[0])
        self.assertIsNone(asyncio.run(publisher.dispatch_once()))
        self.current+=600; store.set_paused('900001','900201',True)
        self.assertIsNone(asyncio.run(publisher.dispatch_once()))
        store.set_paused('900001','900201',False)
        asyncio.run(publisher.dispatch_once()); self.current+=600; asyncio.run(publisher.dispatch_once())
        self.assertEqual(len(gateway.sent),3)
        self.assertTrue(all(store.get_delivery(i)['status']=='sent' for i in ids))
    def test_unknown_stops_later_parts_and_never_resends(self):
        store,bundle,publisher,gateway,ids=self.prepare(101); gateway.error=OutcomeUnknown('timeout')
        asyncio.run(publisher.dispatch_once()); self.current+=600
        self.assertEqual(store.get_delivery(ids[0])['status'],'unknown')
        self.assertIsNone(asyncio.run(publisher.dispatch_once()))
        store.recover_inflight(); self.assertIsNone(asyncio.run(publisher.dispatch_once()))
        store.confirm_delivery(ids[0],'-100'); gateway.error=None
        asyncio.run(publisher.dispatch_once()); self.assertEqual(len(gateway.sent),1)
    def test_failed_only_retries_failed_part(self):
        store,bundle,publisher,gateway,ids=self.prepare(101)
        asyncio.run(publisher.dispatch_once()); self.current+=600; gateway.error=PlatformRejected('denied')
        asyncio.run(publisher.dispatch_once()); self.assertEqual(store.get_delivery(ids[1])['status'],'failed')
        self.current+=600; store.retry_delivery(ids[1]); gateway.error=None
        asyncio.run(publisher.dispatch_once()); self.assertEqual(len(gateway.sent),2)
    def test_recall_cancel_queued(self):
        store,bundle,publisher,gateway,ids=self.prepare(1)
        store.recall_message(bundle.sources[0],now=self.current)
        self.assertIsNone(asyncio.run(publisher.dispatch_once())); self.assertFalse(gateway.sent)
    def test_unknown_analysis_and_uninspected_images_are_blocked(self):
        store=self.store(); gateway=Gateway(); publisher=Publisher(self.settings,store,gateway,clock=lambda:self.current)
        result=self.eligible(); result.risks=['unknown']
        with self.assertRaises(PublishRejected): publisher.enqueue(self.publication_bundle(),'900201',result)
    def test_pause_during_gateway_login_blocks_platform_send(self):
        store,bundle,publisher,gateway,ids=self.prepare(1)
        original=gateway.send_message
        async def changed(group,nodes,*,before_send=None):
            store.set_paused('900001',group,True)
            return await original(group,nodes,before_send=before_send)
        gateway.send_message=changed
        asyncio.run(publisher.dispatch_once())
        self.assertFalse(gateway.sent); self.assertEqual(store.get_delivery(ids[0])['status'],'cancelled')
