import asyncio
from dataclasses import asdict
from pullshit.contracts import OutcomeUnknown,PlatformRejected,DAY
from pullshit.publishing.service import Publisher,PublishRejected
from tests.support import Workspace,Gateway

class PublishingTests(Workspace):
    def prepare(self,count):
        store=self.store(); bundle=self.bundle(count); result=self.eligible(); gateway=Gateway()
        store.put_candidate(bundle,{ '900201':asdict(result),'900202':asdict(result) })
        store.set_candidate_status(bundle.bundle_id,'analyzed')
        publisher=Publisher(self.settings,store,gateway,clock=lambda:self.current)
        ids=publisher.enqueue(bundle,'900201',result)
        return store,bundle,publisher,gateway,ids
    def test_exact_node_boundaries_and_order(self):
        for count,expected in [(99,[99]),(100,[100]),(101,[100,1]),(200,[100,100]),(201,[100,100,1])]:
            with self.subTest(count=count):
                store,bundle,publisher,gateway,ids=self.prepare(count)
                rows=[store.get_delivery(i) for i in ids]
                self.assertEqual([len(r['nodes']) for r in rows],expected)
                nodes=[node for row in rows for node in row['nodes']]
                self.assertEqual([node['data']['content'] for node in nodes],
                                 [message.segments for message in bundle.messages])
                self.assertEqual([node['data']['user_id'] for node in nodes],
                                 [message.user_id for message in bundle.messages])
                self.assertEqual(publisher.enqueue(bundle,'900201',self.eligible()),ids)
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
    def test_route_revocation_and_recall_cancel_queued(self):
        store,bundle,publisher,gateway,ids=self.prepare(1)
        store.recall_message(bundle.sources[0],now=self.current)
        self.assertIsNone(asyncio.run(publisher.dispatch_once())); self.assertFalse(gateway.sent)
    def test_unknown_analysis_and_uninspected_images_are_blocked(self):
        store=self.store(); gateway=Gateway(); publisher=Publisher(self.settings,store,gateway,clock=lambda:self.current)
        result=self.eligible(); result.risks=['unknown']
        with self.assertRaises(PublishRejected): publisher.enqueue(self.bundle(),'900201',result)
    def test_pause_during_gateway_login_blocks_platform_send(self):
        store,bundle,publisher,gateway,ids=self.prepare(1)
        original=gateway.send_forward
        async def changed(group,nodes,*,before_send=None):
            store.set_paused('900001',group,True)
            return await original(group,nodes,before_send=before_send)
        gateway.send_forward=changed
        asyncio.run(publisher.dispatch_once())
        self.assertFalse(gateway.sent); self.assertEqual(store.get_delivery(ids[0])['status'],'cancelled')
