import asyncio
import copy
import hashlib
import json
from dataclasses import asdict

from pullshit.agent.protocol import normalize_analysis
from pullshit.content import selection
from pullshit.content.service import ContentService
from pullshit.contracts import AgentResult, AgentTask, ChatMessage, MediaRef, MessageRef, digest
from pullshit.publishing.service import Publisher, PublishRejected
from tests.support import Workspace, Gateway, verdict


class SelectionTests(Workspace):
    def task(self, bundle=None):
        task=AgentTask('atomic-selection','selection','900201',self.current+90,bundle or self.bundle())
        selection.prepare(task)
        return task

    def image(self,bundle,message,label):
        payload=label.encode();path=self.root/label;path.write_bytes(payload)
        ref=MediaRef(label,message.ref,str(path),hashlib.sha256(payload).hexdigest(),len(payload),'image/png',bundle.expires_at)
        message.segments.append({'type':'image','data':{'file':str(path)}})
        bundle.media.append(ref)
        return ref

    def publish(self,task,result,store=None):
        store=store or self.store();gateway=Gateway()
        publisher=Publisher(self.settings,store,gateway,clock=lambda:self.current)
        bundles=selection.publications(task,result)
        for bundle,analysis in bundles:
            store.put_candidate(bundle,{'900201':asdict(analysis)})
            publisher.enqueue(bundle,'900201',analysis)
        asyncio.run(publisher.dispatch_once())
        return store,gateway,bundles

    def test_screenshots_exclude_unselected_sticker_mention_and_chat(self):
        bundle=self.bundle(4)
        for message in bundle.messages:message.segments=[]
        first=self.image(bundle,bundle.messages[0],'screenshot-a')
        self.image(bundle,bundle.messages[1],'duck-sticker')
        bundle.messages[2].segments=[{'type':'at','data':{'qq':'900301'}},{'type':'text','data':{'text':' '}}]
        last=self.image(bundle,bundle.messages[3],'screenshot-b')
        task=self.task(bundle)
        ids=[unit['id'] for unit in task.selection['units'] if unit['media_id'] in {first.reference_id,last.reference_id}]
        result=verdict(task,ids,inspected_images=[first.reference_id,last.reference_id])
        store,gateway,bundles=self.publish(task,result)
        self.assertEqual([s['type'] for s in gateway.sent[0][1]],['image','image'])
        self.assertEqual([m.reference_id for m in bundles[0][0].media],[first.reference_id,last.reference_id])
        self.assertNotIn('900301',json.dumps(gateway.sent))

    def test_mixed_qq_message_is_selectable_at_segment_level(self):
        bundle=self.bundle();image=self.image(bundle,bundle.messages[0],'picture')
        bundle.messages[0].segments.append({'type':'at','data':{'qq':'900301'}})
        task=self.task(bundle)
        self.assertEqual(len(task.selection['units']),3)
        self.assertEqual(len({unit['message_key'] for unit in task.selection['units']}),1)
        store,gateway,bundles=self.publish(task,verdict(task,['U002'],inspected_images=[image.reference_id]))
        self.assertEqual([s['type'] for s in gateway.sent[0][1]],['image'])
        self.assertEqual(selection.validate(task,verdict(task,['U003'])),'unit_not_publishable')

    def test_multiple_selections_create_separate_publications_and_preserve_order(self):
        task=self.task(self.bundle(3));result=verdict(task,['U003','U001'])
        result.selections.append({'publish_ids':['U002'],'context_status':'complete',
                                  'completeness_reason':'independent','reason':'second choice'})
        outputs=selection.publications(task,result)
        self.assertEqual([[m.ref.message_id for m in b.messages] for b,a in outputs],[['1','3'],['2']])
        self.assertNotEqual(outputs[0][0].bundle_id,outputs[1][0].bundle_id)

    def test_empty_selection_means_no_publication_and_eligible_cannot_omit_ids(self):
        task=self.task()
        self.assertIsNone(selection.validate(task,AgentResult('rejected',reason='none worth sharing')))
        self.assertEqual(selection.validate(task,AgentResult('eligible',.9,reason='whole window approved')),'selection_snapshot_mismatch')
        result=verdict(task);result.selections=[]
        self.assertEqual(selection.validate(task,result),'missing_publish_selection')

    def test_forged_ids_stale_revision_overlap_and_incomplete_are_rejected(self):
        task=self.task(self.bundle(2))
        for edit,error in [(lambda r:r.selections[0].update(publish_ids=['U999']),'publish_id_out_of_scope'),
                           (lambda r:setattr(r,'snapshot_revision',1),'selection_snapshot_mismatch'),
                           (lambda r:r.selections.append(copy.deepcopy(r.selections[0])),'overlapping_selections'),
                           (lambda r:r.selections[0].update(context_status='uncertain'),'selected_context_incomplete')]:
            result=verdict(task);edit(result)
            self.assertEqual(selection.validate(task,result),error)
        raw=asdict(verdict(task));raw['selections'][0]['publish_ids']=['U001','U001']
        self.assertIn('schema_fields',normalize_analysis(raw,'selection')[1])

    def test_unselected_missing_image_does_not_block_complete_text(self):
        bundle=self.bundle();bundle.messages[0].segments.append({'type':'image','data':{'url':'unavailable'}})
        bundle.issues=['media_unavailable'];task=self.task(bundle)
        self.assertEqual(selection.validate(task,verdict(task,['U002'])),'unit_not_publishable')
        store,gateway,bundles=self.publish(task,verdict(task,['U001']))
        self.assertEqual(len(gateway.sent),1)
        self.assertEqual(bundles[0][0].issues,[])

    def test_selected_image_requires_verified_inspection(self):
        bundle=self.bundle();self.image(bundle,bundle.messages[0],'uninspected')
        task=self.task(bundle)
        self.assertEqual(selection.validate(task,verdict(task,['U002']),[]),'selected_image_not_inspected')

    def test_identical_selected_image_has_same_fingerprint_with_different_neighbours(self):
        bundle=self.bundle();image=self.image(bundle,bundle.messages[0],'same-picture')
        first=self.task(bundle);result=verdict(first,['U002'],inspected_images=[image.reference_id])
        store,gateway,outputs=self.publish(first,result)
        other=copy.deepcopy(bundle);other.messages[0].segments.insert(0,{'type':'text','data':{'text':'new irrelevant chatter'}})
        other.bundle_id='different-observation';second=self.task(other)
        published=selection.publications(second,verdict(second,['U003'],inspected_images=[image.reference_id]))[0][0]
        self.assertEqual(published.fingerprint,outputs[0][0].fingerprint)
        self.assertNotEqual(published.bundle_id,outputs[0][0].bundle_id)
        later=verdict(second,['U001','U003'],inspected_images=[image.reference_id])
        selected,analysis=selection.publications(second,later)[0]
        store.put_candidate(selected,{'900201':asdict(analysis)})
        publisher=Publisher(self.settings,store,gateway,clock=lambda:self.current)
        with self.assertRaisesRegex(PublishRejected,'content_already_seen'):
            publisher.enqueue(selected,'900201',analysis)
        self.assertTrue(publisher.enqueue(selected,'900202',analysis))

    def test_old_queued_and_extra_persisted_segments_are_blocked(self):
        store=self.store();bundle=self.bundle();result=self.eligible();gateway=Gateway()
        publisher=Publisher(self.settings,store,gateway,clock=lambda:self.current)
        with self.assertRaisesRegex(PublishRejected,'explicit_selection_required'):
            publisher.enqueue(bundle,'900201',result)
        task=self.task();selected,analysis=selection.publications(task,verdict(task))[0]
        store.put_candidate(selected,{'900201':asdict(analysis)})
        ids=publisher.enqueue(selected,'900201',analysis)
        row=store.get_delivery(ids[0]);row['nodes'].append({'type':'text','data':{'text':'unselected @900301'}})
        store.db.execute('UPDATE deliveries SET nodes=? WHERE delivery_id=?',(json.dumps(row['nodes']),ids[0]))
        asyncio.run(publisher.dispatch_once())
        self.assertEqual(store.get_delivery(ids[0])['status'],'cancelled');self.assertFalse(gateway.sent)

    def test_embedded_selection_keeps_verified_outer_origin_and_recall_guard(self):
        store=self.store();outer=self.bundle().messages[0]
        outer.segments=[{'type':'node','data':{'user_id':'900401','content':[{'type':'text','data':{'text':'self contained embedded text'}}]}}]
        store.put_message(outer)
        bundle=asyncio.run(ContentService(self.settings,store,clock=lambda:self.current).build(outer.ref))
        task=self.task(bundle);selected,analysis=selection.publications(task,verdict(task,['U002']))[0]
        self.assertEqual(selected.sources,[outer.ref])
        store.put_candidate(selected,{'900201':asdict(analysis)});store.recall_message(outer.ref,now=self.current)
        with self.assertRaisesRegex(PublishRejected,'source_recalled'):
            Publisher(self.settings,store,Gateway(),clock=lambda:self.current).enqueue(selected,'900201',analysis)

    def test_review_and_discovery_assignment_survive_database_reopen(self):
        store=self.store();bundle=self.bundle();bundle.trigger_refs=[ref.key for ref in bundle.sources]
        task=self.task(bundle);key=task.selection['units'][0]['key']
        store.put_candidate(bundle);store.set_candidate_status(bundle.bundle_id,'analyzed',analysis={
            '_selection':{'unit_keys':[key],'decision':'rejected','reason':'already assessed','selected_keys':[]}})
        path=store.path
        from pullshit.storage.store import Store
        reopened=Store(path)
        try:
            self.assertEqual(reopened.unassigned_refs(bundle.sources,now=self.current),[])
            self.assertEqual(reopened.unit_history(key,now=self.current)[0]['reason'],'already assessed')
        finally:reopened.close()

    def test_bounded_nearby_context_is_historical_and_cannot_be_republished_alone(self):
        store=self.store();bundle=self.bundle(2);old,new=bundle.messages
        old.occurred_at=self.current-120;new.segments.insert(0,{'type':'reply','data':{'id':old.ref.message_id}})
        for message in bundle.messages:store.put_message(message)
        bundle.messages=[new];bundle.trigger_refs=[new.ref.key]
        task=self.task(bundle);original_id=task.selection['snapshot_id']
        content=ContentService(self.settings,store,clock=lambda:self.current)
        result=asyncio.run(selection.expand(task,{'anchor_id':'U002','mode':'reply','reason':'resolve late reply'},
                                            self.settings,store,content,self.current))
        self.assertEqual(result['snapshot_id'],original_id);self.assertEqual(result['snapshot_revision'],1)
        self.assertEqual([u['id'] for u in result['units']],['U003'])
        self.assertFalse(result['units'][0]['new'])
        self.assertEqual(selection.validate(task,verdict(task,['U003'])),'historical_content_only')
        selected,analysis=selection.publications(task,verdict(task,['U002','U003']))[0]
        self.assertEqual([m.ref.message_id for m in selected.messages],['1','2'])
        again=asyncio.run(selection.expand(task,{'anchor_id':'U002','mode':'reply','reason':'check again'},
                                           self.settings,store,content,self.current))
        self.assertEqual(again['units'],[])
        blocked=asyncio.run(selection.expand(task,{'anchor_id':'U003','mode':'nearby','reason':'walk history'},
                                             self.settings,store,content,self.current))
        self.assertEqual(blocked['error'],'context_call_limit')

    def test_context_character_budget_rejects_without_exposing_or_renumbering(self):
        store=self.store();task=self.task();original=task.selection['units'][0]['id']
        neighbour=self.bundle().messages[0];neighbour.ref=MessageRef('900001','900101','9')
        neighbour.segments=[{'type':'text','data':{'text':'x'*17000}}];store.put_message(neighbour)
        result=asyncio.run(selection.expand(task,{'anchor_id':original,'mode':'nearby','reason':'need previous explanation'},
                                            self.settings,store,ContentService(self.settings,store,clock=lambda:self.current),self.current))
        self.assertEqual(result['error'],'context_expansion_budget')
        self.assertEqual([u['id'] for u in task.selection['units']],[original])
        self.assertEqual(task.selection['revision'],0)

    def test_context_requires_known_anchor_and_reason(self):
        store=self.store();task=self.task();content=ContentService(self.settings,store,clock=lambda:self.current)
        for args in ({'anchor_id':'U999','reason':'fetch anything'},{'anchor_id':'U001'}):
            result=asyncio.run(selection.expand(task,args,self.settings,store,content,self.current))
            self.assertEqual(result['error'],'context_anchor_and_reason_required')
        self.assertEqual(task.selection['expansion_calls'],0)

    def test_reanalysis_uses_a_distinct_snapshot_identity(self):
        first=self.task();result=verdict(first)
        second=self.task(copy.deepcopy(first.bundle))
        self.assertNotEqual(first.selection['snapshot_id'],second.selection['snapshot_id'])
        self.assertEqual(selection.validate(second,result),'selection_snapshot_mismatch')

    def test_context_media_downloads_obey_the_task_image_budget(self):
        store=self.store();task=self.task();neighbour=self.bundle().messages[0]
        neighbour.ref=MessageRef('900001','900101','9')
        neighbour.segments=[{'type':'image','data':{'url':f'https://example.invalid/{i}'}} for i in range(5)]
        store.put_message(neighbour);calls=[];owner=self
        class Cache:
            async def cache(inner,url,source,expires):
                calls.append(url);path=owner.root/f'cached-{len(calls)}';path.write_bytes(url.encode())
                return MediaRef(str(len(calls)),source,str(path),hashlib.sha256(url.encode()).hexdigest(),len(url),'image/png',expires)
        content=ContentService(self.settings,store,media_cache=Cache(),clock=lambda:self.current)
        result=asyncio.run(selection.expand(task,{'anchor_id':'U001','mode':'nearby','reason':'look for the referenced images'},
                                            self.settings,store,content,self.current))
        self.assertEqual(len(calls),4)
        self.assertEqual(result['error'],'context_expansion_budget')
        self.assertEqual(len(task.selection['units']),1)

    def test_historical_combination_requires_an_explicit_new_value_reason(self):
        task=self.task(self.bundle(2));task.selection['units'][0]['new']=False
        result=verdict(task);result.selections[0].pop('new_value_reason')
        self.assertEqual(selection.validate(task,result),'missing_new_value_reason')
