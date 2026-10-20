import asyncio
import copy
import json
from dataclasses import asdict
from unittest.mock import patch

from pullshit.agent.hermes_worker import build_instruction
from pullshit.agent.protocol import normalize_analysis
from pullshit.config import ConfigError, agent_config, load_settings
from pullshit.content import review, selection
from pullshit.contracts import AgentResult, AgentTask
from pullshit.publishing.service import Publisher, PublishRejected
from pullshit.runtime.service import Service
from tests.support import Workspace, Gateway, verdict


class ReviewTests(Workspace):
    def setUp(self):
        super().setUp()
        self.local['review']={'enabled':True}
        self.write_config(); self.settings=load_settings(self.root,'development').with_account('900001')
        self.result=AgentResult('eligible',.95,reason='all configured policies clear',usage={'complete':True})
        self.calls=[]; self.error=None; self.ids=None

    def service(self):
        service=Service(self.root,'development',clock=lambda:self.current)
        service.store=self.store(); service.gateway=Gateway()
        service.publisher=Publisher(self.settings,service.store,service.gateway,settings_provider=service.effective,clock=lambda:self.current)
        owner=self
        class Agent:
            async def run(self,task):
                owner.calls.append(copy.deepcopy(task))
                if task.kind=='selection': return verdict(task,owner.ids,inspected_images=[m.reference_id for m in task.bundle.media])
                if owner.error: raise owner.error
                return copy.deepcopy(owner.result)
        service.agent=Agent()
        class Feedback:
            def examples(self,*args): return []
        service.feedback=Feedback()
        return service

    def analyze(self,service,bundle=None):
        bundle=bundle or self.bundle()
        service.store.put_candidate(bundle)
        asyncio.run(service.analyze(service.store.get_candidate(bundle.bundle_id,now=self.current)))
        return [c for c in service.store.list_candidates(now=self.current) if c['bundle'].publication]

    def test_independent_review_runs_once_per_output_before_broadcast(self):
        service=self.service(); candidates=self.analyze(service)
        self.assertEqual([task.kind for task in self.calls],['selection','review'])
        chosen,check=self.calls
        self.assertNotEqual(chosen.task_id,check.task_id)
        self.assertEqual(check.audience,{'900201','900202'})
        self.assertEqual(check.preferences,{})
        self.assertEqual(check.selection,{})
        self.assertEqual(len(service.store.deliveries()),2)
        self.assertIsNone(review.check(candidates[0]['bundle'],self.settings))
        self.assertEqual(candidates[0]['analysis']['_review']['decision'],'eligible')
        asyncio.run(service.publisher.dispatch_once())
        self.assertEqual(len(service.gateway.sent),1)

    def test_rejected_uncertain_failed_incomplete_usage_and_low_score_never_queue(self):
        for result,error in [
            (AgentResult('rejected',.99,risks=['politics'],reason='political advocacy',usage={'complete':True}),None),
            (AgentResult('needs_review',reason='ambiguous allusion',usage={'complete':True}),None),
            (self.result,TimeoutError()),
            (AgentResult('eligible',.99,reason='clear',usage={'complete':False}),None),
            (AgentResult('eligible',.5,reason='not certain',usage={'complete':True}),None)]:
            with self.subTest(result=result.decision,error=error):
                self.calls=[]; self.result=result; self.error=error
                service=self.service(); publications=self.analyze(service)
                self.assertFalse(service.store.deliveries())
                self.assertIn(publications[0]['status'],{'rejected','needs_review'})
                self.assertTrue(publications[0]['analysis']['_review']['reason'])

    def test_only_selected_content_is_reviewed(self):
        service=self.service(); bundle=self.bundle(2); self.ids=['U001']
        bundle.messages[1].segments[0]['data']['text']='unselected political chatter'
        self.analyze(service,bundle)
        payload=json.dumps(review.view(self.calls[-1].bundle))
        self.assertNotIn('unselected political chatter',payload)
        self.assertEqual(len(self.calls[-1].bundle.messages),1)

    def test_each_independent_selection_has_its_own_review(self):
        service=self.service(); bundle=self.bundle(2); original=service.agent.run
        async def run(task):
            if task.kind!='selection': return await original(task)
            result=verdict(task,['U001']); result.selections.append({**result.selections[0],'publish_ids':['U002']})
            return result
        service.agent.run=run
        outputs=self.analyze(service,bundle)
        self.assertEqual(len(outputs),2)
        self.assertEqual(len(self.calls),2)
        self.assertTrue(all(task.kind=='review' and len(task.bundle.messages)==1 for task in self.calls))

    def test_policy_or_subject_change_invalidates_approval_before_send(self):
        service=self.service(); outputs=self.analyze(service); published=outputs[0]['bundle']
        altered=copy.deepcopy(published);altered.messages[0].segments[0]['data']['text']='changed'
        self.assertEqual(review.check(altered,self.settings),'review_content_changed')
        self.local['review']['policies']=[{'id':'custom','enabled':True,'instructions':'Block a different configured category.'}]
        self.write_config()
        asyncio.run(service.publisher.dispatch_once())
        self.assertFalse(service.gateway.sent)
        self.assertTrue(all(row['status']=='cancelled' for row in service.store.deliveries()))

    def test_unreviewed_legacy_candidate_cannot_enter_queue(self):
        task=AgentTask('selected','selection','900201',self.current+90,self.bundle())
        bundle,result=selection.publications(task,verdict(task))[0]
        with self.assertRaisesRegex(PublishRejected,'review_required'):
            Publisher(self.settings,self.store(),Gateway(),clock=lambda:self.current).enqueue(bundle,'900201',result)

    def test_review_of_whole_card_includes_all_records_and_requires_all_images(self):
        from tests.test_forward_atoms import ForwardAtomTests
        _,task=ForwardAtomTests.build_card(self,image=True)
        bundle,result=selection.publications(task,verdict(task,inspected_images=['inside-image']))[0]
        record=asyncio.run(review.run(bundle,['900201'],self.settings,self.service().agent,clock=lambda:self.current))
        self.assertEqual(record['decision'],'needs_review')
        self.assertEqual(record['reason'],'review_image_not_inspected')
        payload=json.dumps(review.view(bundle),ensure_ascii=False)
        self.assertIn('故事的结尾',payload);self.assertIn('inside-image',payload)
        self.result.inspected_images=['inside-image']
        asyncio.run(review.run(bundle,['900201'],self.settings,self.service().agent,clock=lambda:self.current))
        self.assertIsNone(review.check(bundle,self.settings))

    def test_review_prompt_is_complete_and_cannot_choose_or_expand_content(self):
        task=AgentTask('r','review','900201',self.current+90,self.bundle(),review=review.policy(self.settings))
        text,_=build_instruction(task,{'input_characters':64000})
        self.assertIn('politics',text);self.assertIn('禁止删改',text)
        task.bundle.messages[0].segments[0]['data']['text']='prefix'+'x'*4000+'TAIL'
        text,_=build_instruction(task,{'input_characters':64000});self.assertIn('TAIL',text)
        with self.assertRaisesRegex(ValueError,'input_character_limit'):
            build_instruction(task,{'input_characters':100})
        raw=asdict(verdict(AgentTask('s','selection','900201',self.current+90,self.bundle())))
        self.assertIn('review_cannot_select_content',normalize_analysis(raw,'review')[1])

    def test_invalid_policies_and_non_llm_review_are_rejected(self):
        for value in [[],[{'id':'x','enabled':True,'instructions':''}],
                      [{'id':'x','enabled':True,'instructions':'a'}]*2]:
            with self.subTest(policies=value):
                self.local['review']={'enabled':True,'policies':value};self.write_config()
                with self.assertRaises(ConfigError):load_settings(self.root,'development')
        self.local['review']={'enabled':True};self.local['agent']={'review':{'api_format':'systemone'}};self.write_config()
        with self.assertRaises(ConfigError):load_settings(self.root,'development')

    def test_review_connector_is_independent_of_selection_and_feedback(self):
        self.settings.data['agent']['selection'].update(api_format='systemone',model='selection-model')
        self.settings.data['agent']['review'].update(model='review-model',base_url='https://example.invalid')
        self.assertEqual(agent_config(self.settings.data,'review')['api_format'],'llm')
        self.assertEqual(agent_config(self.settings.data,'review')['model'],'review-model')
        self.assertEqual(agent_config(self.settings.data,'review')['minimum_score'],.8)
