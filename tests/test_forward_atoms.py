import asyncio
import copy
import hashlib
import json
from dataclasses import asdict

from pullshit.agent.hermes_worker import build_instruction
from pullshit.content import selection
from pullshit.content.service import ContentService
from pullshit.contracts import AgentTask, ContentBundle, MediaRef, OutcomeUnknown, MessageRef
from pullshit.publishing.service import Publisher, PublishRejected
from tests.support import Workspace, Gateway, verdict


class ForwardGateway(Gateway):
    def __init__(self):
        super().__init__()
        self.forwarded = []

    async def forward_message(self, ref, target, *, before_send=None):
        if before_send:
            before_send()
        self.forwarded.append((ref, target))
        if self.error:
            raise self.error
        return '1234'


class ForwardAtomTests(Workspace):
    def build_card(self, *, image=False, missing=False, truncated=False):
        store = self.store()
        outer = self.bundle().messages[0]
        outer.segments = [{'type': 'forward', 'data': {'id': 'resource-original'}}]
        store.put_message(outer)
        owner = self

        class Evidence:
            async def fetch_forward(self, ref, identity):
                segments = [{'type': 'text', 'data': {'text': '卡片中的完整故事'}},
                            {'type': 'at', 'data': {'qq': '900301'}}]
                if image:
                    segments.append({'type': 'image', 'data': {'url': 'https://example.invalid/image'}})
                return [{'type': 'node', 'data': {'user_id': '900401', 'nickname': 'Alice',
                        'content': segments}},
                        {'type': 'node', 'data': {'user_id': '900402', 'nickname': 'Bob',
                        'content': [{'type': 'text', 'data': {'text': '故事的结尾'}}]}}]

        class Cache:
            async def cache(self, url, source, expires):
                payload = b'forward-image'; path = owner.root / 'forward-image'; path.write_bytes(payload)
                return MediaRef('inside-image', source, str(path), hashlib.sha256(payload).hexdigest(),
                                len(payload), 'image/png', expires)

        content = ContentService(self.settings, store, Evidence(), Cache() if image and not missing else None,
                                 clock=lambda: self.current)
        if truncated:
            content.max_nodes = 2
        bundle = asyncio.run(content.build(outer.ref))
        task = AgentTask('card', 'selection', '900201', self.current + 90, bundle)
        selection.prepare(task)
        return store, task

    def publish(self, store, task, *, inspected=None, gateway=None):
        output, analysis = selection.publications(task, verdict(task, ['U001'], inspected_images=inspected or []))[0]
        store.put_candidate(output, {'900201': asdict(analysis)})
        restored = store.get_candidate(output.bundle_id, now=self.current)['bundle']
        self.assertEqual(restored.forwards, output.forwards)
        gateway = gateway or ForwardGateway()
        publisher = Publisher(self.settings, store, gateway, clock=lambda: self.current)
        ids = publisher.enqueue(restored, '900201', analysis)
        asyncio.run(publisher.dispatch_once())
        return output, analysis, gateway, publisher, ids

    def test_whole_card_has_one_id_and_native_transport_preserves_original(self):
        store, task = self.build_card()
        self.assertEqual(len(task.selection['units']), 1)
        unit = selection.view(task)['units'][0]
        self.assertEqual(unit['kind'], 'forward')
        self.assertIn('900301', json.dumps(unit['content']))
        self.assertEqual(selection.validate(task, verdict(task, ['U002'])), 'publish_id_out_of_scope')
        instruction, _ = build_instruction(task, task.limits)
        self.assertIn('不能拆选或改写', instruction)
        output, _, gateway, _, ids = self.publish(store, task)
        self.assertEqual(output.messages, [task.bundle.messages[0]])
        self.assertEqual(gateway.forwarded, [(output.messages[0].ref, '900201')])
        self.assertFalse(gateway.sent)
        row = store.get_delivery(ids[0])
        self.assertEqual((row['status'], row['node_count'], row['part_total']), ('sent', 1, 1))
        self.assertNotIn('900301', json.dumps(row['nodes']))

    def test_all_nested_images_require_inspection_and_stay_inside_original(self):
        store, task = self.build_card(image=True)
        self.assertEqual(task.selection['units'][0]['media_ids'], ['inside-image'])
        self.assertEqual(selection.validate(task, verdict(task, ['U001']), []), 'selected_image_not_inspected')
        output, _, gateway, _, _ = self.publish(store, task, inspected=['inside-image'])
        self.assertEqual([m.reference_id for m in output.media], ['inside-image'])
        self.assertFalse(gateway.sent)
        self.assertEqual(len(gateway.forwarded), 1)

    def test_incomplete_or_unreadable_card_is_rejected_as_a_whole(self):
        for options in ({'image': True, 'missing': True}, {'truncated': True}):
            with self.subTest(options=options):
                _, task = self.build_card(**options)
                self.assertEqual(len(task.selection['units']), 1)
                self.assertEqual(selection.validate(task, verdict(task, ['U001'])), 'unit_not_publishable')

    def test_same_card_under_different_original_id_is_deduplicated(self):
        store, task = self.build_card()
        first, _, _, publisher, _ = self.publish(store, task)
        other = copy.deepcopy(task.bundle)
        original = other.messages[0].ref
        new = MessageRef(original.account_id, original.group_id, '999')
        other.messages[0].ref = new
        other.origins = {key: new.key for key in other.origins}
        other.forwards = {new.key: other.forwards[original.key]}
        other.trigger_refs = [new.key]
        second = AgentTask('other', 'selection', '900201', self.current + 90, other)
        output, analysis = selection.publications(second, verdict(second))[0]
        self.assertEqual(output.fingerprint, first.fingerprint)
        # Add an unrelated text fragment: deduplication still uses the whole card.
        output.messages.append(self.bundle(2).messages[1])
        text = output.messages[-1].segments[0]['data']['text']
        from pullshit.contracts import digest
        output.fingerprint = digest(['different', text])
        output.publication['components'].append(digest(['text', text]))
        with self.assertRaisesRegex(PublishRejected, 'content_already_seen'):
            publisher.enqueue(output, '900201', analysis)
        self.assertTrue(publisher.enqueue(output, '900202', analysis))

    def test_mixed_output_has_separate_native_card_between_ordinary_messages(self):
        store, task = self.build_card()
        before, after = self.bundle(3).messages[1:]
        before.occurred_at -= 1; after.occurred_at += 1
        task.bundle.messages = [before, *task.bundle.messages, after]
        task.selection = {}
        result = verdict(task)
        output, analysis = selection.publications(task, result)[0]
        publisher = Publisher(self.settings, store, ForwardGateway(), clock=lambda: self.current)
        ids = publisher.enqueue(output, '900201', analysis)
        rows = [store.get_delivery(identity) for identity in ids]
        self.assertEqual([[s['type'] for s in row['nodes']] for row in rows],
                         [['text'], ['forward_original'], ['text']])

    def test_unknown_native_receipt_is_not_retried(self):
        store, task = self.build_card()
        gateway = ForwardGateway(); gateway.error = OutcomeUnknown('lost ack')
        _, _, _, publisher, ids = self.publish(store, task, gateway=gateway)
        self.assertEqual(store.get_delivery(ids[0])['status'], 'unknown')
        asyncio.run(publisher.dispatch_once())
        self.assertEqual(len(gateway.forwarded), 1)

    def test_cached_card_image_mutation_blocks_native_send(self):
        store, task = self.build_card(image=True)
        output, analysis = selection.publications(task, verdict(task, inspected_images=['inside-image']))[0]
        store.put_candidate(output, {'900201': asdict(analysis)})
        gateway = ForwardGateway(); publisher = Publisher(self.settings, store, gateway, clock=lambda: self.current)
        ids = publisher.enqueue(output, '900201', analysis)
        from pathlib import Path
        Path(output.media[0].path).write_bytes(b'mutated')
        asyncio.run(publisher.dispatch_once())
        self.assertFalse(gateway.forwarded)
        self.assertEqual(store.get_delivery(ids[0])['status'], 'cancelled')

    def test_legacy_flattened_candidate_cannot_be_published(self):
        store, task = self.build_card()
        output, analysis = selection.publications(task, verdict(task))[0]
        output.messages = [task.bundle.messages[1]]
        output.origins = {output.messages[0].ref.key: task.bundle.messages[0].ref.key}
        output.forwards = {}
        with self.assertRaisesRegex(PublishRejected, 'forward_atom_required'):
            Publisher(self.settings, store, ForwardGateway(), clock=lambda: self.current).enqueue(output, '900201', analysis)
