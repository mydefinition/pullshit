"""Publish complete content with durable message and merged-forward jobs."""
from __future__ import annotations
import base64
import copy
import hashlib
import math
from pathlib import Path
import time
from pullshit.contracts import AgentResult, ContentBundle, OutcomeUnknown, PlatformRejected, canonical, digest
from pullshit.content.forwards import component as forward_component

class PublishRejected(ValueError):
    pass

class Publisher:
    def __init__(self,settings,store,gateway,*,settings_provider=None,clock=time.time):
        self.settings,self.store,self.gateway=settings,store,gateway
        self.settings_provider=settings_provider or (lambda: settings)
        self.clock=clock

    def _eligible(self,bundle,target,analysis,settings):
        manifest=bundle.publication
        if (manifest.get('version')!=1 or manifest.get('context_status')!='complete'
                or not manifest.get('snapshot_id') or not manifest.get('publish_ids')):
            raise PublishRejected('explicit_selection_required')
        if bundle.expires_at <= self.clock(): raise PublishRejected('expired')
        if target not in settings.targets: raise PublishRejected('target_not_authorized')
        if not bundle.messages or not bundle.sources: raise PublishRejected('empty_or_unverified_bundle')
        if any(not settings.permits(ref.group_id,target) or (settings.account_id and ref.account_id != settings.account_id) for ref in bundle.sources):
            raise PublishRejected('broadcast_not_authorized')
        if any(m.own or m.recalled for m in bundle.messages): raise PublishRejected('own_or_recalled')
        if any(m.ref.message_id.startswith('embedded:') for m in bundle.messages):
            raise PublishRejected('forward_atom_required')
        for ref in bundle.sources:
            current=self.store.get_message(ref,now=self.clock())
            if current and current.recalled: raise PublishRejected('source_recalled')
        if self.incomplete_reason(bundle):
            raise PublishRejected('context_incomplete')
        if analysis.decision != 'eligible' or analysis.risks: raise PublishRejected('analysis_not_eligible')
        from pullshit.content.review import check
        problem=check(bundle,settings)
        if problem: raise PublishRejected(problem)
        if analysis.score < settings.data['agent']['minimum_score']: raise PublishRejected('score_too_low')
        if any(media.reference_id not in analysis.inspected_images for media in bundle.media): raise PublishRejected('image_not_inspected')
        components=[]; images=[]; forwards=[]
        for message in bundle.messages:
            if message.ref.key in bundle.forwards:
                forward=bundle.forwards[message.ref.key]
                if (forward['issues'] or digest(message.segments)!=forward['segments_digest']
                        or not any(s.get('type') in {'forward','node'} for s in message.segments)
                        or any(s.get('type') not in {'forward','node','text','image'} for s in message.segments)
                        or any(identity not in {m.reference_id for m in bundle.media} for identity in forward['media_ids'])):
                    raise PublishRejected('forward_manifest_mismatch')
                component=forward_component(forward); components.append(component); forwards.append(component)
                continue
            for segment in message.segments:
                if segment.get('type')=='text':
                    components.append(digest(['text',segment.get('data',{}).get('text','')]))
                elif segment.get('type')=='image':
                    media=next((m for m in bundle.media if m.source==message.ref and m.path==segment.get('data',{}).get('file')),None)
                    if media is None: raise PublishRejected('media_unavailable')
                    component=digest(['image',media.sha256]);components.append(component);images.append(component)
                else: raise PublishRejected('unselected_segment')
        if (sorted(components)!=sorted(manifest.get('components',[])) or sorted(images)!=sorted(manifest.get('image_components',[]))
                or sorted(forwards)!=sorted(manifest.get('forward_components',[]))):
            raise PublishRejected('selection_manifest_mismatch')

    @staticmethod
    def incomplete_reason(bundle):
        markers=('missing','limit','timeout','cycle','unavailable','rejected','invalid','unsupported','expired','transport_error','cq_string','embedded_reply','unverified','not_authorized')
        return next((issue for issue in bundle.issues if any(word in issue for word in markers)),None)

    def _records(self,bundle,settings):
        media={m.path:m for m in bundle.media if m.path}
        records=[]
        for message in bundle.messages:
            if message.ref.key in bundle.forwards:
                records.append(({'type':'forward_original','data':{
                    'account_id':message.ref.account_id,'group_id':message.ref.group_id,
                    'message_id':message.ref.message_id}},0))
                continue
            content=[]
            for segment in message.segments:
                kind=segment.get('type'); data=segment.get('data',{})
                if kind == 'text': content.append(({'type':'text','data':{'text':str(data.get('text',''))}},0))
                elif kind == 'at': raise PublishRejected('mention_not_publishable')
                elif kind == 'reply': continue
                elif kind == 'image':
                    path=str(data.get('file','')); image=media.get(path)
                    if image is None or image.expires_at <= self.clock() or not Path(path).is_file(): raise PublishRejected('media_unavailable')
                    if image.size > settings.data['media']['max_image_bytes']: raise PublishRejected('image_size_limit')
                    content.append(({'type':'image','data':{'file':path}},image.size))
                elif kind in {'forward','node'}:
                    continue
                else: raise PublishRejected('unsupported_segment')
            if content:
                records.append(({'type':'node','data':{'user_id':message.user_id,
                    'nickname':message.nickname or message.user_id,'time':int(message.occurred_at),
                    'content':content}},sum(amount for _,amount in content)))
        return records

    @staticmethod
    def _segment_count(nodes):
        return sum(len(node['data']['content']) if node.get('type')=='node' else 1 for node in nodes)

    def enqueue(self,bundle:ContentBundle,target:str,analysis:AgentResult):
        settings=self.settings_provider(); self._eligible(bundle,target,analysis,settings)
        if self.store.seen_publication(bundle.sources[0].account_id,target,bundle,now=self.clock()):
            raise PublishRejected('content_already_seen')
        chunks=self._chunks(bundle,settings)
        rows=[]
        for index,(nodes,media_bytes) in enumerate(chunks):
            nodes=copy.deepcopy(nodes)
            count=self._segment_count(nodes)
            estimated=len(canonical(nodes).encode())+4*math.ceil(media_bytes/3)+512*count
            row={'delivery_id':digest([bundle.sources[0].account_id,target,bundle.fingerprint,index]),
                 'account_id':bundle.sources[0].account_id,'group_id':target,'bundle_id':bundle.bundle_id,'fingerprint':bundle.fingerprint,
                 'part_index':index,'part_total':len(chunks),'nodes':nodes,'node_count':count,'estimated_bytes':estimated,
                 'expires_at':bundle.expires_at,'created_at':self.clock(),'tags':analysis.tags}
            rows.append(row)
        if hasattr(self.store,'enqueue_deliveries'):
            self.store.enqueue_deliveries(rows)
        else:
            for row in rows: self.store.enqueue_delivery(row)
        return [r['delivery_id'] for r in rows]

    def _chunks(self,bundle,settings):
        max_nodes=settings.data['limits']['max_message_segments']; max_bytes=settings.data['limits']['max_batch_media_bytes']
        chunks=[]; records=[]
        def flush():
            if not records: return
            merged=len(records)>1
            chunk=[]; size=0; count=0
            for record,_ in records:
                fragments=[]; content=[]; amount=0
                for segment,media_bytes in record['data']['content']:
                    if media_bytes>max_bytes: raise PublishRejected('record_media_size_limit')
                    if content and (len(content)>=max_nodes or amount+media_bytes>max_bytes):
                        fragments.append((content,amount)); content=[]; amount=0
                    content.append(segment); amount+=media_bytes
                if content: fragments.append((content,amount))
                for content,amount in fragments:
                    if chunk and (count+len(content)>max_nodes or size+amount>max_bytes):
                        chunks.append((chunk,size)); chunk=[]; size=0; count=0
                    if merged:
                        data={**record['data'],'content':content}
                        chunk.append({'type':'node','data':data})
                    else: chunk.extend(content)
                    size+=amount; count+=len(content)
            if chunk: chunks.append((chunk,size))
            records.clear()
        for node,amount in self._records(bundle,settings):
            if node['type']=='forward_original':
                flush()
                chunks.append(([node],0))
                continue
            records.append((node,amount))
        flush()
        if not chunks: raise PublishRejected('empty_bundle')
        return chunks

    def _materialize(self,row,bundle,settings):
        nodes=copy.deepcopy(row['nodes']); allowed={m.path:m for m in bundle.media if m.path}
        if len(nodes)==1 and nodes[0].get('type')=='forward_original':
            # Validate cached inspection evidence; native forwarding uploads no rebuilt media.
            for media in bundle.media:
                with Path(media.path).open('rb') as file:
                    payload=file.read(settings.data['media']['max_image_bytes']+1)
                if (media.expires_at<=self.clock() or len(payload)!=media.size
                        or len(payload)>settings.data['media']['max_image_bytes']
                        or hashlib.sha256(payload).hexdigest()!=media.sha256):
                    raise PublishRejected('media_invalid')
            return nodes
        merged=bool(nodes and nodes[0].get('type')=='node')
        segments=[]
        for node in nodes:
            if merged:
                if node.get('type')!='node' or not isinstance(node.get('data',{}).get('content'),list):
                    raise PublishRejected('invalid_message_payload')
                segments.extend(node['data']['content'])
            else: segments.append(node)
        for segment in segments:
            if segment.get('type') not in {'text','image'} or not isinstance(segment.get('data'),dict):
                raise PublishRejected('invalid_message_payload')
            if segment['type'] != 'image': continue
            path=segment['data']['file']; media=allowed.get(path)
            if media is None or media.expires_at <= self.clock(): raise PublishRejected('media_expired')
            with Path(path).open('rb') as file:
                payload=file.read(settings.data['media']['max_image_bytes']+1)
            if len(payload) != media.size or len(payload)>settings.data['media']['max_image_bytes'] or hashlib.sha256(payload).hexdigest()!=media.sha256:
                raise PublishRejected('media_invalid')
            segment['data']['file']='base64://'+base64.b64encode(payload).decode()
        return nodes

    async def dispatch_once(self):
        settings=self.settings_provider()
        if settings.data['runtime']['mode'] != 'auto' or not settings.data['sending']['enabled']: return None
        rows=self.store.deliveries(status='queued',now=self.clock())
        def priority(row):
            candidate=self.store.get_candidate(row['bundle_id'],now=self.clock())
            value=candidate['analysis'].get(row['group_id'],{}) if candidate else {}
            preferences=self.store.group_state(row['account_id'],row['group_id'])['preferences']
            tags=value.get('tags',[])
            adjustment=.1*sum(float(preferences.get(tag,0)) for tag in tags)/max(1,len(tags))
            return (row['part_index']==0,-float(value.get('score',0))-adjustment,row['created_at'],row['part_index'])
        rows.sort(key=priority)
        for row in rows:
            if row['account_id'] != self.gateway.account_id: continue
            candidate=self.store.get_candidate(row['bundle_id'],now=self.clock())
            if candidate is None or candidate['status'] in {'cancelled','rejected','needs_review'}:
                self.store.cancel_candidate(row['bundle_id'],'candidate_unavailable'); continue
            bundle=candidate['bundle']; analyses=candidate['analysis']; result=analyses.get(row['group_id'],analyses)
            try:
                self._eligible(bundle,row['group_id'],AgentResult.from_dict(result),settings)
                expected=self._chunks(bundle,settings)
                if (row['part_total']!=len(expected) or row['part_index']>=len(expected)
                        or row['nodes']!=expected[row['part_index']][0]):
                    raise PublishRejected('queued_payload_mismatch')
                nodes=self._materialize(row,bundle,settings)
            except (PublishRejected,ValueError,OSError,TypeError,KeyError):
                self.store.cancel_delivery(row['delivery_id'],'publication_check_failed'); continue
            claimed=self.store.claim_delivery(row['delivery_id'],settings.data['limits'],now=self.clock())
            if claimed is None: continue
            try:
                # Read effective settings again at the last boundary before a side effect.
                def before_send():
                    latest=self.settings_provider()
                    self._eligible(bundle,row['group_id'],AgentResult.from_dict(result),latest)
                    if not latest.data['sending']['enabled'] or latest.data['runtime']['mode']!='auto' or self.store.paused(row['account_id'],row['group_id']):
                        raise PublishRejected('paused_or_disabled')
                before_send()
                if len(nodes)==1 and nodes[0]['type']=='forward_original':
                    from pullshit.contracts import MessageRef
                    receipt=await self.gateway.forward_message(MessageRef.from_dict(nodes[0]['data']),row['group_id'],before_send=before_send)
                elif nodes[0]['type']=='node':
                    receipt=await self.gateway.send_forward(row['group_id'],nodes,before_send=before_send)
                else:
                    receipt=await self.gateway.send_message(row['group_id'],nodes,before_send=before_send)
                from pullshit.contracts import platform_id
                receipt=platform_id(receipt,True)
            except PublishRejected:
                self.store.finish_delivery(row['delivery_id'],'cancelled',error_code='rules_changed')
            except PlatformRejected:
                self.store.finish_delivery(row['delivery_id'],'failed',error_code='platform_rejected')
            except BaseException as exc:
                self.store.finish_delivery(row['delivery_id'],'unknown',error_code='receipt_unknown')
                import asyncio
                if isinstance(exc,(asyncio.CancelledError,KeyboardInterrupt,SystemExit)): raise
            else:
                self.store.finish_delivery(row['delivery_id'],'sent',message_id=receipt)
            return row['delivery_id']
        return None
