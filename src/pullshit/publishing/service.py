"""Publish complete content bundles using bounded durable forwarding jobs."""
from __future__ import annotations
import base64
import copy
import hashlib
import math
from pathlib import Path
import time
from pullshit.contracts import AgentResult, ContentBundle, OutcomeUnknown, PlatformRejected, canonical, digest

class PublishRejected(ValueError):
    pass

class Publisher:
    def __init__(self,settings,store,gateway,*,settings_provider=None,clock=time.time):
        self.settings,self.store,self.gateway=settings,store,gateway
        self.settings_provider=settings_provider or (lambda: settings)
        self.clock=clock

    def _eligible(self,bundle,target,analysis,settings):
        if bundle.expires_at <= self.clock(): raise PublishRejected('expired')
        if target not in settings.targets: raise PublishRejected('target_not_authorized')
        if not bundle.messages or not bundle.sources: raise PublishRejected('empty_or_unverified_bundle')
        if any(not settings.permits(ref.group_id,target) or (settings.account_id and ref.account_id != settings.account_id) for ref in bundle.sources):
            raise PublishRejected('route_not_authorized')
        if any(m.own or m.recalled for m in bundle.messages): raise PublishRejected('own_or_recalled')
        for ref in bundle.sources:
            current=self.store.get_message(ref,now=self.clock())
            if current and current.recalled: raise PublishRejected('source_recalled')
        if self.incomplete_reason(bundle):
            raise PublishRejected('context_incomplete')
        if analysis.decision != 'eligible' or analysis.risks: raise PublishRejected('analysis_not_eligible')
        preferences=self.store.group_state(bundle.sources[0].account_id,target)['preferences']
        adjustment=sum(float(preferences.get(tag,0)) for tag in analysis.tags)/max(1,len(analysis.tags))*.1
        if analysis.score+adjustment < settings.data['agent']['minimum_score']: raise PublishRejected('score_too_low')
        if any(media.reference_id not in analysis.inspected_images for media in bundle.media): raise PublishRejected('image_not_inspected')

    @staticmethod
    def incomplete_reason(bundle):
        markers=('missing','limit','timeout','cycle','unavailable','rejected','invalid','unsupported','expired','transport_error','cq_string','embedded_reply','unverified','not_authorized')
        return next((issue for issue in bundle.issues if any(word in issue for word in markers)),None)

    def _nodes(self,bundle,settings):
        media={m.path:m for m in bundle.media if m.path}
        nodes=[]
        for message in bundle.messages:
            content=[]; media_bytes=0
            for segment in message.segments:
                kind=segment.get('type'); data=segment.get('data',{})
                if kind == 'text': content.append({'type':'text','data':{'text':str(data.get('text',''))}})
                elif kind == 'at': content.append({'type':'text','data':{'text':'@'+str(data.get('qq',''))}})
                elif kind == 'reply': continue
                elif kind == 'image':
                    path=str(data.get('file','')); image=media.get(path)
                    if image is None or image.expires_at <= self.clock() or not Path(path).is_file(): raise PublishRejected('media_unavailable')
                    if image.size > settings.data['media']['max_image_bytes']: raise PublishRejected('image_size_limit')
                    content.append({'type':'image','data':{'file':path}}); media_bytes+=image.size
                elif kind in {'forward','node'}:
                    continue
                else: raise PublishRejected('unsupported_segment')
            if content:
                nodes.append(({'type':'node','data':{'user_id':message.user_id,'nickname':message.nickname or message.user_id,'content':content}},media_bytes))
        return nodes

    def enqueue(self,bundle:ContentBundle,target:str,analysis:AgentResult):
        settings=self.settings_provider(); self._eligible(bundle,target,analysis,settings)
        max_nodes=settings.data['limits']['max_forward_nodes']; max_bytes=settings.data['limits']['max_batch_media_bytes']
        chunks=[]; chunk=[]; size=0
        for node,amount in self._nodes(bundle,settings):
            if amount > max_bytes: raise PublishRejected('record_media_size_limit')
            if chunk and (len(chunk)>=max_nodes or size+amount>max_bytes): chunks.append((chunk,size)); chunk=[]; size=0
            chunk.append(node); size+=amount
        if chunk: chunks.append((chunk,size))
        if not chunks: raise PublishRejected('empty_bundle')
        rows=[]
        for index,(nodes,media_bytes) in enumerate(chunks):
            nodes=copy.deepcopy(nodes)
            nodes[0]['data']['content'].insert(0,{'type':'text','data':{'text':f'[内容 {bundle.bundle_id[:8]} · {index+1}/{len(chunks)}]\n'}})
            estimated=len(canonical(nodes).encode())+4*math.ceil(media_bytes/3)+512*len(nodes)
            row={'delivery_id':digest([bundle.sources[0].account_id,target,bundle.fingerprint,index]),
                 'account_id':bundle.sources[0].account_id,'group_id':target,'bundle_id':bundle.bundle_id,'fingerprint':bundle.fingerprint,
                 'part_index':index,'part_total':len(chunks),'nodes':nodes,'node_count':len(nodes),'estimated_bytes':estimated,
                 'expires_at':bundle.expires_at,'created_at':self.clock(),'tags':analysis.tags}
            rows.append(row)
        if hasattr(self.store,'enqueue_deliveries'):
            self.store.enqueue_deliveries(rows)
        else:
            for row in rows: self.store.enqueue_delivery(row)
        return [r['delivery_id'] for r in rows]

    def _materialize(self,row,bundle,settings):
        nodes=copy.deepcopy(row['nodes']); allowed={m.path:m for m in bundle.media if m.path}
        for node in nodes:
            for segment in node['data']['content']:
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
                nodes=self._materialize(row,bundle,settings)
            except (PublishRejected,ValueError,OSError,TypeError,KeyError):
                self.store.cancel_candidate(row['bundle_id'],'publication_check_failed'); continue
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
                receipt=await self.gateway.send_forward(row['group_id'],nodes,before_send=before_send)
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
