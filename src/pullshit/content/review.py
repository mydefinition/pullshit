"""Independent, policy-bound review of the exact selected publication."""
from __future__ import annotations

import asyncio
import copy
from dataclasses import asdict
import time

from pullshit.config import agent_config
from pullshit.contracts import AgentTask, AgentResult, digest


def policy(settings):
    config=settings.data['review']
    return {'minimum_score':config['minimum_score'],
            'policies':[copy.deepcopy(p) for p in config['policies'] if p['enabled']]}


def subject(bundle):
    return {'fingerprint':bundle.fingerprint, 'messages':[asdict(m) for m in bundle.messages],
            'media':[asdict(m) for m in bundle.media], 'forwards':bundle.forwards,
            'publication':bundle.publication, 'origins':bundle.origins}


def view(bundle):
    """Give the reviewer complete publishable content, without selection opinions."""
    return {'messages':[{'message_id':m.ref.message_id, 'sender':m.nickname or m.user_id,
                         'occurred_at':m.occurred_at,
                         'content':bundle.forwards[m.ref.key]['content'] if m.ref.key in bundle.forwards else m.segments}
                        for m in bundle.messages],
            'images':[{'reference_id':m.reference_id,'sha256':m.sha256,'message_id':m.source.message_id}
                      for m in bundle.media]}


def check(bundle,settings):
    if not settings.data['review']['enabled']:
        return None
    record=bundle.review
    if not record: return 'review_required'
    if not isinstance(record,dict): return 'review_invalid_record'
    try:
        AgentResult.from_dict({key:record[key] for key in
                               ('decision','score','risks','reason','inspected_images','usage')})
    except (ValueError,TypeError,KeyError):
        return 'review_invalid_record'
    if record.get('policy_digest')!=digest(policy(settings)): return 'review_policy_changed'
    if record.get('subject_digest')!=digest(subject(bundle)): return 'review_content_changed'
    if record.get('decision')!='eligible' or record.get('risks'): return 'review_not_approved'
    if record.get('score',0)<settings.data['review']['minimum_score']: return 'review_uncertain'
    if any(m.reference_id not in record.get('inspected_images',[]) for m in bundle.media):
        return 'review_image_not_inspected'
    if record.get('usage',{}).get('complete') is not True: return 'review_usage_incomplete'
    return None


async def run(bundle,audience,settings,agent,*,clock=time.time):
    """Fail closed on outages, malformed replies, uninspected images and budget errors."""
    rules=policy(settings); cfg=agent_config(settings.data,'review')
    binding={'policy_digest':digest(rules),'subject_digest':digest(subject(bundle))}
    task=AgentTask(digest(['review',bundle.bundle_id,binding,time.time_ns()]),'review',audience[0],
                   min(bundle.expires_at,clock()+cfg['timeout_seconds']),copy.deepcopy(bundle),
                   target_group_ids=list(audience), review={**rules,**binding},
                   limits={'model_calls':cfg['model_calls'],'tool_calls':cfg['tool_calls'],
                           'output_tokens':cfg['max_output_tokens'],'input_characters':cfg['max_input_characters']})
    task.bundle.review={}
    record={**binding,'task_id':task.task_id,'reviewed_at':clock(),
            'decision':'needs_review','score':0,'risks':[],'reason':'review_unavailable',
            'inspected_images':[],'usage':{}}
    try:
        async with asyncio.timeout(max(0,task.deadline-clock())):
            result=await agent.run(task)
        AgentResult.from_dict(asdict(result))
        from pullshit.agent.protocol import normalize_analysis
        normalized,errors=normalize_analysis(asdict(result),'review')
        if normalized is None: raise ValueError('invalid_review_result')
        record.update({key:normalized[key] for key in ('decision','score','risks','reason','evidence_ids')},
                      inspected_images=list(result.inspected_images),usage=result.usage)
        if clock()>task.deadline: record.update(decision='needs_review',reason='review_deadline_expired')
    except Exception:
        pass
    bundle.review=record
    problem=check(bundle,settings)
    if problem and record['decision']=='eligible':
        record.update(decision='needs_review',reason=problem)
    return record
