"""Task-local atomic content, bounded context access and exact publication manifests."""
from __future__ import annotations

import copy
import json
import secrets

from pullshit.contracts import AgentResult, AgentTask, ContentBundle, MessageRef, digest
from pullshit.content.forwards import component as forward_component


def _atoms(bundle, trigger_refs):
    for message in bundle.messages:
        origin = bundle.origins.get(message.ref.key, message.ref.key)
        if origin != message.ref.key and origin in bundle.forwards:
            continue
        forward = bundle.forwards.get(message.ref.key)
        if forward is not None:
            yield {'key': digest([message.ref.key, -1]), 'message_key': message.ref.key,
                   'origin_key': origin, 'segment_index': -1, 'kind': 'forward',
                   'occurred_at': message.occurred_at, 'sender': message.nickname or message.user_id,
                   'new': origin in trigger_refs,
                   'publishable': not message.own and not message.recalled and not forward['issues'],
                   'content': ['forward', forward['content']], 'component': forward_component(forward),
                   'media_id': None, 'media_ids': forward['media_ids'], 'issues': forward['issues']}
            continue
        for index, segment in enumerate(message.segments):
            kind = segment.get('type')
            data = segment.get('data', {})
            if not isinstance(data,dict):
                data={'text':str(data)}
            media = next((m for m in bundle.media if m.source == message.ref and m.path == data.get('file')), None) if kind == 'image' else None
            publishable = (kind in {'text', 'image'} and not message.own and not message.recalled
                           and not message.ref.message_id.startswith('embedded:')
                           and (bool(str(data.get('text', '')).strip()) if kind == 'text' else bool(media and media.sha256 and media.path)))
            content = (['image', media.sha256] if media else
                       ['text', str(data.get('text', ''))] if kind == 'text' else [kind, data])
            yield {'key': digest([message.ref.key, index]), 'message_key': message.ref.key,
                   'origin_key': origin, 'segment_index': index, 'kind': kind,
                   'occurred_at': message.occurred_at, 'sender': message.nickname or message.user_id,
                   'new': origin in trigger_refs, 'publishable': publishable,
                   'content': content, 'component': digest(content),
                   'media_id': media.reference_id if media else None,
                   'media_ids': [media.reference_id] if media else [],
                   'issues': (['media_unavailable'] if kind == 'image' and not media else
                              ['unsupported_cq'] if kind=='text' and '[CQ:' in str(data.get('text','')) else [])}


def prepare(task: AgentTask, store=None, context_limits=None):
    if task.kind != 'selection' or task.selection:
        return
    bundle = task.bundle
    if not bundle.forwards:
        from pullshit.content.forwards import collect
        collect(bundle)
    triggers = bundle.trigger_refs or [ref.key for ref in bundle.sources]
    task.selection = {'snapshot_id': digest([task.task_id, bundle.bundle_id, triggers, secrets.token_hex(8)]),
                      'revision': 0, 'units': [], 'expansion_calls': 0,
                      'expansion_characters': 0, 'expansion_images': 0, 'expansion_units': 0,
                      'issues': list(bundle.issues), 'context_limits': context_limits or {
                          'expansion_calls':2,'expansion_units':40,'expansion_characters':16000,'expansion_images':4,'nearby_seconds':600}}
    for atom in _atoms(bundle, set(triggers)):
        atom['id'] = f'U{len(task.selection["units"])+1:03d}'
        atom['history'] = store.unit_history(atom['key']) if store else []
        task.selection['units'].append(atom)


def view(task, units=None):
    """Expose atomic content and relationships without cached file paths or full QQ payloads."""
    snapshot = task.selection
    rows = snapshot['units'] if units is None else units
    return {'snapshot_id': snapshot['snapshot_id'], 'snapshot_revision': snapshot['revision'],
            'context_budget': {key: snapshot['context_limits'][key] - snapshot.get(key,0)
                               for key in ('expansion_calls','expansion_units','expansion_characters','expansion_images')},
            'units': [{key: unit[key] for key in ('id', 'message_key', 'origin_key', 'segment_index',
                       'kind', 'occurred_at', 'sender', 'new', 'publishable', 'content', 'media_id', 'media_ids', 'issues', 'history')}
                      for unit in rows], 'context_issues': snapshot['issues']}


def validate(task, result: AgentResult, inspected=None):
    """Return a rejection code; every selected output must be complete and grounded in new input."""
    if task.kind != 'selection':
        return None
    snapshot = task.selection
    if not snapshot:
        return 'missing_selection_snapshot'
    if result.decision != 'eligible':
        return 'selections_without_eligibility' if result.selections else None
    if result.snapshot_id != snapshot['snapshot_id'] or result.snapshot_revision != snapshot['revision']:
        return 'selection_snapshot_mismatch'
    if not result.selections:
        return 'missing_publish_selection'
    known = {unit['id']: unit for unit in snapshot['units']}
    used = set()
    for selection in result.selections:
        ids = selection['publish_ids']
        if any(identity not in known for identity in ids):
            return 'publish_id_out_of_scope'
        if used.intersection(ids):
            return 'overlapping_selections'
        used.update(ids)
        units = [known[identity] for identity in ids]
        if any(not unit['publishable'] or unit['issues'] for unit in units):
            return 'unit_not_publishable'
        if selection['context_status'] != 'complete' or selection.get('risks'):
            return 'selected_context_incomplete'
        if not any(unit['new'] for unit in units):
            return 'historical_content_only'
        if any(not unit['new'] for unit in units) and not selection.get('new_value_reason','').strip():
            return 'missing_new_value_reason'
        if inspected is not None and any(media not in inspected for unit in units for media in unit['media_ids']):
            return 'selected_image_not_inspected'
    return None


def publications(task, result):
    """Build separate immutable candidates from the selected original segments only."""
    problem = validate(task, result, result.inspected_images)
    if problem:
        raise ValueError(problem)
    output = []
    for selection in result.selections:
        selected = set(selection['publish_ids'])
        units = [unit for unit in task.selection['units'] if unit['id'] in selected]
        positions = {(unit['message_key'], unit['segment_index']) for unit in units}
        messages = []
        for message in sorted(task.bundle.messages, key=lambda m: m.occurred_at):
            segments = [copy.deepcopy(segment) for index, segment in enumerate(message.segments)
                        if (message.ref.key, index) in positions or (message.ref.key, -1) in positions]
            if segments:
                item = copy.deepcopy(message)
                item.segments = segments
                messages.append(item)
        media_ids = {identity for unit in units for identity in unit['media_ids']}
        media = [copy.deepcopy(m) for m in task.bundle.media if m.reference_id in media_ids]
        contents = []
        for message in messages:
            if message.ref.key in task.bundle.forwards:
                contents.append(['forward', task.bundle.forwards[message.ref.key]['content']])
                continue
            for index, segment in enumerate(message.segments):
                if segment['type'] == 'text':
                    contents.append(['text', segment['data']['text']])
                else:
                    image = next(m for m in media if m.source == message.ref and m.path == segment['data']['file'])
                    contents.append(['image', image.sha256])
        fingerprint = digest({'selected_content': contents})
        manifest = {'version': 1, 'snapshot_id': result.snapshot_id,
                    'snapshot_revision': result.snapshot_revision,
                    'publish_ids': [unit['id'] for unit in units],
                    'unit_keys': [unit['key'] for unit in units],
                    'components': [unit['component'] for unit in units],
                    'image_components': [unit['component'] for unit in units if unit['kind'] == 'image'],
                    'forward_components': [unit['component'] for unit in units if unit['kind'] == 'forward'],
                    'context_status': selection['context_status'],
                    'completeness_reason': selection['completeness_reason'],
                    'new_value_reason': selection.get('new_value_reason',''),
                    'observation_id': task.selection.get('observation_id', task.bundle.bundle_id)}
        origins = {message.ref.key: task.bundle.origins[message.ref.key] for message in messages
                   if message.ref.key in task.bundle.origins}
        bundle = ContentBundle(digest(['publication', fingerprint, sorted(unit['key'] for unit in units)]),
                               fingerprint, messages, task.bundle.created_at,
                               min(task.bundle.expires_at, *(m.expires_at for m in media)) if media else task.bundle.expires_at,
                               media=media, origins=origins, publication=manifest,
                               forwards={m.ref.key: copy.deepcopy(task.bundle.forwards[m.ref.key]) for m in messages
                                         if m.ref.key in task.bundle.forwards})
        analysis = AgentResult('eligible', selection.get('score', result.score),
                               selection.get('tags', result.tags), reason=selection['reason'],
                               usage=result.usage, inspected_images=[m.reference_id for m in media])
        output.append((bundle, analysis))
    return output


async def expand(task, arguments, settings, store, content, now):
    """Use a shared task budget for anchored local neighbours, replies and review summaries."""
    snapshot = task.selection
    cfg = settings.data['context']
    units = snapshot['units']
    anchor = next((unit for unit in units if unit['id'] == arguments.get('anchor_id')), None)
    if anchor is None or not str(arguments.get('reason', '')).strip():
        return {'error': 'context_anchor_and_reason_required'}
    mode = arguments.get('mode', 'nearby')
    if mode not in {'nearby', 'reply', 'history'}:
        return {'error': 'invalid_context_mode'}
    if snapshot['expansion_calls'] >= cfg['expansion_calls']:
        return {'error': 'context_call_limit'}
    snapshot['expansion_calls'] += 1
    ref = MessageRef(*json.loads(anchor['origin_key']))
    if ref.account_id != settings.account_id or ref.group_id not in settings.sources:
        return {'error': 'context_scope_denied'}
    if mode == 'history':
        read_ids=snapshot.setdefault('history_read_ids',[])
        if anchor['id'] in read_ids:
            return {'already_available':True,'history':[],'snapshot_revision':snapshot['revision']}
        result = {'history': store.unit_history(anchor['key'])}
        size = len(json.dumps(result, ensure_ascii=False))
        if snapshot['expansion_characters'] + size > cfg['expansion_characters']:
            return {'error': 'context_character_limit'}
        snapshot['expansion_characters'] += size
        read_ids.append(anchor['id'])
        return result
    message = next((m for m in task.bundle.messages if m.ref.key == anchor['message_key']), None)
    if message is None:
        return {'error': 'context_anchor_unavailable'}
    if mode == 'reply':
        replies = [s.get('data', {}).get('id') for s in message.segments if s.get('type') == 'reply']
        if len(replies) != 1:
            return {'error': 'no_unique_reply'}
        try:
            refs = [MessageRef.from_dict({'account_id': ref.account_id, 'group_id': ref.group_id, 'message_id': replies[0]})]
        except ValueError:
            return {'error': 'invalid_reply'}
    else:
        neighbours = store.nearby(ref, message.occurred_at, window_seconds=cfg['nearby_seconds'],
                                  limit=cfg['expansion_units'] + 1, now=now)
        refs = [m.ref for m in sorted(neighbours, key=lambda m: m.occurred_at) if not m.own and not m.recalled]
    known_messages = {m.ref.key for m in task.bundle.messages}
    refs = [ref for ref in refs if ref.key not in known_messages]
    if not refs:
        return {'already_available': True, 'snapshot_revision': snapshot['revision'], 'units': []}
    remaining_units=cfg['expansion_units']-snapshot['expansion_units']
    if remaining_units<=0:
        return {'error':'context_unit_limit'}
    extra = await content.build(refs[:remaining_units],media_limit=max(0,cfg['expansion_images']-snapshot['expansion_images']))
    known_keys = {unit['key'] for unit in units}
    additions = [atom for atom in _atoms(extra, set()) if atom['key'] not in known_keys]
    for index, atom in enumerate(additions):
        atom['id'] = f'U{len(units)+index+1:03d}'
        atom['history'] = store.unit_history(atom['key'])
    images = sum(max(int(atom['kind'] == 'image'), len(atom['media_ids'])) for atom in additions)
    probe=copy.copy(task)
    probe.selection={**snapshot,'revision':snapshot['revision']+bool(additions)}
    result=view(probe,additions)
    result['context_issues']=extra.issues
    characters=len(json.dumps(result,ensure_ascii=False))
    max_chars=arguments.get('max_chars')
    if type(max_chars) is int and max_chars>0 and characters>max_chars:
        return {'error':'context_character_limit'}
    if (snapshot['expansion_units'] + len(additions) > cfg['expansion_units']
            or snapshot['expansion_characters'] + characters > cfg['expansion_characters']
            or snapshot['expansion_images'] + images > cfg['expansion_images']):
        return {'error': 'context_expansion_budget'}
    snapshot['expansion_units'] += len(additions)
    snapshot['expansion_characters'] += characters
    snapshot['expansion_images'] += images
    units.extend(additions)
    snapshot['revision'] += bool(additions)
    snapshot['issues'] = list(dict.fromkeys(snapshot['issues'] + extra.issues))
    task.bundle.messages.extend(m for m in extra.messages if m.ref.key not in known_messages)
    known_media = {m.reference_id for m in task.bundle.media}
    task.bundle.media.extend(m for m in extra.media if m.reference_id not in known_media)
    task.bundle.origins.update(extra.origins)
    task.bundle.forwards.update(extra.forwards)
    task.bundle.expires_at = min(task.bundle.expires_at, extra.expires_at)
    result['context_budget']=view(task,[])['context_budget']
    return result
