"""Whole merged-forward evidence and immutable publication identity."""
from pullshit.contracts import digest


def collect(bundle):
    """Keep expanded records readable inside one atom; only the original is sent."""
    for outer in bundle.messages:
        if outer.ref.key in bundle.origins or not any(s.get('type') in {'forward', 'node'} for s in outer.segments):
            continue
        members = [outer] + [m for m in bundle.messages if bundle.origins.get(m.ref.key) == outer.ref.key]
        keys = {m.ref.key for m in members}
        media = [m for m in bundle.media if m.source.key in keys]
        issues = list(bundle.issues)
        records = []
        for message in members:
            segments = []
            for segment in message.segments:
                kind, data = segment.get('type'), segment.get('data', {})
                if kind == 'image':
                    image = next((m for m in media if m.source == message.ref and m.path == data.get('file')), None)
                    if image is None:
                        issues.append('media_unavailable')
                    segments.append(['image', image.sha256 if image else None])
                elif kind in {'forward', 'node'}:
                    segments.append([kind])
                else:
                    segments.append([kind, data])
                    if kind not in {'text', 'at', 'face', 'reply'}:
                        issues.append('unsupported_forward_content')
            if message is not outer:
                records.append({'sender': message.nickname or message.user_id, 'user_id': message.user_id,
                                'segments': segments})
            else:
                records.append({'segments': segments})
        if len(members) < 2 or not any(row['segments'] for row in records[1:]):
            issues.append('forward_unavailable')
        # Native forwarding copies the whole outer message, including any siblings.
        if any(s.get('type') not in {'forward', 'node', 'text', 'image'} for s in outer.segments):
            issues.append('unsupported_forward_outer_segment')
        bundle.forwards[outer.ref.key] = {
            'content': records, 'media_ids': [m.reference_id for m in media],
            'issues': list(dict.fromkeys(issues)), 'segments_digest': digest(outer.segments)}


def component(forward):
    return digest(['forward', forward['content']])
