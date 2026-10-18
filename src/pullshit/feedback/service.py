"""Learn group preferences from explicit and precisely attributed feedback."""
from __future__ import annotations
from datetime import datetime,timezone
import time
from pullshit.config import agent_config
from pullshit.contracts import AgentTask, ChatEvent, ChatMessage, DAY, MessageRef, digest

class FeedbackService:
    def __init__(self,settings,store,agent=None,*,content=None,clock=time.time):
        self.settings,self.store,self.agent,self.clock=settings,store,agent,clock
        self.content=content

    @staticmethod
    def addressed_to_bot(message):
        """Accept a top-level platform mention of the message's receiving account."""
        return bool(message and any(segment.get('type')=='at'
                    and isinstance(segment.get('data'),dict)
                    and str(segment['data'].get('qq',''))==message.ref.account_id
                    for segment in message.segments))

    def _signal(self,event,subject,kind,direction,payload,active=True):
        return {'signal_id':digest([event.ref.account_id,event.ref.group_id,event.ref.message_id,event.user_id,kind,payload.get('reaction')]),
                'account_id':event.ref.account_id,'group_id':event.ref.group_id,'bundle_id':subject.get('bundle_id'),
                'publication_id':subject.get('fingerprint'),'user_id':event.user_id,'kind':kind,'direction':direction,
                'tags':subject.get('tags',[]),'occurred_at':event.occurred_at,
                'expires_at':min(event.occurred_at+self.settings.data['retention']['content_days']*DAY,subject.get('expires_at',float('inf'))),
                'payload':payload,'active':active}

    def capture(self,event):
        """Persist a quoted feedback request before asynchronous interpretation."""
        if (event.kind!='message' or not self.addressed_to_bot(event.message) or event.user_id==event.ref.account_id
                or event.ref.group_id not in self.settings.sources|self.settings.targets
                or event.occurred_at+self.settings.data['retention']['content_days']*DAY<=self.clock()):
            return None
        replies=[s.get('data',{}).get('id') for s in event.message.segments if s.get('type')=='reply']
        text=''.join(str(s.get('data',{}).get('text','')) for s in event.message.segments if s.get('type')=='text').strip()
        if len(replies)!=1 or not text or (text.startswith('/') and text not in {'/喜欢','/少来点'}): return None
        try: target=MessageRef.from_dict({'account_id':event.ref.account_id,'group_id':event.ref.group_id,'message_id':replies[0]})
        except (ValueError,TypeError): return None
        kind='explicit' if text in {'/喜欢','/少来点'} else 'reply'
        signal=self._signal(event,{},kind,'unknown',{'text':text,'target_message_id':target.message_id,
                            'feedback_message_id':event.ref.message_id,'event_id':event.event_id,
                            'addressed_to_bot':True,'processing':'pending'})
        self.store.put_feedback(signal)
        return self.store.get_feedback(signal['signal_id'],now=self.clock())

    def pending_events(self,account_id,limit=200):
        rows=self.store.pending_feedback(account_id,self.settings.sources|self.settings.targets,now=self.clock(),limit=limit)
        events=[]
        for row in rows:
            payload=row['payload'];ref=MessageRef(account_id,row['group_id'],payload['feedback_message_id'])
            if payload.get('addressed_to_bot') is not True:
                original=self.store.get_message(ref,now=self.clock())
                if not self.addressed_to_bot(original):
                    self.store.complete_feedback(row['signal_id'],payload={**payload,'processing':'done',
                        'recognized':False,'ordinary_chat':True,'addressed_to_bot':False,
                        'reason':'feedback_requires_bot_mention'},direction='unknown',tags=[],now=self.clock())
                    continue
                payload={**payload,'addressed_to_bot':True}
                self.store.complete_feedback(row['signal_id'],payload=payload,now=self.clock())
            message=ChatMessage(ref,row['user_id'],'',row['occurred_at'],[
                {'type':'reply','data':{'id':payload['target_message_id']}},
                {'type':'at','data':{'qq':account_id}},
                {'type':'text','data':{'text':payload['text']}}],expires_at=row['expires_at'])
            events.append(ChatEvent('message',ref,row['occurred_at'],payload['event_id'],row['user_id'],message))
        return events

    async def apply(self,event):
        if (event.ref.group_id not in self.settings.sources|self.settings.targets
                or event.occurred_at+7*DAY<=self.clock() or event.user_id==event.ref.account_id): return None
        if event.kind=='reaction':
            delivery=self.store.delivery_for_message(event.ref)
            if not delivery or delivery['expires_at']<=self.clock(): return None
            signal=self._signal(event,delivery,'reaction','unknown',{'reaction':event.reaction},active=bool(event.added))
            signal['signal_id']=digest([event.ref.key,event.user_id,event.reaction,'reaction'])
            self.store.put_feedback(signal); return signal
        signal=self.capture(event)
        if signal is None or signal['payload'].get('processing')!='pending': return None
        payload=dict(signal['payload']);target=MessageRef(event.ref.account_id,event.ref.group_id,payload['target_message_id'])
        delivery=self.store.delivery_for_message(target)
        candidate=self.store.get_candidate(delivery['bundle_id'],now=self.clock()) if delivery else None
        bundle=candidate['bundle'] if candidate else None
        if bundle is None:
            from pullshit.content.service import ContentService
            content=self.content or ContentService(self.settings,self.store,clock=self.clock)
            bundle=await content.build(target)
            if target not in bundle.sources or bundle.expires_at<=self.clock() or any(m.recalled for m in bundle.messages):
                payload.update(processing='done',recognized=False,reason='quoted_message_unavailable')
                self.store.complete_feedback(signal['signal_id'],payload=payload,now=self.clock())
                return None
            self.store.put_candidate(bundle)
        subject=delivery or {'bundle_id':bundle.bundle_id,'fingerprint':bundle.fingerprint,'expires_at':bundle.expires_at,'tags':[]}
        tags=list(subject.get('tags',[]))
        if not tags and candidate:
            for analysis in candidate['analysis'].values():
                if isinstance(analysis,dict): tags.extend(analysis.get('tags',[]))
        direction={'/喜欢':'positive','/少来点':'negative'}.get(payload['text'])
        if direction is None or not tags:
            if self.agent is None:
                direction=direction or 'unknown'
            else:
                task=AgentTask(digest(['feedback',signal['signal_id']]),'feedback',event.ref.group_id,
                    self.clock()+agent_config(self.settings.data,'feedback')['timeout_seconds'],bundle=bundle,
                    preferences=self.store.group_state(event.ref.account_id,event.ref.group_id)['preferences'],
                    feedback={'text':payload['text'],'target_message_id':target.message_id,'evidence_id':event.ref.key,'known_tags':tags})
                result=await self.agent.run(task)
                direction=(result.direction or 'unknown') if result.decision=='eligible' and not result.risks else 'unknown'
                if signal['kind']=='explicit': direction={'/喜欢':'positive','/少来点':'negative'}[payload['text']]
                tags=tags or list(result.tags)
                payload.update(reason=result.reason,usage=result.usage)
                payload['ordinary_chat']=signal['kind']=='reply' and result.decision=='rejected' and result.direction in {None,'unknown'}
        payload['quoted_summary']='\n'.join(str(segment.get('data',{}).get('text',''))
            for message in bundle.messages for segment in message.segments if segment.get('type')=='text')[:400]
        payload.update(processing='done',recognized=direction in {'positive','negative'})
        return self.store.complete_feedback(signal['signal_id'],bundle_id=subject['bundle_id'],
                    publication_id=subject['fingerprint'],direction=direction or 'unknown',tags=list(dict.fromkeys(tags)),
                    payload=payload,expires_at=bundle.expires_at,now=self.clock())

    def examples(self,account_id,group_id,limit=6):
        rows=self.store.feedback_since(account_id,group_id,self.clock()-7*DAY,now=self.clock())
        examples=[];seen=set()
        for row in reversed(rows):
            key=(row['publication_id'],row['user_id'])
            if key in seen or row['direction'] not in {'positive','negative'} or row['kind']=='reaction': continue
            seen.add(key)
            examples.append({'direction':row['direction'],'tags':row['tags'],'text':row['payload'].get('text','')[:300],'quoted_summary':row['payload'].get('quoted_summary','')[:400]})
            if len(examples)>=limit: break
        return examples

    def learn(self,account_id,group_id):
        state=self.store.group_state(account_id,group_id); current=self.clock()
        day=datetime.fromtimestamp(current,timezone.utc).date()
        last=state.get('last_learning_at',0)
        if last and datetime.fromtimestamp(last,timezone.utc).date()>=day: return state
        signals=self.store.feedback_since(account_id,group_id,current-7*DAY,now=current)
        # One contribution per member and bundle, with explicit feedback taking precedence.
        votes={}
        for item in sorted(signals,key=lambda s:s['occurred_at']):
            if item['kind']=='reaction' or item['direction'] not in {'positive','negative'}: continue
            key=(item['publication_id'],item['user_id'])
            if key in votes and votes[key]['kind']=='explicit' and item['kind']!='explicit': continue
            votes[key]=item
        preferences=dict(state['preferences']); config=self.settings.data['preferences']; stats={}
        publication_sizes={}
        for item in votes.values(): publication_sizes[item['publication_id']]=publication_sizes.get(item['publication_id'],0)+1
        # Bound each publication's total influence across its members and topics.
        stats={}
        for item in votes.values():
            for tag in set(item['tags']):
                entry=stats.setdefault(tag,{'publications':set(),'members':set(),'sum':0.0,'weight':0.0})
                weight=(1.0 if item['kind']=='explicit' else .5)/max(1,len(set(item['tags'])))/publication_sizes[item['publication_id']]
                entry['publications'].add(item['publication_id']); entry['members'].add(item['user_id'])
                entry['sum']+=weight*(1 if item['direction']=='positive' else -1); entry['weight']+=weight
        changed=False
        for tag,entry in stats.items():
            if len(entry['publications'])<config['min_publications'] or len(entry['members'])<config['min_members']: continue
            old=float(preferences.get(tag,0)); desired=entry['sum']/(2+entry['weight'])
            step=max(-config['max_step'],min(config['max_step'],desired-old))
            preferences[tag]=round(max(-1,min(1,old+step)),6); changed |= bool(step)
        values={'last_learning_at':current}
        if changed:
            values.update(preferences=preferences,version=state['version']+1,snapshots=[{'version':state['version'],'preferences':state['preferences']}])
        self.store.set_group_state(account_id,group_id,values)
        return self.store.group_state(account_id,group_id)

    def rollback(self,account_id,group_id):
        state=self.store.group_state(account_id,group_id)
        if not state['snapshots']: raise ValueError('没有偏好快照')
        previous=state['snapshots'][-1]
        self.store.set_group_state(account_id,group_id,{'preferences':previous['preferences'],'version':state['version']+1,'snapshots':[]})
        return self.store.group_state(account_id,group_id)
