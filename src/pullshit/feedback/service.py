"""Learn group preferences from explicit and precisely attributed feedback."""
from __future__ import annotations
from datetime import datetime,timezone
import time
from pullshit.contracts import AgentTask, DAY, MessageRef, digest

class FeedbackService:
    def __init__(self,settings,store,agent=None,*,clock=time.time):
        self.settings,self.store,self.agent,self.clock=settings,store,agent,clock

    def _signal(self,event,delivery,kind,direction,payload,active=True):
        return {'signal_id':digest([event.ref.account_id,event.ref.group_id,event.ref.message_id,event.user_id,kind,payload.get('reaction')]),
                'account_id':event.ref.account_id,'group_id':event.ref.group_id,'bundle_id':delivery['bundle_id'],
                'publication_id':delivery['fingerprint'],'user_id':event.user_id,'kind':kind,'direction':direction,
                'tags':delivery.get('tags',[]),'occurred_at':event.occurred_at,'expires_at':min(event.occurred_at+self.settings.data['retention']['content_days']*DAY,delivery['expires_at']),
                'payload':payload,'active':active}

    async def apply(self,event):
        if event.ref.group_id not in self.settings.targets or event.occurred_at+7*DAY<=self.clock() or event.user_id==event.ref.account_id:
            return None
        if event.kind=='reaction':
            delivery=self.store.delivery_for_message(event.ref)
            if not delivery or delivery['expires_at']<=self.clock(): return None
            signal=self._signal(event,delivery,'reaction','unknown',{'reaction':event.reaction},active=bool(event.added))
            # The platform's target-message identity is stable for reaction add/remove.
            signal['signal_id']=digest([event.ref.key,event.user_id,event.reaction,'reaction'])
            self.store.put_feedback(signal); return signal
        if event.kind!='message' or not event.message: return None
        segments=event.message.segments
        replies=[s.get('data',{}).get('id') for s in segments if s.get('type')=='reply']
        if len(replies)!=1: return None
        target=MessageRef(event.ref.account_id,event.ref.group_id,str(replies[0]))
        delivery=self.store.delivery_for_message(target)
        if not delivery or delivery['expires_at']<=self.clock(): return None
        text=''.join(str(s.get('data',{}).get('text','')) for s in segments if s.get('type')=='text').strip()
        direction={'/喜欢':'positive','/少来点':'negative'}.get(text)
        kind='explicit'
        if direction is None:
            if text.startswith('/') or not text or self.agent is None: return None
            kind='reply'; candidate=self.store.get_candidate(delivery['bundle_id'],now=self.clock())
            if candidate is None: return None
            task=AgentTask(digest(['feedback',event.event_id]),'feedback',event.ref.group_id,
                           self.clock()+self.settings.data['agent']['timeout_seconds'],bundle=candidate['bundle'],
                           preferences=self.store.group_state(event.ref.account_id,event.ref.group_id)['preferences'],
                           feedback={'text':text,'target_message_id':target.message_id,'evidence_id':event.ref.key})
            result=await self.agent.run(task)
            direction=(result.direction or 'unknown') if result.decision=='eligible' and not result.risks else 'unknown'
        signal=self._signal(event,delivery,kind,direction,{'text':text,'target_message_id':target.message_id})
        self.store.put_feedback(signal); return signal

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
