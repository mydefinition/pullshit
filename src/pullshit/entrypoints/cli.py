"""Local operating interface for the content curation service."""
from __future__ import annotations
import argparse
import asyncio
from dataclasses import asdict
from datetime import datetime,timezone
import json
import logging
from logging.handlers import TimedRotatingFileHandler
from pathlib import Path
import sys
from pullshit.config import ConfigError,load_settings
from pullshit.contracts import canonical,platform_id

def parser():
    result=argparse.ArgumentParser(prog='pullshit')
    result.add_argument('--root',type=Path,default=Path.cwd())
    sub=result.add_subparsers(dest='command',required=True)
    for name in ['run','check-config','status','candidates','pause','resume','confirm','retry','reanalyze','cleanup','rollback']:
        command=sub.add_parser(name); command.add_argument('--profile',choices=['development','production'],required=True)
        if name in {'pause','resume','rollback'}:
            command.add_argument('--account',default='*'); command.add_argument('--group',default='*')
        if name=='check-config': command.add_argument('--ready',action='store_true')
        if name=='candidates': command.add_argument('--limit',type=int,default=30)
        if name in {'confirm','retry'}: command.add_argument('delivery_id')
        if name=='confirm': command.add_argument('--message-id',required=True)
        if name=='reanalyze': command.add_argument('bundle_id')
        if name=='cleanup': command.add_argument('--compact',action='store_true')
    replay=sub.add_parser('replay'); replay.add_argument('fixture',type=Path); replay.add_argument('--database',type=Path,required=True)
    replay.add_argument('--account',required=True); replay.add_argument('--source-group',action='append',required=True); replay.add_argument('--target-group',action='append',required=True)
    replay.add_argument('--decisions',type=Path)
    return result

def configure_logging(settings):
    directory=settings.path('logging','directory'); directory.mkdir(parents=True,exist_ok=True,mode=0o700)
    handler=TimedRotatingFileHandler(directory/'service.log',when='midnight',utc=True,backupCount=settings.data['retention']['log_days'],encoding='utf-8')
    formatter=logging.Formatter('%(asctime)sZ %(levelname)s %(message)s'); formatter.converter=__import__('time').gmtime; handler.setFormatter(formatter)
    logger=logging.getLogger('pullshit'); logger.handlers=[handler,logging.StreamHandler(sys.stderr)]; logger.setLevel(settings.data['logging']['level'])
    for name in ['websockets','httpx','httpcore']: logging.getLogger(name).setLevel(logging.ERROR)

async def execute(args):
    if args.command=='replay':
        from pullshit.runtime.replay import replay
        return await replay(args.root,args.fixture,args.database,platform_id(args.account),[platform_id(v) for v in args.source_group],[platform_id(v) for v in args.target_group],args.decisions)
    settings=load_settings(args.root,args.profile)
    if args.command=='check-config':
        if args.ready: settings.validate_live()
        return {'profile':args.profile,'valid':True,'ready':bool(args.ready),'sources':len(settings.sources),'targets':len(settings.targets),'routes':len(settings.routes)}
    if args.command=='run':
        from pullshit.runtime.service import Service
        configure_logging(settings); return await Service(args.root,args.profile).run()
    from pullshit.storage.store import Store
    store=Store(settings.path('storage','database_path'),settings.data['retention'])
    try:
        if args.command=='status':
            return {'profile':args.profile,'state':store.summary(),'global_paused':store.group_state('*','*')['paused'],
                    'budget':store.budget_summary(datetime.now(timezone.utc).date().isoformat()),
                    'deliveries':[{k:v for k,v in row.items() if k in {'delivery_id','group_id','bundle_id','part_index','part_total','status','message_id','error_code'}} for row in store.deliveries()]}
        if args.command=='candidates':
            return [{'bundle_id':row['bundle'].bundle_id,'status':row['status'],'records':len(row['bundle'].messages),'expires_at':row['bundle'].expires_at,'analysis':row['analysis']} for row in store.list_candidates(limit=max(1,min(args.limit,200)))]
        if args.command in {'pause','resume'}:
            if args.account!='*': platform_id(args.account)
            if args.group!='*' and args.group not in settings.targets: raise ValueError('目标群未授权')
            store.set_paused(args.account,args.group,args.command=='pause')
            return {'account':args.account,'group':args.group,'paused':args.command=='pause'}
        if args.command=='confirm':
            receipt=platform_id(args.message_id,True); store.confirm_delivery(args.delivery_id,receipt)
            return {'delivery_id':args.delivery_id,'message_id':receipt,'status':'sent'}
        if args.command=='retry':
            store.retry_delivery(args.delivery_id); return {'delivery_id':args.delivery_id,'status':'queued'}
        if args.command=='reanalyze':
            if store.get_candidate(args.bundle_id) is None: raise ValueError('候选已过期或不存在')
            store.set_candidate_status(args.bundle_id,'collected',analysis={}); return {'bundle_id':args.bundle_id,'status':'collected'}
        if args.command=='rollback':
            from pullshit.feedback.service import FeedbackService
            if args.account=='*' or args.group not in settings.targets: raise ValueError('回滚需要明确账号和授权目标群')
            return FeedbackService(settings,store).rollback(platform_id(args.account),args.group)
        if args.command=='cleanup':
            from pullshit.content.media import MediaCache
            from pullshit.agent.executor import clean_stale_workers
            from pullshit.runtime.maintenance import Maintenance
            return Maintenance(settings,store,MediaCache(settings,store),clean_stale_workers).clean(compact=args.compact)
    finally: store.close()

def main(argv=None):
    args=parser().parse_args(argv)
    try:
        result=asyncio.run(execute(args))
        if result is not None: print(canonical(result))
        return 0
    except KeyboardInterrupt: return 130
    except (ConfigError,ValueError) as exc:
        print(canonical({'error':str(exc)}),file=sys.stderr); return 2
    except Exception:
        print(canonical({'error':'操作失败，请检查本地诊断日志'}),file=sys.stderr); return 1

if __name__=='__main__': raise SystemExit(main())
