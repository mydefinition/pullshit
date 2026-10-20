"""Validated JSON configuration and isolated environment paths."""
from __future__ import annotations
from dataclasses import dataclass
import copy
import json
import math
import os
from pathlib import Path
from urllib.parse import urlsplit
from pullshit.contracts import platform_id

class ConfigError(ValueError):
    pass

def agent_config(data, kind):
    """Resolve one task's connector while sharing execution and expense limits."""
    base=data['agent']
    result={key:copy.deepcopy(value) for key,value in base.items() if key not in {'selection','feedback','review'}}
    role=base.get(kind,{})
    for key,value in role.items():
        if key in {'api_format','free'} or value:
            result[key]=value
    result.setdefault('api_format','llm')
    result.setdefault('free',False)
    if kind=='review':
        result['minimum_score']=data['review']['minimum_score']
    return result

def merge(base, overlay):
    result=copy.deepcopy(base)
    for key,value in overlay.items():
        if key not in base:
            raise ConfigError(f'未知配置字段: {key}')
        if isinstance(base[key],dict):
            if not isinstance(value,dict): raise ConfigError(f'配置类型错误: {key}')
            result[key]=merge(base[key],value)
        elif isinstance(base[key],list):
            if not isinstance(value,list): raise ConfigError(f'配置类型错误: {key}')
            result[key]=value
        else:
            if type(value) is not type(base[key]) and not (type(base[key]) is float and type(value) is int):
                raise ConfigError(f'配置类型错误: {key}')
            result[key]=value
    return result

def read_json(path):
    def pairs(items):
        result={}
        for key,value in items:
            if key in result: raise ConfigError('配置包含重复字段')
            result[key]=value
        return result
    try:
        value=json.loads(Path(path).read_text(), object_pairs_hook=pairs,parse_constant=lambda _: (_ for _ in ()).throw(ConfigError('非法数字')))
    except (OSError,ValueError) as exc:
        raise ConfigError('无法读取有效配置') from exc
    if not isinstance(value,dict): raise ConfigError('配置必须是对象')
    return value

@dataclass(frozen=True)
class Settings:
    root: Path
    profile: str
    data: dict
    account_id: str = ''
    @property
    def sources(self): return set(self.data['authorization']['source_group_ids'])
    @property
    def targets(self): return set(self.data['authorization']['target_group_ids'])
    @property
    def operators(self): return set(self.data['authorization']['operator_qq_ids'])
    def with_account(self,account_id): return Settings(self.root,self.profile,self.data,platform_id(account_id))
    def path(self,section,key): return self.root / self.data[section][key]
    def token(self): return os.environ.get(self.data['napcat']['access_token_env'],'')
    def permits(self,source,target): return source in self.sources and target in self.targets and source != target
    def broadcast_targets(self,bundle):
        return self.targets - {ref.group_id for ref in bundle.sources}
    def validate_live(self):
        nc=self.data['napcat']; agent=self.data['agent']
        if not self.sources or not self.targets: raise ConfigError('自动运行需要配置来源和目标')
        parsed=urlsplit(nc['endpoint'])
        if parsed.scheme not in {'ws','wss'} or not parsed.hostname or parsed.username or parsed.password or parsed.query or parsed.fragment:
            raise ConfigError('NapCat 地址无效')
        if not self.token(): raise ConfigError('缺少 NapCat token 环境变量')
        for kind in ('selection','feedback',*(['review'] if self.data['review']['enabled'] else [])):
            connector=agent_config(self.data,kind)
            if not connector['model'] or not connector['base_url'] or not os.environ.get(connector['api_key_env']):
                raise ConfigError(f'缺少 {kind} 模型连接配置')
            parsed=urlsplit(connector['base_url'])
            if parsed.scheme not in {'http','https'} or not parsed.hostname or parsed.username or parsed.password or parsed.query or parsed.fragment:
                raise ConfigError(f'{kind} 模型地址无效')
            if connector['api_format']=='llm' and not connector['provider']:
                raise ConfigError(f'缺少 {kind} provider')
        if agent['daily_budget_usd'] <= 0 or agent['task_budget_usd'] <= 0:
            raise ConfigError('必须显式配置模型预算')
        if agent['input_usd_per_million']<=0 or agent['output_usd_per_million']<=0:
            raise ConfigError('必须显式配置模型单价')
        if agent['vision_model'] and (not agent['vision_base_url'] or not os.environ.get(agent['vision_api_key_env']) or agent['vision_input_usd_per_million']<=0 or agent['vision_output_usd_per_million']<=0):
            raise ConfigError('视觉模型需要连接、密钥和独立单价')
        if not Path(agent['python']).is_file(): raise ConfigError('Hermes Python 解释器不存在')


def load_settings(root: Path,profile: str) -> Settings:
    root=Path(root).resolve()
    if profile not in {'development','production'}: raise ConfigError('环境必须为 development 或 production')
    defaults=read_json(root/'config/defaults.json')
    value=merge(defaults,read_json(root/f'config/local/{profile}.json'))
    for key in ('source_group_ids','target_group_ids','operator_qq_ids'):
        ids=value['authorization'][key]
        if any(not isinstance(v,str) for v in ids) or len(set(ids)) != len(ids): raise ConfigError('群和操作者 ID 必须为唯一字符串')
        try:
            for item in ids: platform_id(item)
        except ValueError as exc: raise ConfigError('无效授权 ID') from exc
    settings=Settings(root,profile,value)
    if value['runtime']['mode'] not in {'auto','shadow'}: raise ConfigError('运行模式必须为 auto 或 shadow')
    for section in ('runtime','napcat','content','context','media','limits','retention'):
        for key,item in value[section].items():
            if isinstance(item,(int,float)) and (isinstance(item,bool) or not math.isfinite(item) or item <= 0): raise ConfigError(f'配置必须为正数: {section}.{key}')
    for key in ('concurrency','timeout_seconds','model_calls','tool_calls','max_output_tokens','max_input_characters'):
        if value['agent'][key] <= 0: raise ConfigError(f'配置必须为正数: agent.{key}')
    for key in ('daily_budget_usd','task_budget_usd','input_usd_per_million','output_usd_per_million','vision_input_usd_per_million','vision_output_usd_per_million'):
        if not math.isfinite(value['agent'][key]) or value['agent'][key] < 0: raise ConfigError('模型预算与单价不能为负')
    if not 0 <= value['agent']['minimum_score'] <= 1: raise ConfigError('选材分数范围无效')
    for kind in ('selection','feedback','review'):
        role=value['agent'].get(kind,{})
        if role.get('api_format','llm') not in ({'llm','systemone'} if kind=='selection' else {'llm'}):
            raise ConfigError(f'{kind} API 格式无效；反馈仅支持 llm')
        if role.get('free') and (kind!='selection' or role.get('api_format')!='systemone'):
            raise ConfigError('free 仅用于自托管 selection systemone')
        for key in ('timeout_seconds','max_input_characters'):
            if role.get(key,0)<0: raise ConfigError(f'{kind}.{key} 不能为负')
    decision=value['agent'].get('decision',{})
    review=value['review']
    if (isinstance(review['minimum_score'],bool) or not math.isfinite(review['minimum_score'])
            or not 0<review['minimum_score']<=1):
        raise ConfigError('审查置信度必须在 (0,1]')
    identities=set()
    if len(review['policies'])>20: raise ConfigError('审查策略最多 20 条')
    for policy in review['policies']:
        if (not isinstance(policy,dict) or set(policy)!={'id','enabled','instructions'}
                or not isinstance(policy['id'],str) or not policy['id'].isascii()
                or not policy['id'].replace('_','').replace('-','').isalnum() or len(policy['id'])>64
                or policy['id'] in identities or type(policy['enabled']) is not bool
                or not isinstance(policy['instructions'],str) or not policy['instructions'].strip()
                or len(policy['instructions'])>8000):
            raise ConfigError('审查策略需要唯一 id、enabled 和非空 instructions')
        identities.add(policy['id'])
    if review['enabled'] and not any(p['enabled'] for p in review['policies']):
        raise ConfigError('启用审查时至少配置一条有效策略')
    if decision.get('think_mode','') not in {'','never','auto','always'}: raise ConfigError('决策思考模式无效')
    if not 1<=decision.get('max_units',16)<=24 or not 1<=decision.get('think_budget',512)<=4096:
        raise ConfigError('决策单元或思考预算范围无效')
    if value['retention']['message_days'] > 7 or value['retention']['content_days'] > 7 or value['retention']['log_days'] > 7:
        raise ConfigError('内容和日志保留期限最多 7 天')
    if value['limits']['max_message_segments'] > 100: raise ConfigError('单条普通消息最多 100 个消息段')
    if value['preferences']['max_step'] > .05 or value['preferences']['max_step'] <= 0:
        raise ConfigError('偏好调整步长必须在 (0,0.05]')
    for section,fields in {'storage':['database_path','media_directory','worker_directory'],'logging':['directory']}.items():
        for field in fields:
            prefix='logs' if section == 'logging' else 'data'
            defaults_path={'database_path':'state.sqlite3','media_directory':'media','worker_directory':'workers','directory':''}[field]
            raw=value[section][field] or str(Path(prefix)/profile/defaults_path)
            destination=(root/raw).resolve(); required=(root/prefix/profile).resolve()
            if not destination.is_relative_to(required) or required != root/prefix/profile:
                raise ConfigError('环境存储路径必须独立且位于对应目录')
            value[section][field]=str(destination.relative_to(root))
    return settings
