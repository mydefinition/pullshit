"""Validated JSON configuration and isolated environment paths."""
from __future__ import annotations
from dataclasses import dataclass
import copy
import json
import os
from pathlib import Path
from urllib.parse import urlsplit
from pullshit.contracts import platform_id

class ConfigError(ValueError):
    pass

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
    @property
    def routes(self): return {(r['source_group_id'],r['target_group_id']) for r in self.data['authorization']['routes']}
    def with_account(self,account_id): return Settings(self.root,self.profile,self.data,platform_id(account_id))
    def path(self,section,key): return self.root / self.data[section][key]
    def token(self): return os.environ.get(self.data['napcat']['access_token_env'],'')
    def permits(self,source,target): return (source,target) in self.routes
    def validate_live(self):
        nc=self.data['napcat']; agent=self.data['agent']
        if not self.sources or not self.targets or not self.routes: raise ConfigError('自动运行需要配置来源、目标和路由')
        parsed=urlsplit(nc['endpoint'])
        if parsed.scheme not in {'ws','wss'} or not parsed.hostname or parsed.username or parsed.password or parsed.query or parsed.fragment:
            raise ConfigError('NapCat 地址无效')
        if not self.token(): raise ConfigError('缺少 NapCat token 环境变量')
        if not agent['model'] or not agent['provider'] or not agent['base_url'] or not os.environ.get(agent['api_key_env']):
            raise ConfigError('缺少 Hermes 模型连接配置')
        if agent['daily_budget_usd'] <= 0 or agent['task_budget_usd'] <= 0:
            raise ConfigError('必须显式配置模型预算')
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
    routes=value['authorization']['routes']
    if any(not isinstance(r,dict) or set(r) != {'source_group_id','target_group_id'} or r['source_group_id'] not in settings.sources or r['target_group_id'] not in settings.targets for r in routes):
        raise ConfigError('路由范围无效')
    if len(settings.routes) != len(routes): raise ConfigError('重复路由')
    if value['runtime']['mode'] not in {'auto','shadow'}: raise ConfigError('运行模式必须为 auto 或 shadow')
    for section in ('content','context','media','limits','retention'):
        for key,item in value[section].items():
            if isinstance(item,(int,float)) and (isinstance(item,bool) or item <= 0): raise ConfigError(f'配置必须为正数: {section}.{key}')
    for key in ('concurrency','timeout_seconds','model_calls','tool_calls','max_output_tokens','max_input_characters'):
        if value['agent'][key] <= 0: raise ConfigError(f'配置必须为正数: agent.{key}')
    for key in ('daily_budget_usd','task_budget_usd','input_usd_per_million','output_usd_per_million'):
        if value['agent'][key] < 0: raise ConfigError('模型预算与单价不能为负')
    if not 0 <= value['agent']['minimum_score'] <= 1: raise ConfigError('选材分数范围无效')
    if value['retention']['message_days'] > 7 or value['retention']['content_days'] > 7 or value['retention']['log_days'] > 7:
        raise ConfigError('内容和日志保留期限最多 7 天')
    if value['limits']['max_forward_nodes'] > 100: raise ConfigError('单次合并转发最多 100 个节点')
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
