"""Hermes 插件：向上游 Hermes（固定 commit 7cc3fb8）暴露 pullshit 项目工具集。

通过标准插件加载（HERMES_HOME/plugins/pullshit 下 plugin.yaml + register(ctx)）
注册唯一工具集 pullshit 与 llm_request/llm_execution 中间件；不改动也不 fork
上游核心。工具处理器经协议桥同步调用父进程，父进程按任务范围授权；模型请求
由 llm_execution 中间件在执行前后经预算协议预留与结算，预算被拒时中止本次
请求，禁止未计量的 provider 调用。
"""
from __future__ import annotations

import json

from pullshit.agent import protocol

_bridge = None
inspected_images: list[str] = []
last_submission: dict | None = None
pending_usage_id: str | None = None

_STRING = {'type': 'string'}


def _schema(name: str, description: str, properties: dict, required: list) -> dict:
    return {
        'name': name, 'description': description,
        'parameters': {'type': 'object', 'properties': properties,
                       'required': required, 'additionalProperties': False},
    }


def bind(bridge) -> None:
    global _bridge
    _bridge = bridge


def _call(name: str, arguments: dict) -> str:
    bridge = _bridge
    if bridge is None:
        raise RuntimeError('pullshit 协议桥未绑定，拒绝工具调用')
    result = bridge.tool_call(name, arguments or {})
    if name == 'inspect_image' and isinstance(result, dict) and result.get('ok'):
        for key in ('reference_id', 'path'):
            if result.get(key):
                inspected_images.append(str(result[key]))
    if name == 'submit_analysis' and isinstance(result, dict) and result.get('accepted') \
            and isinstance(arguments.get('analysis'), dict):
        global last_submission
        last_submission = arguments['analysis']
    return json.dumps(result, ensure_ascii=False, allow_nan=False)


def get_context(arguments: dict) -> str:
    return _call('get_context', arguments)


def inspect_image(arguments: dict) -> str:
    return _call('inspect_image', arguments)


def get_group_preferences(arguments: dict) -> str:
    return _call('get_group_preferences', arguments)


def submit_analysis(arguments: dict) -> str:
    return _call('submit_analysis', arguments)


def _estimate(request) -> dict:
    """按请求体积与输出上限估算一次模型调用的 token 与美元金额。"""
    bridge = _bridge
    cfg = getattr(bridge, 'config', {}) if bridge is not None else {}
    max_tokens = int(cfg.get('max_output_tokens', 2048) or 2048)
    if isinstance(request, dict):
        try:
            max_tokens = int(request.get('max_tokens') or max_tokens)
        except (TypeError, ValueError):
            pass
        try:
            text = json.dumps(request, ensure_ascii=False, default=str)
        except (TypeError, ValueError):
            text = json.dumps({'estimate': 1})
        input_tokens = max(1, len(text.encode('utf-8')))
    else:
        input_tokens = 512
    input_price = float(cfg.get('input_usd_per_million', 0.0) or 0.0)
    output_price = float(cfg.get('output_usd_per_million', 0.0) or 0.0)
    estimate_usd = (input_tokens * input_price + max_tokens * output_price) / 1e6
    return {'input_tokens': input_tokens, 'output_tokens': max_tokens, 'estimate_usd': estimate_usd}


def _usage_of(result) -> dict:
    """从 provider 响应提取 usage；无法得到完整 token 数时标记不完整。"""
    usage = getattr(result, 'usage', None)
    tokens = {
        'input': getattr(usage, 'prompt_tokens', None),
        'output': getattr(usage, 'completion_tokens', None),
        'cache_read': getattr(usage, 'cache_read_input_tokens', None),
        'cache_write': getattr(usage, 'cache_creation_input_tokens', None),
    }
    complete = isinstance(tokens['input'], int) and isinstance(tokens['output'], int)
    cost = getattr(result, 'cost', None)
    if not isinstance(cost, (int, float)):
        hidden = getattr(usage, '_hidden_params', None) if usage is not None else None
        cost = getattr(hidden, 'response_cost', None) if hidden is not None else None
    if not isinstance(cost, (int, float)):
        bridge = _bridge
        cfg = getattr(bridge, 'config', {}) if bridge is not None else {}
        if complete:
            cost = ((tokens['input'] * float(cfg.get('input_usd_per_million', 0.0) or 0.0)
                     + tokens['output'] * float(cfg.get('output_usd_per_million', 0.0) or 0.0)) / 1e6)
    return {'tokens': tokens, 'cost_usd': float(cost) if isinstance(cost, (int, float)) else 0.0,
            'complete': complete}


def _metered_llm_execution(**context) -> object:
    """llm_execution 中间件：请求前预留预算，完成后结算实际用量。

    Hermes 的中间件链在回调抛异常时会跳过该回调继续执行 provider，因此预算被拒
    时采用不调用 next_call、直接返回 None 的方式中止本次 provider 调用；Hermes
    对无效响应的重试有界，最终以失败回合结束。
    """
    request = context.get('request')
    next_call = context.get('next_call')
    bridge = _bridge
    if not callable(next_call) or bridge is None:
        return None
    if len(json.dumps(request, ensure_ascii=False, default=str)) > int(bridge.config.get('max_input_characters', 64000)):
        return None
    bridge.model_calls += 1
    estimate = _estimate(request)
    try:
        decision = bridge.reserve(estimate['estimate_usd'], estimate['input_tokens'],
                                  estimate['output_tokens'])
    except Exception:
        # 上游中间件链会吞掉 next_call 之前的异常，因此桥故障时同样返回 None 中止调用
        return None
    if not decision.get('ok'):
        return None
    global pending_usage_id
    pending_usage_id = decision.get('id')
    try:
        result = next_call()
    except BaseException:
        bridge.report_usage(decision.get('id', ''), 0.0, complete=False, tokens={})
        raise
    usage = _usage_of(result)
    bridge.report_usage(decision.get('id', ''), usage['cost_usd'], usage['complete'],
                        usage['tokens'])
    if usage['complete']:
        pending_usage_id = None
    return result


def _post_api_usage(*, usage=None, **kwargs):
    """Settle streamed responses using Hermes' normalized per-request usage hook."""
    global pending_usage_id
    if pending_usage_id is None or not isinstance(usage, dict):
        return
    inputs, outputs = usage.get('input_tokens'), usage.get('output_tokens')
    if type(inputs) is not int or type(outputs) is not int:
        return
    config = _bridge.config
    amount = (inputs * float(config.get('input_usd_per_million', 0)) + outputs * float(config.get('output_usd_per_million', 0))) / 1e6
    _bridge.report_usage(pending_usage_id, amount, True, {'input': inputs, 'output': outputs})
    pending_usage_id = None


def _strip_sdk_retries(**context) -> dict:
    """llm_request 中间件：清零已知的 SDK 重试参数，防止未计量的自动重试。"""
    request = context.get('request')
    if not isinstance(request, dict):
        return {}
    changes = {}
    for key in ('num_retries', 'max_retries', 'retries'):
        if request.get(key):
            changes[key] = 0
    if request.get('retry_strategy'):
        changes['retry_strategy'] = None
    if not changes:
        return {}
    return {'request': {**request, **changes}}


def register(ctx) -> None:
    """标准 Hermes 插件入口：只注册 pullshit 工具集与预算中间件。"""
    ctx.register_tool(
        name='get_context', toolset=protocol.TOOLSET_NAME,
        schema=_schema('get_context', '选材时以已有原子编号为锚点有限补充上下文：reply 获取引用，nearby 获取附近消息，history 获取处理摘要；填写缺失信息 reason。反馈任务可按原消息引用读取。',
                       {'anchor_id':_STRING,'mode':{'type':'string','enum':['reply','nearby','history']},'reason':_STRING,
                        'group_id': _STRING, 'message_id': _STRING, 'max_chars': {'type': 'integer'}}, []),
        handler=get_context)
    ctx.register_tool(
        name='inspect_image', toolset=protocol.TOOLSET_NAME,
        schema=_schema('inspect_image', '检查素材内容包中的一张图片媒体；只能引用任务素材中的媒体。',
                       {'reference_id': _STRING, 'path': _STRING, 'question': _STRING}, []),
        handler=inspect_image)
    ctx.register_tool(
        name='get_group_preferences', toolset=protocol.TOOLSET_NAME,
        schema=_schema('get_group_preferences', '读取目标群的口味偏好信号。',
                       {'group_id': _STRING}, ['group_id']),
        handler=get_group_preferences)
    ctx.register_tool(
        name='submit_analysis', toolset=protocol.TOOLSET_NAME,
        schema=_schema('submit_analysis', '提交最终结构化结论；字段必须符合要求，提交后由父进程校验并记录。',
                       {'analysis': {'type': 'object'}}, ['analysis']),
        handler=submit_analysis)
    ctx.register_middleware(protocol.LLM_EXECUTION_MIDDLEWARE, _metered_llm_execution)
    ctx.register_middleware(protocol.LLM_REQUEST_MIDDLEWARE, _strip_sdk_retries)
    ctx.register_hook('post_api_request', _post_api_usage)
