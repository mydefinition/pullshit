"""pullshit agent 子进程协议与结构化结论校验。

executor（父进程）与 hermes_worker（子进程）通过子进程 stdin/stdout 上的 JSON
Lines 通信；本模块是唯一的线上契约与结论 schema 校验实现，双方共同引用。
"""
from __future__ import annotations

from pullshit.contracts import AgentResult

PROTOCOL_VERSION = 1
TOOLSET_NAME = 'pullshit'
TOOL_NAMES = ('get_context', 'inspect_image', 'get_group_preferences', 'submit_analysis')
LLM_REQUEST_MIDDLEWARE = 'llm_request'
LLM_EXECUTION_MIDDLEWARE = 'llm_execution'
MAX_PROTOCOL_LINE = 1 << 20

DECISIONS = ('eligible', 'rejected', 'needs_review')
DIRECTIONS = ('positive', 'negative', 'unknown')

_RESULT_FIELDS = ('decision', 'score', 'tags', 'risks', 'evidence_ids', 'reason', 'direction',
                  'selections', 'snapshot_id', 'snapshot_revision')


def normalize_analysis(raw, kind: str) -> tuple[dict | None, list[str]]:
    """校验并归一化模型输出的结构化结论。

    返回 (字段 dict, 错误码列表)。字段 dict 只含契约内字段，usage 与
    inspected_images 由调用方另行校验。feedback 任务必须给出 direction。
    """
    if not isinstance(raw, dict):
        return None, ['schema_object']
    value = {key: raw.get(key) for key in _RESULT_FIELDS if key in raw}
    value.setdefault('tags', [])
    value.setdefault('risks', [])
    value.setdefault('evidence_ids', [])
    value.setdefault('score', 0)
    value.setdefault('reason', '')
    try:
        result = AgentResult.from_dict(value)
    except (ValueError, TypeError):
        return None, ['schema_fields']
    errors: list[str] = []
    if not result.reason.strip():
        errors.append('missing_reason')
    if result.decision == 'eligible' and result.risks:
        errors.append('eligible_with_blocking_risks')
    if kind == 'feedback':
        if result.direction not in DIRECTIONS:
            errors.append('schema_direction')
        if result.selections:
            errors.append('feedback_with_selections')
    else:
        result.direction = None
    if errors:
        return None, errors
    return {
        'decision': result.decision, 'score': result.score, 'tags': list(result.tags),
        'risks': list(result.risks), 'evidence_ids': list(result.evidence_ids),
        'reason': result.reason, 'direction': result.direction,
        'selections': result.selections, 'snapshot_id': result.snapshot_id,
        'snapshot_revision': result.snapshot_revision,
    }, []
