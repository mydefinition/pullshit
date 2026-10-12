"""Business contracts shared by the platform, workflows and repositories."""
from __future__ import annotations

from dataclasses import asdict, dataclass, field
import hashlib
import json
import re
import time
from typing import Any, AsyncIterator, Protocol

DAY = 86400

def now() -> float:
    return time.time()

def canonical(value: Any) -> str:
    if hasattr(value, '__dataclass_fields__'):
        value = asdict(value)
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(',', ':'), allow_nan=False)

def digest(value: Any) -> str:
    return hashlib.sha256(canonical(value).encode()).hexdigest()

def platform_id(value: Any, signed: bool = False) -> str:
    if isinstance(value, bool) or not isinstance(value, (str, int)):
        raise ValueError('invalid platform ID')
    text = str(value)
    if not re.fullmatch(r'-?\d+' if signed else r'\d+', text) or len(text)>64:
        raise ValueError('invalid platform ID')
    return text

@dataclass(frozen=True)
class MessageRef:
    account_id: str
    group_id: str
    message_id: str

    @property
    def key(self) -> str:
        return canonical([self.account_id, self.group_id, self.message_id])

    @classmethod
    def from_dict(cls, value: dict) -> MessageRef:
        message_id = value['message_id']
        if not isinstance(message_id, str) or not re.fullmatch(r'embedded:[0-9a-f]+', message_id):
            message_id = platform_id(message_id, True)
        return cls(platform_id(value['account_id']), platform_id(value['group_id']), message_id)

@dataclass
class ChatMessage:
    ref: MessageRef
    user_id: str
    nickname: str
    occurred_at: float
    segments: list[dict]
    own: bool = False
    recalled: bool = False
    expires_at: float = 0

    def __post_init__(self):
        if not self.expires_at:
            self.expires_at = self.occurred_at + 3 * DAY

    @classmethod
    def from_dict(cls, value: dict) -> ChatMessage:
        return cls(**{**value, 'ref': MessageRef.from_dict(value['ref'])})

@dataclass
class ChatEvent:
    kind: str
    ref: MessageRef
    occurred_at: float
    event_id: str
    user_id: str = ''
    message: ChatMessage | None = None
    reaction: str | None = None
    added: bool | None = None

@dataclass
class MediaRef:
    reference_id: str
    source: MessageRef
    path: str | None = None
    sha256: str | None = None
    size: int = 0
    mime: str | None = None
    expires_at: float = 0

    @classmethod
    def from_dict(cls, value: dict) -> MediaRef:
        return cls(**{**value, 'source': MessageRef.from_dict(value['source'])})

@dataclass
class ContentBundle:
    bundle_id: str
    fingerprint: str
    messages: list[ChatMessage]
    created_at: float
    expires_at: float
    media: list[MediaRef] = field(default_factory=list)
    issues: list[str] = field(default_factory=list)

    @property
    def sources(self) -> list[MessageRef]:
        return list(dict.fromkeys(message.ref for message in self.messages if not message.ref.message_id.startswith('embedded:')))

    @classmethod
    def from_dict(cls, value: dict) -> ContentBundle:
        return cls(**{**value, 'messages': [ChatMessage.from_dict(v) for v in value['messages']],
                     'media': [MediaRef.from_dict(v) for v in value.get('media', [])]})

@dataclass
class AgentTask:
    task_id: str
    kind: str
    target_group_id: str
    deadline: float
    bundle: ContentBundle | None = None
    preferences: dict = field(default_factory=dict)
    feedback: dict = field(default_factory=dict)
    limits: dict = field(default_factory=lambda: {'model_calls': 8, 'tool_calls': 8, 'output_tokens': 2048, 'input_characters': 64000})

    @classmethod
    def from_dict(cls, value: dict) -> AgentTask:
        return cls(**{**value, 'bundle': ContentBundle.from_dict(value['bundle']) if value.get('bundle') else None})

@dataclass
class AgentResult:
    decision: str
    score: float = 0
    tags: list[str] = field(default_factory=list)
    risks: list[str] = field(default_factory=list)
    evidence_ids: list[str] = field(default_factory=list)
    reason: str = ''
    usage: dict = field(default_factory=dict)
    inspected_images: list[str] = field(default_factory=list)
    direction: str | None = None

    @classmethod
    def from_dict(cls, value: dict) -> AgentResult:
        result = cls(**value)
        if result.decision not in {'eligible', 'rejected', 'needs_review'}:
            raise ValueError('invalid decision')
        if isinstance(result.score, bool) or not isinstance(result.score, (int, float)) or not 0 <= result.score <= 1:
            raise ValueError('invalid score')
        for key in ('tags', 'risks', 'evidence_ids', 'inspected_images'):
            entries = getattr(result, key)
            if not isinstance(entries, list) or len(entries) > 1000 or any(not isinstance(v, str) or len(v) > 512 for v in entries):
                raise ValueError('invalid result list')
        if not isinstance(result.reason, str) or len(result.reason) > 4096 or not isinstance(result.usage, dict):
            raise ValueError('invalid result')
        if result.direction not in {None, 'positive', 'negative', 'unknown'}:
            raise ValueError('invalid feedback direction')
        return result

class ChatGateway(Protocol):
    account_id: str
    def events(self) -> AsyncIterator[ChatEvent]: ...
    async def fetch_message(self, ref: MessageRef) -> ChatMessage | None: ...
    async def fetch_forward(self, ref: MessageRef, forward_id: str) -> list[dict]: ...
    async def send_forward(self, group_id: str, nodes: list[dict], *, before_send=None) -> str: ...

class AgentExecutor(Protocol):
    async def run(self, task: AgentTask) -> AgentResult: ...
    async def cancel(self, task_id: str) -> None: ...

class PlatformRejected(RuntimeError):
    """The platform explicitly rejected the operation."""

class OutcomeUnknown(RuntimeError):
    """An operation's platform outcome cannot be established."""
