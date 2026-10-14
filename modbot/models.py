"""Plain data types shared by every module. No discord.py imports here."""

from __future__ import annotations

import hashlib
from dataclasses import dataclass, field
from enum import StrEnum


class Decision(StrEnum):
    DELETE = "DELETE"
    KEEP = "KEEP"
    ESCALATE = "ESCALATE"  # post to #mod-logs and keep; no moderator action required
    ERROR = "ERROR"


class Trigger(StrEnum):
    AUTO = "auto"
    REPORT = "report"
    RECHECK = "recheck"  # evaluated with report thresholds

    @property
    def threshold_key(self) -> str:
        return "auto" if self is Trigger.AUTO else "report"


def content_hash(content: str) -> str:
    return hashlib.sha256(content.encode("utf-8")).hexdigest()


@dataclass(frozen=True)
class ModerationInput:
    message_id: int
    channel_id: int
    author_id: int
    content: str
    parent_content: str | None = None
    recent_author: tuple[str, ...] = ()  # Phase 2: author's last <=2 messages within 5 min

    @property
    def hash(self) -> str:
        return content_hash(self.content)


@dataclass(frozen=True)
class ModerationResult:
    source: str  # "OPENAI_MODERATION" | "LOCAL_ML"
    model_version: str
    scores: dict[str, float]  # raw scores; NOT calibrated probabilities
    flagged: dict[str, bool]
    context: dict[str, float] = field(default_factory=dict)  # Phase 2 context labels


@dataclass(frozen=True)
class Verdict:
    decision: Decision
    rule_id: str | None
    reason: str
    enforced: bool  # False = would-have-acted (dry-run or rule not enforced)
    ping: bool  # ping the moderator role in #mod-logs
    post: bool  # post to #mod-logs at all (False = database row only)
    redact: bool = False  # never store or post the message text

    @classmethod
    def keep(cls) -> Verdict:
        return cls(Decision.KEEP, None, "below all thresholds", False, False, False)

    @classmethod
    def error(cls, reason: str) -> Verdict:
        return cls(Decision.ERROR, None, reason, False, False, False)
