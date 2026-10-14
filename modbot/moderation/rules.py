"""Rule engine. Code never names a rule; behaviour comes from rule attributes."""

from __future__ import annotations

import hashlib
from dataclasses import dataclass, field, replace
from pathlib import Path
from typing import Protocol

import yaml

from modbot.models import Decision, ModerationResult, Trigger, Verdict

_TRIGGERS = ("auto", "report")


class Flags(Protocol):
    @property
    def dry_run(self) -> bool: ...

    def enforce(self, rule_id: str) -> bool: ...


@dataclass(frozen=True)
class StaticFlags:
    """Simple Flags implementation (tests, and snapshots of the settings table)."""

    dry_run: bool = True
    enforced: frozenset[str] = frozenset()

    def enforce(self, rule_id: str) -> bool:
        return rule_id in self.enforced


@dataclass(frozen=True)
class Rule:
    id: str
    categories: tuple[str, ...]
    delete_at: dict[str, float] = field(default_factory=dict)  # trigger -> cutoff
    escalate_at: float | None = None
    log_at: float | None = None
    delete_if_flagged: bool = False
    ignores_dry_run: bool = False
    ignores_context: bool = False
    redact: bool = False
    ping: bool = False
    default_enforce: bool = False
    suppress_if: dict[str, float] = field(default_factory=dict)

    @classmethod
    def from_dict(cls, d: dict) -> Rule:
        known = {f for f in cls.__dataclass_fields__}
        unknown = set(d) - known
        if unknown:
            raise ValueError(f"rule {d.get('id')!r}: unknown keys {sorted(unknown)}")
        if not d.get("id") or not d.get("categories"):
            raise ValueError(f"rule needs id and categories: {d!r}")
        delete_at = dict(d.get("delete_at") or {})
        bad = set(delete_at) - set(_TRIGGERS)
        if bad:
            raise ValueError(f"rule {d['id']!r}: delete_at keys must be {_TRIGGERS}, got {sorted(bad)}")
        return cls(
            id=d["id"],
            categories=tuple(d["categories"]),
            delete_at={k: float(v) for k, v in delete_at.items()},
            escalate_at=_opt_float(d.get("escalate_at")),
            log_at=_opt_float(d.get("log_at")),
            delete_if_flagged=bool(d.get("delete_if_flagged", False)),
            ignores_dry_run=bool(d.get("ignores_dry_run", False)),
            ignores_context=bool(d.get("ignores_context", False)),
            redact=bool(d.get("redact", False)),
            ping=bool(d.get("ping", False)),
            default_enforce=bool(d.get("default_enforce", False)),
            suppress_if={k: float(v) for k, v in (d.get("suppress_if") or {}).items()},
        )


def _opt_float(v) -> float | None:
    return None if v is None else float(v)


@dataclass(frozen=True)
class RuleSet:
    rules: tuple[Rule, ...]
    version: str  # sha256 of the file contents; no git needed

    @classmethod
    def from_text(cls, text: str) -> RuleSet:
        raw = yaml.safe_load(text) or []
        if not isinstance(raw, list):
            raise ValueError("rules.yaml must be a list of rules")
        rules = tuple(Rule.from_dict(r) for r in raw)
        ids = [r.id for r in rules]
        if len(ids) != len(set(ids)):
            raise ValueError("duplicate rule ids in rules.yaml")
        return cls(rules, hashlib.sha256(text.encode()).hexdigest()[:16])

    @classmethod
    def load(cls, path: str | Path) -> RuleSet:
        return cls.from_text(Path(path).read_text(encoding="utf-8"))

    def get(self, rule_id: str) -> Rule | None:
        return next((r for r in self.rules if r.id == rule_id), None)

    @property
    def categories(self) -> set[str]:
        return {c for r in self.rules for c in r.categories}


_RANK = {Decision.DELETE: 2, Decision.ESCALATE: 1}


def _evaluate_rule(rule: Rule, result: ModerationResult, flags: Flags, key: str) -> Verdict | None:
    """One rule's verdict, or None if it doesn't match at all."""
    cat, score = max(((c, result.scores.get(c, 0.0)) for c in rule.categories), key=lambda t: t[1])
    live = flags.enforce(rule.id) and (not flags.dry_run or rule.ignores_dry_run)

    def verdict(decision: Decision, reason: str, enforced: bool, ping: bool) -> Verdict:
        return Verdict(decision, rule.id, reason, enforced, ping, True, rule.redact)

    if rule.delete_if_flagged and result.flagged.get(cat, False):
        return verdict(Decision.DELETE, f"{cat} flagged by classifier ({score:.2f})", live, rule.ping)
    cutoff = rule.delete_at.get(key)
    suppressed = not rule.ignores_context and any(
        result.context.get(label, 0.0) >= t for label, t in rule.suppress_if.items()
    )
    if cutoff is not None and score >= cutoff and not suppressed:
        return verdict(Decision.DELETE, f"{cat}={score:.2f} >= {key} cutoff {cutoff}", live, rule.ping)
    if rule.log_at is not None and score >= rule.log_at:
        return verdict(Decision.ESCALATE, f"{cat}={score:.2f} (log tier)", False, False)
    if rule.escalate_at is not None and score >= rule.escalate_at:
        reason = f"{cat}={score:.2f}" + (" (context suppressed deletion)" if suppressed else "")
        return verdict(Decision.ESCALATE, reason, False, False)
    return None


def evaluate(rules: RuleSet, result: ModerationResult, flags: Flags, trigger: Trigger) -> Verdict:
    """Pure and cheap: re-run for every trigger against current rules and flags.

    Every rule is evaluated; the most severe outcome wins (live delete, then
    would-delete, then flag), with rule order breaking ties. A weak signal from
    an early rule can't mask a strong one from a later rule. Redaction is sticky:
    if any matching rule redacts, the final verdict does too.
    """
    key = trigger.threshold_key
    matches = [(i, v) for i, rule in enumerate(rules.rules)
               if (v := _evaluate_rule(rule, result, flags, key)) is not None]
    if not matches:
        return Verdict.keep()
    _, best = max(matches, key=lambda iv: (iv[1].decision is Decision.DELETE and iv[1].enforced,
                                           _RANK.get(iv[1].decision, 0), -iv[0]))
    others = [v for _, v in matches if v is not best]
    redact = best.redact or any(v.redact for v in others)
    reason = best.reason
    if others:
        reason += "; also " + ", ".join(f"{v.rule_id} ({v.reason})" for v in others)
    return replace(best, redact=redact, reason=reason)
