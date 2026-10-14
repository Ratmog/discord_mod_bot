import itertools

import pytest

from modbot.models import Decision, ModerationResult, Trigger
from modbot.moderation.rules import RuleSet, StaticFlags, evaluate

ALL_ON = StaticFlags(dry_run=False, enforced=frozenset({"CHILD_SAFETY", "HATE_THREAT", "THREAT", "VIOLENT_CONTENT"}))


def res(scores=None, flagged=None, context=None):
    return ModerationResult("T", "v", scores or {}, flagged or {}, context or {})


def test_shipped_rules_load(rules):
    assert [r.id for r in rules.rules] == ["CHILD_SAFETY", "HATE_THREAT", "THREAT", "VIOLENT_CONTENT"]
    assert len(rules.version) == 16


def test_version_changes_with_content():
    a = RuleSet.from_text("- {id: A, categories: [x]}\n")
    b = RuleSet.from_text("- {id: A, categories: [x], escalate_at: 0.5}\n")
    assert a.version != b.version


@pytest.mark.parametrize("text", [
    "- {id: A, categories: [x], delete_at: {sometimes: 0.5}}",
    "- {id: A, categories: [x], typo_key: 1}",
    "- {id: A, categories: [x]}\n- {id: A, categories: [y]}",
    "- {categories: [x]}",
])
def test_bad_rules_rejected(text):
    with pytest.raises(ValueError):
        RuleSet.from_text(text)


@pytest.mark.parametrize("score,trigger,expected", [
    (0.84, Trigger.AUTO, Decision.ESCALATE),     # just below auto cutoff
    (0.85, Trigger.AUTO, Decision.DELETE),       # at auto cutoff
    (0.64, Trigger.REPORT, Decision.ESCALATE),   # just below report cutoff
    (0.65, Trigger.REPORT, Decision.DELETE),     # at report cutoff
    (0.70, Trigger.AUTO, Decision.ESCALATE),     # between cutoffs: kept automatically...
    (0.70, Trigger.REPORT, Decision.DELETE),     # ...deleted when reported
    (0.70, Trigger.RECHECK, Decision.DELETE),    # recheck uses report thresholds
    (0.49, Trigger.AUTO, Decision.KEEP),         # below escalate
])
def test_hate_threat_thresholds(rules, score, trigger, expected):
    v = evaluate(rules, res({"hate/threatening": score}), ALL_ON, trigger)
    assert v.decision is expected


def test_report_never_deletes_unlisted_category(rules):
    # Ordinary offensiveness (e.g. plain "hate" or "harassment") is never a deletion, reported or not.
    v = evaluate(rules, res({"hate": 0.99, "harassment": 0.99, "sexual": 0.99}), ALL_ON, Trigger.REPORT)
    assert v.decision is Decision.KEEP


def test_directed_threat_deletes(rules):
    v = evaluate(rules, res({"violence": 0.1, "harassment/threatening": 0.81}), ALL_ON, Trigger.AUTO)
    assert v.decision is Decision.DELETE and v.rule_id == "THREAT"


@pytest.mark.parametrize("trigger", [Trigger.AUTO, Trigger.REPORT])
def test_violent_content_alone_never_deletes(rules, trigger):
    # Fiction / gore with no directed threat: flag at most, even when reported.
    v = evaluate(rules, res({"violence": 0.98, "harassment/threatening": 0.02}), ALL_ON, trigger)
    assert v.decision is Decision.ESCALATE and v.rule_id == "VIOLENT_CONTENT"


def test_unflagged_child_safety_only_logs(rules):
    for trigger in (Trigger.AUTO, Trigger.REPORT):
        v = evaluate(rules, res({"sexual/minors": 0.99}), ALL_ON, trigger)
        assert v.decision is Decision.ESCALATE and not v.enforced and v.redact


def test_unflagged_child_safety_does_not_mask_live_threat(rules):
    v = evaluate(rules, res({"sexual/minors": 0.8, "hate/threatening": 0.9}), ALL_ON, Trigger.AUTO)
    assert v.rule_id == "HATE_THREAT" and v.decision is Decision.DELETE and v.redact


def test_child_safety_flag_deletes_and_pings_and_redacts(rules):
    v = evaluate(rules, res({"sexual/minors": 0.2}, {"sexual/minors": True}), ALL_ON, Trigger.AUTO)
    assert (v.decision, v.enforced, v.ping, v.redact) == (Decision.DELETE, True, True, True)


def test_child_safety_log_tier_posts_without_ping(rules):
    v = evaluate(rules, res({"sexual/minors": 0.15}), ALL_ON, Trigger.AUTO)
    assert (v.decision, v.post, v.ping, v.redact) == (Decision.ESCALATE, True, False, True)


def test_child_safety_below_log_tier_falls_through(rules):
    v = evaluate(rules, res({"sexual/minors": 0.05}), ALL_ON, Trigger.AUTO)
    assert v.decision is Decision.KEEP


@pytest.mark.parametrize("dry_run,cs_on,hate_on", list(itertools.product([True, False], repeat=3)))
def test_enforce_dry_run_matrix(rules, dry_run, cs_on, hate_on):
    enforced = {r for r, on in (("CHILD_SAFETY", cs_on), ("HATE_THREAT", hate_on)) if on}
    flags = StaticFlags(dry_run=dry_run, enforced=frozenset(enforced))
    cs = evaluate(rules, res(flagged={"sexual/minors": True}), flags, Trigger.AUTO)
    hate = evaluate(rules, res({"hate/threatening": 0.9}), flags, Trigger.AUTO)
    assert cs.decision is Decision.DELETE and hate.decision is Decision.DELETE
    assert cs.enforced == cs_on  # ignores_dry_run
    assert hate.enforced == (hate_on and not dry_run)


def test_enabling_one_rule_does_not_enable_another(rules):
    flags = StaticFlags(dry_run=False, enforced=frozenset({"HATE_THREAT"}))
    v = evaluate(rules, res({"harassment/threatening": 0.95}), flags, Trigger.AUTO)
    assert v.decision is Decision.DELETE and not v.enforced


SUPPRESS = """
- id: T
  categories: [violence]
  delete_at: {auto: 0.5, report: 0.5}
  escalate_at: 0.3
  suppress_if: {QUOTATION: 0.7}
- id: CS
  categories: [sexual/minors]
  delete_at: {auto: 0.5, report: 0.5}
  ignores_context: true
  suppress_if: {QUOTATION: 0.7}
"""


@pytest.mark.parametrize("quote,expected", [(0.69, Decision.DELETE), (0.70, Decision.ESCALATE)])
def test_suppress_if(quote, expected):
    rs = RuleSet.from_text(SUPPRESS)
    v = evaluate(rs, res({"violence": 0.9}, context={"QUOTATION": quote}), StaticFlags(False, frozenset({"T"})),
                 Trigger.AUTO)
    assert v.decision is expected


def test_suppress_if_ignored_with_ignores_context():
    rs = RuleSet.from_text(SUPPRESS)
    v = evaluate(rs, res({"sexual/minors": 0.9}, context={"QUOTATION": 0.99}), StaticFlags(False), Trigger.AUTO)
    assert v.decision is Decision.DELETE
