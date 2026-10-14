"""Policy regression checks for initial launch defaults and admin controls."""
import asyncio
from types import SimpleNamespace

from modbot.cogs.auto import Auto
from modbot.models import Decision, ModerationInput, ModerationResult, Trigger
from modbot.moderation.pipeline import Pipeline, seed_rule_settings
from modbot.moderation.rules import RuleSet, StaticFlags, evaluate
from .conftest import FakeClassifier, FakeGateway


def test_exempt_staff_channel_and_thread():
    auto = object.__new__(Auto)
    auto.bot = SimpleNamespace(config=SimpleNamespace(guild_id=1, exempt_channel_ids=frozenset({10})))

    def msg(channel_id, parent_id=None):
        return SimpleNamespace(
            guild=SimpleNamespace(id=1),
            channel=SimpleNamespace(id=channel_id, parent_id=parent_id),
            author=SimpleNamespace(bot=False, system=False),
            webhook_id=None, content="normal member text",
        )
    assert not auto._eligible(msg(10))
    assert not auto._eligible(msg(21, 10))
    assert auto._eligible(msg(20))


async def test_audited_toggle_concurrency_and_no_op(db):
    old, changed = await db.set_setting_audited("enforce:THREAT", "true", 111)
    assert changed and old is None
    results = await asyncio.gather(
        *(db.set_setting_audited("enforce:THREAT", "false", 200 + i) for i in range(5))
    )
    assert sum(changed for _old, changed in results) == 1
    async with db.conn.execute(
        "SELECT admin_id, old_value, new_value FROM settings_audit ORDER BY audit_id"
    ) as cur:
        entries = [tuple(r) async for r in cur]
    assert entries[0] == (111, None, "true")
    assert len(entries) == 2 and entries[1][1:] == ("true", "false")


async def test_duplicate_reports_recorded_but_not_reposted(db, rules):
    await seed_rule_settings(db, rules, dry_run_default=True)
    clf = FakeClassifier({"bad": {"hate/threatening": 0.9}})
    gw = FakeGateway()
    p = Pipeline(db, clf, gw, rules, 1)
    inp = ModerationInput(999, 10, 42, "bad")
    await asyncio.gather(*(p.run(inp, Trigger.REPORT) for _ in range(5)))
    assert len(gw.posts) == 1
    async with db.conn.execute(
        "SELECT report_count, notify FROM cases WHERE message_id = 999 ORDER BY case_id"
    ) as cur:
        rows = [tuple(r) async for r in cur]
    assert sorted(n for n, _notify in rows) == [1, 2, 3, 4, 5]
    assert sum(notify == "sent" for _n, notify in rows) == 1


async def test_changed_enforcement_gets_new_notification(db, rules):
    await seed_rule_settings(db, rules, dry_run_default=True)
    clf = FakeClassifier({"bad": {"hate/threatening": 0.9}})
    gw = FakeGateway()
    inp = ModerationInput(1001, 10, 42, "bad")
    gw.add(inp)
    p = Pipeline(db, clf, gw, rules, 1)
    await p.run(inp, Trigger.REPORT)
    assert len(gw.posts) == 1
    await db.set_setting("dry_run", "false")
    await db.set_setting("enforce:HATE_THREAT", "true")
    outcome = await p.run(inp, Trigger.REPORT)
    assert outcome.deleted and len(gw.posts) == 2


def test_log_and_escalate_thresholds_are_ordered():
    rules = RuleSet.from_text(
        "- id: A\n  categories: [x]\n  log_at: 0.10\n  escalate_at: 0.70\n"
    )
    flags = StaticFlags()
    v = evaluate(rules, ModerationResult("T", "v", {"x": 0.9}, {}), flags, Trigger.AUTO)
    assert v.decision is Decision.ESCALATE
    assert "log tier" not in v.reason
