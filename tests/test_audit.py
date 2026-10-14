"""Regression tests for the third review (027dced) and the full audit after it."""

import asyncio
from types import SimpleNamespace

from modbot.cogs._util import is_bot_account
from modbot.models import DISCORD_EPOCH_MS, Decision, ModerationInput, ModerationResult, Trigger
from modbot.moderation import pipeline as pipeline_mod
from modbot.moderation.pipeline import Pipeline, seed_rule_settings
from modbot.moderation.rules import RuleSet, StaticFlags, evaluate

from .conftest import FakeClassifier, FakeGateway

ALL_ON = StaticFlags(dry_run=False, enforced=frozenset({"CHILD_SAFETY", "HATE_THREAT", "THREAT", "VIOLENT_CONTENT"}))
CS_FLAG = {"sexual/minors": True}


async def make(db, rules, table=None, flagged=None, enforce=("HATE_THREAT", "THREAT")):
    await seed_rule_settings(db, rules, dry_run_default=False)
    for r in enforce:
        await db.set_setting(f"enforce:{r}", "true")
    clf, gw = FakeClassifier(table, flagged), FakeGateway()
    return Pipeline(db, clf, gw, rules, 1), clf, gw


async def case_rows(db, message_id):
    async with db.conn.execute("SELECT * FROM cases WHERE message_id = ? ORDER BY case_id", (message_id,)) as cur:
        return [dict(r) async for r in cur]


# ---- review findings --------------------------------------------------------

async def test_report_cooldown_is_atomic(db):
    results = await asyncio.gather(*(db.report_allowed("same-reporter") for _ in range(10)))
    assert sum(results) == 1


async def test_concurrent_report_counts_are_distinct(db, rules):
    p, clf, gw = await make(db, rules, {"z": {}})
    m = ModerationInput(20, 10, 42, "z")
    await asyncio.gather(*(p.run(m, Trigger.REPORT) for _ in range(5)))
    assert sorted(r["report_count"] for r in await case_rows(db, 20)) == [1, 2, 3, 4, 5]


async def test_gone_unconfirmed_is_not_counted_as_enforced(db, rules):
    p, clf, gw = await make(db, rules, {"bad": {"hate/threatening": 0.95}})
    m = ModerationInput(21, 10, 42, "bad")
    gw.add(m)

    async def crash(channel_id, message_id):
        raise asyncio.CancelledError

    gw.delete_message = crash
    try:
        await p.run(m, Trigger.REPORT)
    except asyncio.CancelledError:
        pass
    gw.messages.pop(21)
    await p.reconcile()
    (row,) = await case_rows(db, 21)
    assert (row["action"], row["enforced"]) == ("gone_unconfirmed", 0)
    stats = await db.stats(0)
    assert all(enforced == 0 for *_, enforced, _n in stats)


async def test_failed_post_is_retried_by_outbox_exactly_once(db, rules, monkeypatch):
    monkeypatch.setattr(pipeline_mod, "OUTBOX_GRACE", -1)  # don't wait the real 60 s
    p, clf, gw = await make(db, rules, {"bad": {"hate/threatening": 0.95}})
    m = ModerationInput(22, 10, 42, "bad")
    gw.add(m)
    gw.fail_posts = True
    out = await p.run(m, Trigger.REPORT)
    assert out.deleted and gw.posts == []
    gw.fail_posts = False
    assert await p.flush_notifications() == 1
    assert await p.flush_notifications() == 0
    assert len(gw.posts) == 1 and "Deleted" in gw.posts[0].title


async def test_successful_post_is_not_reposted(db, rules, monkeypatch):
    monkeypatch.setattr(pipeline_mod, "OUTBOX_GRACE", -1)
    p, clf, gw = await make(db, rules, {"bad": {"hate/threatening": 0.95}})
    gw.add(ModerationInput(23, 10, 42, "bad"))
    await p.run(ModerationInput(23, 10, 42, "bad"), Trigger.REPORT)
    assert await p.flush_notifications() == 0 and len(gw.posts) == 1


async def test_outbox_skips_deletes_still_in_flight(db, rules, monkeypatch):
    monkeypatch.setattr(pipeline_mod, "OUTBOX_GRACE", -1)
    p, clf, gw = await make(db, rules, {"bad": {"hate/threatening": 0.95}})
    m = ModerationInput(24, 10, 42, "bad")
    gw.add(m)
    started, release = asyncio.Event(), asyncio.Event()
    real = gw.delete_message

    async def slow(channel_id, message_id):
        started.set()
        await release.wait()
        await real(channel_id, message_id)

    gw.delete_message = slow
    task = asyncio.create_task(p.run(m, Trigger.REPORT))
    await started.wait()
    assert await p.flush_notifications() == 0  # case is 'pending': not the outbox's to post
    release.set()
    await task
    assert len(gw.posts) == 1


# ---- audit: child-safety conditions -----------------------------------------

async def test_child_safety_failed_delete_still_pings(db, rules):
    p, clf, gw = await make(db, rules, flagged={"bad": CS_FLAG})
    m = ModerationInput(30, 10, 42, "bad")
    gw.add(m)
    gw.fail_ids = {30}
    out = await p.run(m, Trigger.AUTO)
    assert not out.deleted and out.verdict.decision is Decision.ERROR
    post = gw.posts[-1]
    assert post.ping, "content is still up: this is exactly when mods must be pinged"
    assert any(name == "Next step" and "may still be up" in value for name, value in post.fields)


async def test_child_safety_pings_even_when_another_rule_decides(rules):
    flags = StaticFlags(dry_run=False, enforced=frozenset({"THREAT"}))  # CS enforcement turned off by an admin
    v = evaluate(rules, ModerationResult("T", "v", {"harassment/threatening": 0.99, "sexual/minors": 0.3}, CS_FLAG),
                 flags, Trigger.AUTO)
    assert v.rule_id == "THREAT" and v.enforced
    assert v.ping and v.redact


async def test_reconcile_rechecks_message_still_up(db, rules):
    """Crash after 'pending' but before DELETE reached Discord: the content is still
    up, so recovery must re-judge and remove it, not just mark the case failed."""
    p, clf, gw = await make(db, rules, flagged={"bad": CS_FLAG}, enforce=())
    m = ModerationInput(31, 10, 42, "bad")
    gw.add(m)
    real = gw.delete_message

    async def crash(channel_id, message_id):
        raise asyncio.CancelledError

    gw.delete_message = crash
    try:
        await p.run(m, Trigger.AUTO)
    except asyncio.CancelledError:
        pass
    gw.delete_message = real
    assert await p.reconcile() == 1
    assert gw.deleted == [31]
    first, second = await case_rows(db, 31)
    assert (first["action"], first["notify"]) == ("superseded", None)  # no stale post for the old case
    assert second["action"] == "done"


def test_flag_on_non_max_category_counts():
    rs = RuleSet.from_text("- {id: R, categories: [a, b], delete_if_flagged: true, default_enforce: true}")
    v = evaluate(rs, ModerationResult("T", "v", {"a": 0.9, "b": 0.1}, {"b": True}),
                 StaticFlags(False, frozenset({"R"})), Trigger.AUTO)
    assert v.decision is Decision.DELETE and "b flagged" in v.reason


# ---- audit: other conditions ------------------------------------------------

def test_webhook_proxies_are_moderated_but_bots_are_not():
    def m(bot, webhook_id, system=False):
        return SimpleNamespace(author=SimpleNamespace(bot=bot, system=system), webhook_id=webhook_id)

    assert is_bot_account(m(bot=True, webhook_id=None))
    assert is_bot_account(m(bot=False, webhook_id=None, system=True))
    assert not is_bot_account(m(bot=True, webhook_id=123))  # PluralKit / Tupperbox
    assert not is_bot_account(m(bot=False, webhook_id=None))


async def test_already_gone_is_titled_as_such(db, rules):
    p, clf, gw = await make(db, rules, {"bad": {"hate/threatening": 0.95}})
    out = await p.run(ModerationInput(32, 10, 42, "bad"), Trigger.REPORT)  # never added: already gone
    assert out.gone and "Already gone" in gw.posts[-1].title


async def test_modlog_shows_message_time_not_post_time(db, rules):
    p, clf, gw = await make(db, rules, {"bad": {"hate/threatening": 0.95}})
    mid = ((1_700_000_000 * 1000 - DISCORD_EPOCH_MS) << 22) + 1  # 2023-11-14 22:13:20 UTC
    gw.add(ModerationInput(mid, 10, 42, "bad"))
    await p.run(ModerationInput(mid, 10, 42, "bad"), Trigger.REPORT)
    message_field = dict(gw.posts[-1].fields)["Message"]
    assert "posted 2023-11-14 22:13:20 UTC" in message_field


async def test_wrong_length_classifier_output_is_a_failure_not_a_crash(db, rules):
    p, clf, gw = await make(db, rules, {"a": {}, "b": {}})

    async def short(batch):
        return [ModerationResult("T", "v", {}, {})]  # one result for two inputs

    clf.classify = short
    outs = await p.run_many([ModerationInput(33, 10, 42, "a"), ModerationInput(34, 10, 42, "b")], Trigger.AUTO)
    assert [o.scored for o in outs] == [False, False]


async def test_has_case_detects_already_judged_text(db, rules):
    p, clf, gw = await make(db, rules, {"hi": {}})
    m = ModerationInput(35, 10, 42, "hi")
    assert not await db.has_case(35, m.hash)
    await p.run(m, Trigger.AUTO)
    assert await db.has_case(35, m.hash)
    assert not await db.has_case(35, ModerationInput(35, 10, 42, "edited").hash)
