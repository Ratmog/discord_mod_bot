import asyncio

from modbot.models import Decision, ModerationInput, Trigger
from modbot.moderation.pipeline import Pipeline, seed_rule_settings

from .conftest import FakeClassifier, FakeGateway

GUILD = 1


def msg(mid: int, content: str, author: int = 42) -> ModerationInput:
    return ModerationInput(mid, 10, author, content)


async def make(db, rules, table=None, flagged=None, dry_run=True):
    await seed_rule_settings(db, rules, dry_run_default=dry_run)
    clf = FakeClassifier(table, flagged)
    gw = FakeGateway()
    return Pipeline(db, clf, gw, rules, GUILD), clf, gw


async def test_cached_score_then_enforce_toggle_then_report_deletes(db, rules):
    """Regression: a message scored by the auto path, then a rule is enforced,
    then the message is reported -> must act, without re-classifying."""
    p, clf, gw = await make(db, rules, {"threat": {"hate/threatening": 0.9}})
    m = msg(1, "threat")
    gw.add(m)
    first = await p.run(m, Trigger.AUTO)
    assert first.verdict.decision is Decision.DELETE and not first.deleted  # would-delete only

    await db.set_setting("dry_run", "false")
    await db.set_setting("enforce:HATE_THREAT", "true")
    second = await p.run(m, Trigger.REPORT)
    assert second.deleted and gw.deleted == [1]
    assert clf.calls == [1]  # scored once, decided twice


async def test_report_threshold_stricter_than_auto_from_same_score(db, rules):
    p, clf, gw = await make(db, rules, {"meh": {"hate/threatening": 0.7}}, dry_run=False)
    await db.set_setting("enforce:HATE_THREAT", "true")
    m = msg(2, "meh")
    gw.add(m)
    auto = await p.run(m, Trigger.AUTO)
    assert auto.verdict.decision is Decision.ESCALATE and not auto.deleted
    rep = await p.run(m, Trigger.REPORT)
    assert rep.deleted
    assert clf.calls == [1]


async def test_classifier_error_is_kept_and_retried_later(db, rules):
    p, clf, gw = await make(db, rules, {"x": {"hate/threatening": 0.9}})
    m = msg(3, "x")
    clf.fail = True
    out = await p.run(m, Trigger.REPORT)
    assert out.verdict.decision is Decision.ERROR and not out.deleted
    clf.fail = False
    out2 = await p.run(m, Trigger.REPORT)
    assert out2.verdict.decision is Decision.DELETE  # retried, not the cached ERROR
    assert clf.calls == [1, 1]


async def test_concurrent_reports_share_one_call(db, rules):
    p, clf, gw = await make(db, rules, {"y": {}})
    clf.delay = 0.05
    m = msg(4, "y")
    outs = await asyncio.gather(*(p.run(m, Trigger.REPORT) for _ in range(5)))
    assert clf.calls == [1]
    assert all(o.verdict.decision is Decision.KEEP for o in outs)


async def test_report_count_increments(db, rules):
    p, clf, gw = await make(db, rules, {"z": {}})
    m = msg(5, "z")
    for _ in range(3):
        await p.run(m, Trigger.REPORT)
    assert await db.prior_reports(5, m.hash) == 3


async def test_batching(db, rules):
    p, clf, gw = await make(db, rules)
    await p.run_many([msg(100 + i, f"m{i}") for i in range(25)], Trigger.AUTO)
    assert clf.calls == [20, 5]


async def test_child_safety_deletes_in_dry_run_redacts_pings_and_dms(db, rules):
    p, clf, gw = await make(db, rules, flagged={"bad": {"sexual/minors": True}}, dry_run=True)
    m = msg(6, "bad", author=77)
    gw.add(m)
    out = await p.run(m, Trigger.AUTO)
    assert out.deleted and gw.dms == [77]
    post = gw.posts[-1]
    assert post.ping
    assert ("Content", "_redacted_") in post.fields
    async with db.conn.execute("SELECT content FROM cases WHERE case_id = ?", (out.case_id,)) as cur:
        assert (await cur.fetchone())["content"] is None


async def test_edited_since_scoring_is_rejudged(db, rules):
    p, clf, gw = await make(db, rules, {"bad": {"hate/threatening": 0.9}, "fine now": {}}, dry_run=False)
    await db.set_setting("enforce:HATE_THREAT", "true")
    m = msg(7, "bad")
    gw.add(msg(7, "fine now"))  # edited before we got to delete it
    out = await p.run(m, Trigger.AUTO)
    assert not out.deleted and out.verdict.decision is Decision.KEEP


async def test_already_gone(db, rules):
    p, clf, gw = await make(db, rules, {"bad": {"hate/threatening": 0.9}}, dry_run=False)
    await db.set_setting("enforce:HATE_THREAT", "true")
    out = await p.run(msg(8, "bad"), Trigger.REPORT)  # never added to the fake gateway
    assert out.gone and not out.deleted


async def test_settings_seed_once(db, rules):
    await seed_rule_settings(db, rules, dry_run_default=True)
    await db.set_setting("enforce:HATE_THREAT", "true")
    await seed_rule_settings(db, rules, dry_run_default=False)  # restart with different defaults
    assert db.setting("enforce:HATE_THREAT") == "true"
    assert db.setting("dry_run") == "true"
