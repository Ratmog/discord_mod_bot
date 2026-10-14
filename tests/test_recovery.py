"""Regression tests for the fourth review (7f4f03a): crash recovery and catch-up."""

import asyncio
from types import SimpleNamespace

import discord

from modbot.gateway import TransientError
from modbot.models import ModerationInput, Trigger
from modbot.moderation import pipeline as pipeline_mod
from modbot.moderation.pipeline import Pipeline, seed_rule_settings

from .conftest import FakeClassifier, FakeGateway

CS_FLAG = {"sexual/minors": True}


async def make(db, rules, table=None, flagged=None):
    await seed_rule_settings(db, rules, dry_run_default=False)  # CHILD_SAFETY is live by default
    await db.set_setting("enforce:HATE_THREAT", "true")
    clf, gw = FakeClassifier(table, flagged), FakeGateway()
    return Pipeline(db, clf, gw, rules, 1), clf, gw


async def crash_before_delete(p, gw, m, trigger=Trigger.AUTO):
    """The process dies after recording 'pending' but before Discord gets the DELETE."""
    real = gw.delete_message

    async def crash(channel_id, message_id):
        raise asyncio.CancelledError

    gw.delete_message = crash
    try:
        await p.run(m, trigger)
    except asyncio.CancelledError:
        pass
    gw.delete_message = real
    p._acting.clear()  # a real restart starts with nothing in flight


async def rows(db, message_id):
    async with db.conn.execute("SELECT * FROM cases WHERE message_id = ? ORDER BY case_id", (message_id,)) as cur:
        return [dict(r) async for r in cur]


# ---- 2. recovery must not lose the alert when the re-check can't score -------

async def test_recovery_keeps_alert_when_recheck_cannot_score(db, rules, monkeypatch):
    monkeypatch.setattr(pipeline_mod, "OUTBOX_GRACE", -1)
    p, clf, gw = await make(db, rules, flagged={"bad v1": CS_FLAG})
    gw.add(ModerationInput(40, 10, 42, "bad v1"))
    await crash_before_delete(p, gw, ModerationInput(40, 10, 42, "bad v1"))
    gw.add(ModerationInput(40, 10, 42, "bad v2"))  # edited while ModBot was down: no cached score
    clf.fail = True  # ...and OpenAI is unavailable during recovery

    assert await p.reconcile() == 1
    assert 40 in gw.messages and gw.deleted == []  # still up: nothing could be re-judged
    original = (await rows(db, 40))[0]
    assert (original["action"], original["decision"], original["notify"]) == ("failed", "ERROR", "pending")

    assert await p.flush_notifications() >= 1  # the original alert goes out
    alert = next(post for post in gw.posts if post.case_id == original["case_id"])
    assert alert.ping
    assert any(name == "Next step" and "may still be up" in value for name, value in alert.fields)


async def test_recovery_uses_cached_scores_when_text_unchanged(db, rules):
    p, clf, gw = await make(db, rules, flagged={"bad": CS_FLAG})
    m = ModerationInput(41, 10, 42, "bad")
    gw.add(m)
    await crash_before_delete(p, gw, m)
    clf.fail = True  # outage, but the unchanged text's scores are cached
    assert await p.reconcile() == 1
    assert gw.deleted == [41]
    first, second = await rows(db, 41)
    assert (first["action"], first["notify"]) == ("superseded", None)
    assert second["action"] == "done"


# ---- 3. periodic reconcile: retries, and never touches a live delete ---------

async def test_unresolved_action_is_resolved_by_a_later_pass(db, rules):
    p, clf, gw = await make(db, rules, flagged={"bad": CS_FLAG})
    m = ModerationInput(42, 10, 42, "bad")
    gw.add(m)
    await crash_before_delete(p, gw, m)
    real_fetch = gw.fetch_message

    async def flaky(channel_id, message_id):
        raise TransientError("503")

    gw.fetch_message = flaky
    assert await p.reconcile() == 0
    assert (await rows(db, 42))[0]["action"] == "pending"
    gw.fetch_message = real_fetch  # Discord recovers
    assert await p.reconcile() == 1
    assert gw.deleted == [42]


def test_reconcile_runs_periodically_not_just_at_startup():
    from modbot.cogs.recovery import Recovery

    assert Recovery.reconcile.minutes == 2  # a tasks.loop, so a failed startup pass gets retried


async def test_periodic_reconcile_ignores_a_live_delete(db, rules):
    p, clf, gw = await make(db, rules, {"bad": {"hate/threatening": 0.95}})
    m = ModerationInput(43, 10, 42, "bad")
    gw.add(m)
    started, release = asyncio.Event(), asyncio.Event()
    real = gw.delete_message

    async def slow(channel_id, message_id):
        started.set()
        await release.wait()
        await real(channel_id, message_id)

    gw.delete_message = slow
    live = asyncio.create_task(p.run(m, Trigger.REPORT))
    await started.wait()
    assert await p.reconcile() == 0  # the 'pending' row belongs to a delete in flight
    release.set()
    await live
    (row,) = await rows(db, 43)
    assert row["action"] == "done" and row["notify"] == "sent"
    assert "restarted" not in row["reason"]
    assert gw.delete_attempts == [43] and len(gw.posts) == 1


async def test_resolved_actions_are_never_overwritten(db, rules):
    p, clf, gw = await make(db, rules, {"bad": {"hate/threatening": 0.95}})
    m = ModerationInput(44, 10, 42, "bad")
    gw.add(m)
    out = await p.run(m, Trigger.REPORT)
    assert out.deleted
    assert not await db.finish_action(out.case_id, "superseded")
    assert (await rows(db, 44))[0]["action"] == "done"


# ---- 4. catch-up must not clear its checkpoint after a partial scan ----------

def _game_msg(mid):
    return SimpleNamespace(id=mid, guild=SimpleNamespace(id=1), author=SimpleNamespace(id=9, bot=True),
                           mentions=[], reference=None)


class _Chan:
    def __init__(self, cid, msgs):
        self.id, self.msgs, self.fail = cid, msgs, False

    async def history(self, **_):
        if self.fail:
            raise discord.HTTPException(SimpleNamespace(status=503, reason="Service Unavailable"), "down")
        for m in self.msgs:
            yield m


async def test_partial_catch_up_keeps_checkpoint_and_retries(db):
    from modbot.cogs.cleanup import Cleanup

    bot = SimpleNamespace(db=db, config=SimpleNamespace(
        guild_id=1, game_bot_ids=frozenset({9}), cleanup_include_commands=False, game_channel_ids=frozenset()))
    cog = Cleanup(bot)
    a, b, c = _Chan(1, [_game_msg(101)]), _Chan(2, [_game_msg(201)]), _Chan(3, [_game_msg(301)])
    b.fail = True
    cog._channels = lambda: [a, b, c]

    assert await cog.catch_up() is False
    assert db.setting("gateway_down_since")  # window kept open (and survives a restart)
    assert cog._catchup_due  # next sweep retries
    b.fail = False
    assert await cog.catch_up() is True
    assert db.setting("gateway_down_since") == "" and not cog._catchup_due
    async with db.conn.execute("SELECT message_id FROM cleanup_queue ORDER BY message_id") as cur:
        assert [r["message_id"] async for r in cur] == [101, 201, 301]
