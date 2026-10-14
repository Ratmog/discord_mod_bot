"""Regression tests for the second code review of f81cea0. Each failed on that commit."""

import asyncio

import aiosqlite

from modbot.db import MIGRATIONS, Database
from modbot.models import Decision, ModerationInput, ModerationResult, Trigger
from modbot.moderation.pipeline import Pipeline, seed_rule_settings
from modbot.moderation.rules import StaticFlags, evaluate

from .conftest import FakeClassifier, FakeGateway

ALL_ON = StaticFlags(dry_run=False, enforced=frozenset({"CHILD_SAFETY", "HATE_THREAT", "THREAT", "VIOLENT_CONTENT"}))


async def live(db, rules, table):
    await seed_rule_settings(db, rules, dry_run_default=False)
    await db.set_setting("enforce:HATE_THREAT", "true")
    await db.set_setting("enforce:THREAT", "true")
    clf, gw = FakeClassifier(table), FakeGateway()
    return Pipeline(db, clf, gw, rules, 1), clf, gw


# 1. Edited message that is still a violation must not deadlock on its own lock.
async def test_edited_message_still_violation_no_deadlock(db, rules):
    p, clf, gw = await live(db, rules, {"bad v1": {"hate/threatening": 0.95},
                                        "bad v2": {"hate/threatening": 0.95}})
    gw.add(ModerationInput(1, 10, 42, "bad v2"))  # edited after scoring
    out = await asyncio.wait_for(p.run(ModerationInput(1, 10, 42, "bad v1"), Trigger.AUTO), timeout=1.0)
    assert out.deleted and gw.deleted == [1]


async def test_message_that_keeps_changing_gives_up_without_hanging(db, rules):
    p, clf, gw = await live(db, rules, {f"bad {i}": {"hate/threatening": 0.95} for i in range(10)})
    n = {"i": 0}
    real_fetch = gw.fetch_message

    async def ever_changing(channel_id, message_id):
        n["i"] += 1
        gw.add(ModerationInput(1, 10, 42, f"bad {n['i']}"))
        return await real_fetch(channel_id, message_id)

    gw.add(ModerationInput(1, 10, 42, "bad 0"))
    gw.fetch_message = ever_changing
    out = await asyncio.wait_for(p.run(ModerationInput(1, 10, 42, "bad 0"), Trigger.AUTO), timeout=1.0)
    assert not out.deleted and out.verdict.decision is Decision.ERROR


# 2. A database created by an older schema must upgrade on connect.
OLD_CASES = """
CREATE TABLE cases (
    case_id INTEGER PRIMARY KEY AUTOINCREMENT, message_id INTEGER NOT NULL, channel_id INTEGER NOT NULL,
    author_id INTEGER NOT NULL, content_hash TEXT NOT NULL, content TEXT, source TEXT, model_version TEXT,
    rules_version TEXT NOT NULL, trigger TEXT NOT NULL, report_count INTEGER NOT NULL DEFAULT 0,
    scores_json TEXT, decision TEXT NOT NULL, rule_id TEXT, reason TEXT, enforced INTEGER NOT NULL,
    mod_outcome TEXT, created_at INTEGER NOT NULL
);
INSERT INTO cases(message_id, channel_id, author_id, content_hash, rules_version, trigger, decision,
                  enforced, created_at) VALUES (1, 2, 3, 'h', 'v', 'auto', 'KEEP', 0, 0);
"""


async def test_existing_database_migrates(tmp_path):
    path = str(tmp_path / "old.db")
    async with aiosqlite.connect(path) as conn:
        await conn.executescript(OLD_CASES)
        await conn.commit()
    db = Database(path)
    await db.connect()  # f81cea0: OperationalError: no such column: action
    async with db.conn.execute("PRAGMA table_info(cases)") as cur:
        cols = {r["name"] async for r in cur}
    assert {column for table, column, _ in MIGRATIONS if table == "cases"} <= cols
    async with db.conn.execute("SELECT COUNT(*) AS n FROM cases") as cur:
        assert (await cur.fetchone())["n"] == 1  # old rows kept
    await db.close()
    again = Database(path)
    await again.connect()  # idempotent on a second start
    await again.close()


# 3. A weak signal from an earlier rule must not mask a stronger later rule.
def res(scores, flagged=None):
    return ModerationResult("T", "v", scores, flagged or {})


def test_low_priority_log_does_not_mask_threat(rules):
    v = evaluate(rules, res({"sexual/minors": 0.15, "harassment/threatening": 0.96}), ALL_ON, Trigger.AUTO)
    assert v.decision is Decision.DELETE and v.rule_id == "THREAT"
    assert v.redact  # the child-safety signal still forces redaction


def test_live_delete_beats_would_delete(rules):
    flags = StaticFlags(dry_run=False, enforced=frozenset({"THREAT"}))
    v = evaluate(rules, res({"hate/threatening": 0.95, "harassment/threatening": 0.9}), flags, Trigger.AUTO)
    assert v.decision is Decision.DELETE and v.enforced and v.rule_id == "THREAT"


def test_flagged_child_safety_still_wins(rules):
    v = evaluate(rules, res({"sexual/minors": 0.2, "harassment/threatening": 0.99}, {"sexual/minors": True}),
                 ALL_ON, Trigger.AUTO)
    assert v.rule_id == "CHILD_SAFETY" and v.ping


# 4. The lock must not be evicted while callers still wait on it.
async def test_late_reports_after_failed_delete_never_overlap(db, rules):
    p, clf, gw = await live(db, rules, {"bad": {"hate/threatening": 0.95}})
    m = ModerationInput(5, 10, 42, "bad")
    gw.add(m)
    gw.fail_ids = {5}  # every delete fails, so no report short-circuits on "already deleted"
    active = {"now": 0, "max": 0}
    first_done = asyncio.Event()
    real_delete = gw.delete_message

    async def tracked(channel_id, message_id):
        active["now"] += 1
        active["max"] = max(active["max"], active["now"])
        try:
            await asyncio.sleep(0.02)
            await real_delete(channel_id, message_id)
        finally:
            active["now"] -= 1
            first_done.set()

    gw.delete_message = tracked
    first = [asyncio.create_task(p.run(m, Trigger.REPORT)) for _ in range(3)]
    await first_done.wait()  # first holder done; others are queued on its lock
    await asyncio.sleep(0.015)  # it has released (and on f81cea0, evicted) the lock
    later = [asyncio.create_task(p.run(m, Trigger.REPORT)) for _ in range(3)]
    await asyncio.gather(*first, *later)
    assert active["max"] == 1
    assert p._locks == {}


# 5. Recovery must not claim a deletion it can't prove.
async def test_reconcile_marks_gone_unconfirmed(db, rules):
    p, clf, gw = await live(db, rules, {"bad": {"hate/threatening": 0.95}})
    m = ModerationInput(6, 10, 42, "bad")
    gw.add(m)

    async def crash(channel_id, message_id):
        raise asyncio.CancelledError  # died after writing 'pending', before DELETE was sent

    gw.delete_message = crash
    try:
        await p.run(m, Trigger.REPORT)
    except asyncio.CancelledError:
        pass
    gw.messages.pop(6)  # a human moderator removed it meanwhile
    assert await p.reconcile() == 1
    async with db.conn.execute("SELECT action FROM cases WHERE message_id = 6") as cur:
        assert (await cur.fetchone())["action"] == "gone_unconfirmed"
    assert await db.already_deleted(6)  # still no point acting on it again
