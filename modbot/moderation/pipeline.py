"""Moderation pipeline: cached scores -> current rules -> act -> record -> notify.

Invariants:
- The expensive prediction is cached; the decision never is. Every trigger
  (auto, report, recheck) re-runs `evaluate()` against the current rules.yaml
  and the current enforce/dry-run settings.
- At most one enforcement action per message at a time (reference-counted lock),
  and nothing called while holding that lock re-enters it.
- An enforced delete is recorded as `pending` before Discord is called, so a
  crash leaves something `reconcile()` can resolve.
- Mod-log posts go through an outbox (`cases.notify`): written as pending with
  the case, marked sent once Discord accepts the post, retried by
  `flush_notifications()` otherwise. At-least-once: a crash between a post and
  its "sent" mark can repeat a post, which beats losing a child-safety alert.
"""

from __future__ import annotations

import asyncio
import datetime as dt
import logging
from dataclasses import dataclass, replace

from modbot.db import Database, now
from modbot.gateway import Gateway, GoneError, ModlogPost, TransientError
from modbot.models import (
    Decision,
    ModerationInput,
    ModerationResult,
    Trigger,
    Verdict,
    content_hash,
    snowflake_ts,
)
from modbot.moderation.base import Classifier
from modbot.moderation.rules import RuleSet, evaluate

log = logging.getLogger(__name__)

AUTHOR_DM = (
    "A message you posted was removed automatically by the server's moderation filter. "
    "If you think that was a mistake (for example, you were quoting or reporting something), "
    "contact a moderator."
)
REPORT_FORM = "https://dis.gd/request"

MAX_EDIT_RETRIES = 2  # re-judge an edited message at most this many times before giving up
OUTBOX_GRACE = 60  # seconds before the outbox retries a post the inline path may still be sending
OUTBOX_MAX_AGE = 7 * 86400
OUTBOX_BATCH = 20

COLORS = {"deleted": 0xD83C3E, "would": 0xE8A33D, "flag": 0x5B8DEF, "error": 0x888888}


class SettingsFlags:
    """Reads enforce/dry-run state from the settings table on every call."""

    def __init__(self, db: Database):
        self.db = db

    @property
    def dry_run(self) -> bool:
        return self.db.setting("dry_run", "true") == "true"

    def enforce(self, rule_id: str) -> bool:
        return self.db.setting(f"enforce:{rule_id}", "false") == "true"


async def seed_rule_settings(db: Database, rules: RuleSet, dry_run_default: bool) -> None:
    """rules.yaml and .env seed the settings table once; after that the DB wins."""
    await db.seed_setting("dry_run", "true" if dry_run_default else "false")
    for r in rules.rules:
        await db.seed_setting(f"enforce:{r.id}", "true" if r.default_enforce else "false")


@dataclass(frozen=True)
class Outcome:
    verdict: Verdict
    case_id: int | None
    deleted: bool = False
    gone: bool = False
    scored: bool = True  # False = the classifier was unavailable; worth retrying later


def _is_live_delete(v: Verdict) -> bool:
    return v.decision is Decision.DELETE and v.enforced


class _LockEntry:
    __slots__ = ("lock", "refs")

    def __init__(self) -> None:
        self.lock = asyncio.Lock()
        self.refs = 0


class Pipeline:
    def __init__(
        self,
        db: Database,
        classifier: Classifier,
        gateway: Gateway,
        rules: RuleSet,
        guild_id: int,
        flags=None,
    ):
        self.db = db
        self.classifier = classifier
        self.gw = gateway
        self.rules = rules
        self.guild_id = guild_id
        self.flags = flags or SettingsFlags(db)
        self._inflight: dict[tuple, asyncio.Future] = {}
        self._locks: dict[int, _LockEntry] = {}

    # ---- public -------------------------------------------------------------

    async def run(self, inp: ModerationInput, trigger: Trigger) -> Outcome:
        return (await self.run_many([inp], trigger))[0]

    async def run_many(self, inps: list[ModerationInput], trigger: Trigger) -> list[Outcome]:
        results = await self._scores(inps)
        return [await self._finish(inp, res, trigger) for inp, res in zip(inps, results, strict=True)]

    # ---- scores: cache, in-flight sharing, batching -------------------------

    def _key(self, inp: ModerationInput) -> tuple:
        return (inp.message_id, inp.hash, self.classifier.model_id)

    async def _scores(self, inps: list[ModerationInput]) -> list[ModerationResult | None]:
        """Never awaits a message lock, so it's safe to call while holding one."""
        results: list[ModerationResult | None] = [None] * len(inps)
        waits: dict[int, asyncio.Future] = {}
        mine: list[int] = []
        loop = asyncio.get_running_loop()

        for i, inp in enumerate(inps):
            cached = await self.db.get_scores(inp, self.classifier.model_id)
            if cached is not None:
                results[i] = cached
                continue
            key = self._key(inp)
            if key in self._inflight:  # another report or the auto path is already asking
                waits[i] = self._inflight[key]
                continue
            self._inflight[key] = loop.create_future()
            mine.append(i)

        try:
            size = max(1, self.classifier.max_batch)
            for start in range(0, len(mine), size):
                chunk = mine[start:start + size]
                batch = [inps[i] for i in chunk]
                try:
                    out = await self.classifier.classify(batch)
                    if len(out) != len(batch):
                        raise RuntimeError(f"classifier returned {len(out)} results for {len(batch)} inputs")
                except Exception:
                    log.warning("classifier failed for %d message(s)", len(batch), exc_info=True)
                    out = [None] * len(batch)  # nothing cached, so the next trigger retries
                for i, res in zip(chunk, out, strict=True):
                    if res is not None:
                        await self.db.put_scores(inps[i], self.classifier.model_id, res)
                    results[i] = res
                    fut = self._inflight.pop(self._key(inps[i]), None)
                    if fut and not fut.done():
                        fut.set_result(res)
        finally:
            for i in mine:  # cancellation or an unexpected error: never leave waiters hanging
                fut = self._inflight.pop(self._key(inps[i]), None)
                if fut and not fut.done():
                    fut.set_result(None)

        for i, fut in waits.items():
            results[i] = await fut
        return results

    # ---- decide, act, record ------------------------------------------------

    async def _finish(self, inp: ModerationInput, result: ModerationResult | None, trigger: Trigger) -> Outcome:
        if result is None:
            return await self._record(inp, None, Verdict.error("classifier unavailable; kept"), trigger, scored=False)
        verdict = evaluate(self.rules, result, self.flags, trigger)
        if not _is_live_delete(verdict):
            return await self._record(inp, result, verdict, trigger)

        # One enforcement action per message at a time. The entry is reference-counted
        # and evicted only when nobody holds or awaits it: Lock.locked() is False
        # between a release and the next waiter waking, so it can't be used for this.
        entry = self._locks.get(inp.message_id)
        if entry is None:
            entry = self._locks[inp.message_id] = _LockEntry()
        entry.refs += 1
        try:
            async with entry.lock:
                return await self._enforce(inp, result, verdict, trigger)
        finally:
            entry.refs -= 1
            if entry.refs == 0 and self._locks.get(inp.message_id) is entry:
                del self._locks[inp.message_id]

    async def _record(
        self, inp: ModerationInput, result: ModerationResult | None, verdict: Verdict, trigger: Trigger,
        *, action: str | None = None, gone: bool = False, scored: bool = True,
    ) -> Outcome:
        case_id = await self.db.append_case(inp, result, verdict, trigger.value, self.rules.version, action=action)
        if verdict.post:
            await self._notify(case_id)
        return Outcome(verdict, case_id, gone=gone, scored=scored)

    async def _enforce(
        self, inp: ModerationInput, result: ModerationResult, verdict: Verdict, trigger: Trigger,
    ) -> Outcome:
        """Runs under the message's lock. Never calls back into _finish (the lock isn't reentrant)."""
        for attempt in range(MAX_EDIT_RETRIES + 1):
            if await self.db.already_deleted(inp.message_id):
                # A concurrent caller already removed it: record, but no second post, DM or Discord call.
                verdict = replace(verdict, enforced=False, reason=verdict.reason + "; already removed")
                case_id = await self.db.append_case(inp, result, verdict, trigger.value, self.rules.version,
                                                    notify=False)
                return Outcome(verdict, case_id, gone=True)

            try:
                current = await self.gw.fetch_message(inp.channel_id, inp.message_id)
            except GoneError:
                verdict = replace(verdict, enforced=False, reason=verdict.reason + "; already gone")
                return await self._record(inp, result, verdict, trigger, action="gone", gone=True)
            except TransientError:
                verdict = replace(verdict, decision=Decision.ERROR, enforced=False,
                                  reason=verdict.reason + "; re-fetch failed, kept")
                return await self._record(inp, result, verdict, trigger)

            if content_hash(current.content) == inp.hash:
                break
            if attempt == MAX_EDIT_RETRIES:
                verdict = replace(verdict, decision=Decision.ERROR, enforced=False,
                                  reason=verdict.reason + "; message keeps changing, kept")
                return await self._record(inp, result, verdict, trigger)

            # Edited since scoring: judge the current text, still under this lock.
            inp = replace(inp, content=current.content)
            new_result = (await self._scores([inp]))[0]
            if new_result is None:
                return await self._record(inp, None, Verdict.error("classifier unavailable; kept"), trigger,
                                          scored=False)
            result = new_result
            verdict = evaluate(self.rules, result, self.flags, trigger)
            if not _is_live_delete(verdict):
                return await self._record(inp, result, verdict, trigger)

        # Write the audit record BEFORE deleting, so a crash in between leaves a
        # pending action that reconcile() resolves on restart.
        case_id = await self.db.append_case(inp, result, verdict, trigger.value, self.rules.version, action="pending")
        deleted = gone = False
        try:
            await self.gw.delete_message(inp.channel_id, inp.message_id)
        except GoneError:
            gone = True
            verdict = replace(verdict, enforced=False, reason=verdict.reason + "; already gone")
            await self.db.finish_action(case_id, "gone", verdict)
        except TransientError:
            verdict = replace(verdict, decision=Decision.ERROR, enforced=False,
                              reason=verdict.reason + "; delete failed, kept")
            await self.db.finish_action(case_id, "failed", verdict)
        else:
            deleted = True
            await self.db.finish_action(case_id, "done")
        if verdict.post:
            await self._notify(case_id)  # pings still fire when the delete failed: that's when they matter most
        if deleted:
            await self.gw.dm(inp.author_id, AUTHOR_DM)
        return Outcome(verdict, case_id, deleted=deleted, gone=gone)

    async def reconcile(self) -> int:
        """Resolve deletions left 'pending' by a crash. Returns how many were resolved.

        Missing message: it's gone, but that doesn't prove ModBot deleted it (a
        moderator may have), so it's gone_unconfirmed. Still present: the crash
        hit before Discord got the DELETE, so the violation may still be up;
        re-check it with current rules rather than leaving it.
        """
        resolved = 0
        for row in await self.db.pending_actions():
            try:
                current = await self.gw.fetch_message(row["channel_id"], row["message_id"])
            except GoneError:
                await self.db.finish_action(row["case_id"], "gone_unconfirmed")
            except TransientError:
                continue  # try again next start
            else:
                await self.db.finish_action(row["case_id"], "superseded")
                trigger = Trigger.AUTO if row["trigger"] == Trigger.AUTO.value else Trigger.RECHECK
                inp = ModerationInput(current.message_id, current.channel_id, current.author_id, current.content)
                await self.run(inp, trigger)
            resolved += 1
        return resolved

    # ---- mod-log outbox -----------------------------------------------------

    async def _notify(self, case_id: int) -> bool:
        row = await self.db.get_case(case_id)
        if row is None:
            return False
        ok = await self.gw.post_modlog(await self._build_post(row))
        if ok:
            await self.db.set_notified(case_id)
        return ok

    async def flush_notifications(self) -> int:
        """Retry mod-log posts that didn't go through. Returns how many were sent."""
        t = now()
        sent = 0
        for case_id in await self.db.pending_notifications(t - OUTBOX_GRACE, t - OUTBOX_MAX_AGE, OUTBOX_BATCH):
            if not await self._notify(case_id):
                break  # Discord is still refusing; try the rest next tick
            sent += 1
        return sent

    async def _build_post(self, c) -> ModlogPost:
        """A mod-log post built from the stored case, so inline posts and outbox retries match."""
        action, decision = c["action"], c["decision"]
        removed = action in ("done", "gone", "gone_unconfirmed")
        if action == "done":
            title, color = "Deleted", COLORS["deleted"]
        elif action == "gone_unconfirmed":
            title, color = "Gone (removal not confirmed)", COLORS["deleted"]
        elif action == "gone":
            title, color = "Already gone", COLORS["error"]
        elif decision == Decision.DELETE.value:
            title, color = "Would delete (not enforced)", COLORS["would"]
        elif decision == Decision.ERROR.value:
            title, color = "Error, kept", COLORS["error"]
        else:
            title, color = "Flagged, kept", COLORS["flag"]

        link = f"https://discord.com/channels/{self.guild_id}/{c['channel_id']}/{c['message_id']}"
        posted = dt.datetime.fromtimestamp(snowflake_ts(c["message_id"]), dt.UTC).strftime("%Y-%m-%d %H:%M:%S UTC")
        trigger = c["trigger"] + (f" (reported {c['report_count']}x)" if c["report_count"] else "")
        fields = [
            ("Rule", f"`{c['rule_id']}`"),
            ("Signal", c["reason"] or "-"),
            ("Trigger", trigger),
            ("Author", f"<@{c['author_id']}> (`{c['author_id']}`)"),
            ("Message", f"{link}\n<#{c['channel_id']}> · posted {posted}"),
            ("Content", c["content"][:1000] if c["content"] is not None else "_redacted_"),
        ]
        ping = bool(c["ping"])
        if ping:
            if removed:
                step = (f"Report it to Discord via the web form ({REPORT_FORM}) using the IDs above; "
                        "the in-app report no longer works on a deleted message.")
            else:
                step = (f"The message may still be up: delete it manually, then report it to Discord "
                        f"via the web form ({REPORT_FORM}).")
            fields.append(("Next step", step))
            n = await self.db.flagged_deletions(c["author_id"], c["rule_id"] or "", now() - 30 * 86400)
            if n >= 2:
                fields.append(("Repeat offender", f"{n} confirmed deletions under this rule in 30 days"))
        return ModlogPost(f"Case #{c['case_id']} · {title}", color, fields, c["case_id"], ping=ping, buttons=True)


def user_facing(outcome: Outcome) -> str:
    v = outcome.verdict
    if outcome.gone:
        return "That message is already gone."
    if outcome.deleted:
        return "Thanks, that message was removed."
    if not outcome.scored:
        return "Couldn't check that message right now. Try again in a bit."
    if v.decision is Decision.ERROR:
        return "Something went wrong acting on that message; it's been logged for the moderators."
    if v.decision in (Decision.DELETE, Decision.ESCALATE):
        return "Thanks, it's been logged for the moderators."
    return "Thanks, checked it: it doesn't break the server's rules."
