"""Moderation pipeline: cached scores -> current rules -> act -> record.

Invariant: the expensive prediction is cached; the decision never is. Every
trigger (auto, report, recheck) re-runs `evaluate()` against the current
rules.yaml and the current enforce/dry-run settings.
"""

from __future__ import annotations

import asyncio
import datetime as dt
import logging
from dataclasses import dataclass, replace

from modbot.db import Database, now
from modbot.gateway import Gateway, GoneError, ModlogPost, TransientError
from modbot.models import Decision, ModerationInput, ModerationResult, Trigger, Verdict, content_hash
from modbot.moderation.base import Classifier
from modbot.moderation.rules import RuleSet, evaluate

log = logging.getLogger(__name__)

AUTHOR_DM = (
    "A message you posted was removed automatically by the server's filter. "
    "If you were reporting or discussing abuse rather than joking, contact a moderator."
)
REPORT_FORM = "https://dis.gd/request"

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

    async def _finish(
        self, inp: ModerationInput, result: ModerationResult | None, trigger: Trigger, retried: bool = False
    ) -> Outcome:
        rules = self.rules
        report_count = 0
        if trigger is Trigger.REPORT:
            report_count = await self.db.prior_reports(inp.message_id, inp.hash) + 1

        if result is None:
            verdict = Verdict.error("classifier unavailable; kept")
            case_id = await self.db.append_case(inp, None, verdict, trigger.value, rules.version, report_count)
            return Outcome(verdict, case_id)

        verdict = evaluate(rules, result, self.flags, trigger)
        deleted = gone = False

        if verdict.decision is Decision.DELETE and verdict.enforced:
            try:
                current = await self.gw.fetch_message(inp.channel_id, inp.message_id)
            except GoneError:
                gone = True
            except TransientError:
                verdict = replace(verdict, decision=Decision.ERROR, enforced=False,
                                  reason=verdict.reason + "; re-fetch failed, kept")
            else:
                if content_hash(current.content) != inp.hash:
                    if not retried:  # edited since scoring: judge the new text once
                        new_inp = replace(inp, content=current.content)
                        new_res = (await self._scores([new_inp]))[0]
                        return await self._finish(new_inp, new_res, trigger, retried=True)
                    verdict = replace(verdict, decision=Decision.ERROR, enforced=False,
                                      reason=verdict.reason + "; message keeps changing, kept")
                else:
                    try:
                        await self.gw.delete_message(inp.channel_id, inp.message_id)
                        deleted = True
                    except GoneError:
                        gone = True
                    except TransientError:
                        verdict = replace(verdict, decision=Decision.ERROR, enforced=False,
                                          reason=verdict.reason + "; delete failed, kept")
            if gone:
                verdict = replace(verdict, enforced=False, reason=verdict.reason + "; already gone")

        case_id = await self.db.append_case(inp, result, verdict, trigger.value, rules.version, report_count)

        if verdict.post:
            await self.gw.post_modlog(await self._modlog_post(inp, verdict, trigger, case_id, deleted, report_count))
        if deleted:
            await self.gw.dm(inp.author_id, AUTHOR_DM)
        return Outcome(verdict, case_id, deleted, gone)

    async def _modlog_post(
        self, inp: ModerationInput, v: Verdict, trigger: Trigger, case_id: int, deleted: bool, report_count: int
    ) -> ModlogPost:
        if deleted:
            title, color = "Deleted", COLORS["deleted"]
        elif v.decision is Decision.DELETE:
            title, color = "Would delete (not enforced)", COLORS["would"]
        elif v.decision is Decision.ERROR:
            title, color = "Error, kept", COLORS["error"]
        else:
            title, color = "Flagged, kept", COLORS["flag"]

        link = f"https://discord.com/channels/{self.guild_id}/{inp.channel_id}/{inp.message_id}"
        ts = dt.datetime.fromtimestamp(now(), dt.UTC).strftime("%Y-%m-%d %H:%M:%S UTC")
        fields = [
            ("Rule", f"`{v.rule_id}`"),
            ("Signal", v.reason),
            ("Trigger", trigger.value + (f" (reported {report_count}x)" if report_count else "")),
            ("Author", f"<@{inp.author_id}> (`{inp.author_id}`)"),
            ("Message", f"{link}\n<#{inp.channel_id}> · {ts}"),
        ]
        if not v.redact:
            fields.append(("Content", inp.content[:1000]))
        else:
            fields.append(("Content", "_redacted_"))

        ping = v.ping and deleted
        if ping:
            fields.append(("Next step", f"Report to Discord via the web form ({REPORT_FORM}) using the IDs "
                                        "above; the in-app report no longer works on a deleted message."))
            n = await self.db.flagged_deletions(inp.author_id, v.rule_id or "", now() - 30 * 86400)
            if n >= 2:
                fields.append(("Repeat offender", f"{n} enforced deletions under this rule in 30 days"))
        return ModlogPost(f"Case #{case_id} · {title}", color, fields, case_id, ping=ping, buttons=True)


def user_facing(outcome: Outcome) -> str:
    v = outcome.verdict
    if outcome.gone:
        return "That message is already gone."
    if outcome.deleted:
        return "Thanks, that message was removed."
    if v.decision is Decision.ERROR:
        return "Couldn't check that message right now; it's been logged. Try again in a bit."
    if v.decision in (Decision.DELETE, Decision.ESCALATE):
        return "Thanks, it's been logged for the moderators."
    return "Thanks, checked it: it doesn't break the server's rules."
