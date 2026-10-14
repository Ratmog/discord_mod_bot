"""Phase 1 auto path: score every human message.

Every rule except child safety starts unenforced, so this mostly produces
would-DELETE and flagged rows over real traffic: the calibration data for
gate G3. Child safety is live from day one by policy.
"""

from __future__ import annotations

import asyncio
import logging
from typing import TYPE_CHECKING

import discord
from discord.ext import commands

from modbot.autoqueue import AutoQueue
from modbot.cogs._util import is_bot_account, to_input
from modbot.models import ModerationInput, Trigger, content_hash
from modbot.priority import is_high_priority

if TYPE_CHECKING:
    from modbot.bot import ModBot

log = logging.getLogger(__name__)
WORKERS = 2
LINGER = 0.5  # seconds to wait for more messages so bursts go out as one batch
MAX_RETRIES = 3  # re-score after a classifier outage, with growing delays
RETRY_DELAY = 30  # seconds x attempt number


class Auto(commands.Cog):
    def __init__(self, bot: ModBot):
        self.bot = bot
        self.queue: AutoQueue[ModerationInput] = AutoQueue(maxsize=500)
        self._workers: list[asyncio.Task] = []
        self._retry_handles: set[asyncio.TimerHandle] = set()
        self._attempts: dict[tuple[int, str], int] = {}
        self._last_dropped = 0
        self._last_dropped_high = 0

    async def cog_load(self) -> None:
        self._workers = [asyncio.create_task(self._worker(i)) for i in range(WORKERS)]

    async def cog_unload(self) -> None:
        for t in self._workers:
            t.cancel()
        for h in self._retry_handles:
            h.cancel()

    def _eligible(self, msg: discord.Message) -> bool:
        # Messages that @ a game bot are still scored: exempting them would let
        # anyone bypass moderation by mentioning Pokétwo.
        return (
            msg.guild is not None
            and msg.guild.id == self.bot.config.guild_id
            and not is_bot_account(msg)
            and bool(msg.content.strip())
        )

    def _enqueue(self, inp: ModerationInput) -> None:
        self.queue.put(inp, high=is_high_priority(inp.content))
        if self.queue.dropped != self._last_dropped:
            log.warning("auto queue overflow: %d normal-priority messages dropped so far", self.queue.dropped)
            self._last_dropped = self.queue.dropped
        if self.queue.dropped_high != self._last_dropped_high:
            log.error("auto queue hard cap hit: %d HIGH-priority messages dropped; is OpenAI down?",
                      self.queue.dropped_high)
            self._last_dropped_high = self.queue.dropped_high

    @commands.Cog.listener()
    async def on_message(self, msg: discord.Message) -> None:
        if self._eligible(msg):
            self._enqueue(to_input(msg))

    @commands.Cog.listener()
    async def on_raw_message_edit(self, payload: discord.RawMessageUpdateEvent) -> None:
        if payload.guild_id != self.bot.config.guild_id:
            return
        msg = payload.message  # the full updated message; no fetch needed
        cached = payload.cached_message
        if cached is not None and cached.content == msg.content:
            return  # embed unfurl or pin, not a text edit
        if not self._eligible(msg):
            return
        if await self.bot.db.has_case(msg.id, content_hash(msg.content)):
            return  # this exact text was already judged (uncached unfurl edits land here)
        self._enqueue(to_input(msg))  # new text -> new score -> new case row

    def _schedule_retry(self, inp: ModerationInput) -> None:
        key = (inp.message_id, inp.hash)
        n = self._attempts.get(key, 0) + 1
        if n > MAX_RETRIES:
            self._attempts.pop(key, None)
            log.warning("giving up on message %s after %d classifier failures", inp.message_id, MAX_RETRIES)
            return
        self._attempts[key] = n
        loop = asyncio.get_running_loop()

        def fire() -> None:
            self._retry_handles.discard(handle)
            self._enqueue(inp)

        handle = loop.call_later(RETRY_DELAY * n, fire)
        self._retry_handles.add(handle)

    async def _worker(self, n: int) -> None:
        clf = self.bot.pipeline.classifier
        while True:
            try:
                batch = await self.queue.take(clf.max_batch, linger=LINGER)
                if not batch:  # the other worker took everything during the linger
                    continue
                outcomes = await self.bot.pipeline.run_many(batch, Trigger.AUTO)
                for inp, outcome in zip(batch, outcomes, strict=True):
                    if outcome.scored:
                        self._attempts.pop((inp.message_id, inp.hash), None)
                    else:  # classifier was down: don't let these go unchecked forever
                        self._schedule_retry(inp)
            except asyncio.CancelledError:
                raise
            except Exception:
                log.exception("auto worker %d failed on a batch; continuing", n)
                await asyncio.sleep(1)


async def setup(bot: ModBot) -> None:
    await bot.add_cog(Auto(bot))
