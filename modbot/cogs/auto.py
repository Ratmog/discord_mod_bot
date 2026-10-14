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
from modbot.cogs._util import is_game_command, to_input
from modbot.models import ModerationInput, Trigger
from modbot.priority import is_high_priority

if TYPE_CHECKING:
    from modbot.bot import ModBot

log = logging.getLogger(__name__)
WORKERS = 2


class Auto(commands.Cog):
    def __init__(self, bot: ModBot):
        self.bot = bot
        self.queue: AutoQueue[ModerationInput] = AutoQueue(maxsize=500)
        self._workers: list[asyncio.Task] = []
        self._last_dropped = 0

    async def cog_load(self) -> None:
        self._workers = [asyncio.create_task(self._worker(i)) for i in range(WORKERS)]

    async def cog_unload(self) -> None:
        for t in self._workers:
            t.cancel()

    def _eligible(self, msg: discord.Message) -> bool:
        cfg = self.bot.config
        return (
            msg.guild is not None
            and msg.guild.id == cfg.guild_id
            and not msg.author.bot
            and not msg.webhook_id
            and bool(msg.content.strip())
            and not is_game_command(msg, cfg.game_bot_ids)
        )

    def _enqueue(self, msg: discord.Message) -> None:
        self.queue.put(to_input(msg), high=is_high_priority(msg.content))
        if self.queue.dropped != self._last_dropped:
            log.warning("auto queue overflow: %d normal-priority messages dropped so far", self.queue.dropped)
            self._last_dropped = self.queue.dropped

    @commands.Cog.listener()
    async def on_message(self, msg: discord.Message) -> None:
        if self._eligible(msg):
            self._enqueue(msg)

    @commands.Cog.listener()
    async def on_raw_message_edit(self, payload: discord.RawMessageUpdateEvent) -> None:
        # Embed unfurls fire edits with no content change; skip before touching the DB.
        new_content = payload.data.get("content")
        if new_content is None:
            return
        cached = payload.cached_message
        if cached is not None and cached.content == new_content:
            return
        if payload.guild_id != self.bot.config.guild_id:
            return
        channel = self.bot.get_channel(payload.channel_id)
        if channel is None or not hasattr(channel, "fetch_message"):
            return
        try:
            msg = await channel.fetch_message(payload.message_id)
        except discord.HTTPException:
            return
        if self._eligible(msg):
            self._enqueue(msg)  # new hash -> new score -> new case row

    async def _worker(self, n: int) -> None:
        clf = self.bot.pipeline.classifier
        while True:
            try:
                batch = await self.queue.take(clf.max_batch)
                await self.bot.pipeline.run_many(batch, Trigger.AUTO)
            except asyncio.CancelledError:
                raise
            except Exception:
                log.exception("auto worker %d failed on a batch; continuing", n)
                await asyncio.sleep(1)


async def setup(bot: ModBot) -> None:
    await bot.add_cog(Auto(bot))
