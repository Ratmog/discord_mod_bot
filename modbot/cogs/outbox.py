"""Retries mod-log posts that didn't go through (Discord error, or a crash
between recording a case and posting it). See Pipeline.flush_notifications."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from discord.ext import commands, tasks

if TYPE_CHECKING:
    from modbot.bot import ModBot

log = logging.getLogger(__name__)


class Outbox(commands.Cog):
    def __init__(self, bot: ModBot):
        self.bot = bot

    async def cog_load(self) -> None:
        self.flush.start()

    async def cog_unload(self) -> None:
        self.flush.cancel()

    @tasks.loop(seconds=60)
    async def flush(self) -> None:
        try:
            sent = await self.bot.pipeline.flush_notifications()
            if sent:
                log.info("outbox: re-sent %d mod-log post(s)", sent)
        except Exception:
            log.exception("outbox flush failed; will retry next minute")

    @flush.before_loop
    async def _before_flush(self) -> None:
        await self.bot.wait_until_ready()


async def setup(bot: ModBot) -> None:
    await bot.add_cog(Outbox(bot))
