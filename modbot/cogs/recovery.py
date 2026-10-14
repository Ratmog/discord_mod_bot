"""Background recovery, every pass also running once at startup:

- reconcile: resolve enforced deletes a crash left 'pending'. Periodic, not just
  at startup, so a Discord hiccup during startup doesn't leave a violation up
  until the next restart. Skips deletes in flight in this process.
- outbox: re-send mod-log posts that didn't go through.

Each loop catches its own errors: an uncaught exception would stop a tasks.loop.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from discord.ext import commands, tasks

if TYPE_CHECKING:
    from modbot.bot import ModBot

log = logging.getLogger(__name__)


class Recovery(commands.Cog):
    def __init__(self, bot: ModBot):
        self.bot = bot

    async def cog_load(self) -> None:
        self.reconcile.start()
        self.outbox.start()

    async def cog_unload(self) -> None:
        self.reconcile.cancel()
        self.outbox.cancel()

    @tasks.loop(minutes=2)
    async def reconcile(self) -> None:
        try:
            resolved = await self.bot.pipeline.reconcile()
            if resolved:
                log.info("reconciled %d pending deletion(s)", resolved)
        except Exception:
            log.exception("reconcile pass failed; will retry")

    @tasks.loop(seconds=60)
    async def outbox(self) -> None:
        try:
            sent = await self.bot.pipeline.flush_notifications()
            if sent:
                log.info("outbox: re-sent %d mod-log post(s)", sent)
        except Exception:
            log.exception("outbox flush failed; will retry")

    @reconcile.before_loop
    @outbox.before_loop
    async def _wait_ready(self) -> None:
        await self.bot.wait_until_ready()


async def setup(bot: ModBot) -> None:
    await bot.add_cog(Recovery(bot))
