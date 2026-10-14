"""Delete Pokétwo/Lawliet messages 5-6 minutes after they post.

Enqueue on arrival, sweep every 60 s. The first sweep after start also scans
recent history for game messages posted while the bot was offline.
"""

from __future__ import annotations

import datetime as dt
import logging
from typing import TYPE_CHECKING

import discord
from discord.ext import commands, tasks

from modbot.cleanup import snowflake_ts, sweep
from modbot.cogs._util import is_game_command
from modbot.db import now
from modbot.gateway import ModlogPost

if TYPE_CHECKING:
    from modbot.bot import ModBot

log = logging.getLogger(__name__)
CATCHUP_WINDOW = 2 * 3600


class Cleanup(commands.Cog):
    def __init__(self, bot: ModBot):
        self.bot = bot

    async def cog_load(self) -> None:
        self.sweeper.start()
        self.maintenance.start()

    async def cog_unload(self) -> None:
        self.sweeper.cancel()
        self.maintenance.cancel()

    def _should_clean(self, msg: discord.Message) -> bool:
        cfg = self.bot.config
        if msg.guild is None or msg.guild.id != cfg.guild_id:
            return False
        if msg.author.id in cfg.game_bot_ids:
            return True
        return cfg.cleanup_include_commands and not msg.author.bot and is_game_command(msg, cfg.game_bot_ids)

    @commands.Cog.listener()
    async def on_message(self, msg: discord.Message) -> None:
        if self._should_clean(msg):
            await self.bot.db.enqueue_cleanup(msg.id, msg.channel.id, snowflake_ts(msg.id))

    def _channels(self) -> list[discord.abc.Messageable]:
        guild = self.bot.get_guild(self.bot.config.guild_id)
        if guild is None:
            return []
        ids = self.bot.config.game_channel_ids
        candidates = list(guild.text_channels) + list(guild.threads)  # threads: active ones only
        if ids:
            candidates = [c for c in candidates if c.id in ids or getattr(c, "parent_id", None) in ids]
        me = guild.me
        return [c for c in candidates
                if c.permissions_for(me).read_message_history and c.permissions_for(me).manage_messages]

    async def catch_up(self) -> None:
        """Game messages posted while offline: bounded window, paginated to the end (INSERT OR IGNORE)."""
        last = self.bot.db.last_heartbeat() or 0
        since = max(last, now() - CATCHUP_WINDOW) - 60
        after = dt.datetime.fromtimestamp(since, dt.UTC)
        found = 0
        for ch in self._channels():
            try:
                async for m in ch.history(after=after, limit=None, oldest_first=True):
                    if self._should_clean(m):
                        await self.bot.db.enqueue_cleanup(m.id, ch.id, snowflake_ts(m.id))
                        found += 1
            except discord.HTTPException:
                log.warning("catch-up failed for channel %s", ch.id, exc_info=True)
        log.info("cleanup catch-up queued %d message(s)", found)

    @tasks.loop(seconds=60)
    async def sweeper(self) -> None:
        if self.sweeper.current_loop == 0:
            await self.catch_up()  # first iteration only; on_ready repeats on IDENTIFY and isn't safe
        report = await sweep(self.bot.db, self.bot.gateway)
        if report.deleted or report.gone or report.retried:
            log.info("sweep: deleted=%d gone=%d retried=%d", report.deleted, report.gone, report.retried)
        if report.failed:
            await self.bot.gateway.post_modlog(ModlogPost(
                "Cleanup failing", 0x888888,
                [("Messages", f"{len(report.failed)} game message(s) failed 5 deletion attempts. "
                              "Check the bot still has Manage Messages in those channels.")],
            ))
        await self.bot.db.heartbeat()

    @sweeper.before_loop
    async def _before_sweep(self) -> None:
        await self.bot.wait_until_ready()

    @sweeper.error
    async def _sweep_error(self, error: BaseException) -> None:
        log.exception("sweeper crashed; restarting", exc_info=error)
        self.sweeper.restart()

    @tasks.loop(hours=24)
    async def maintenance(self) -> None:
        await self.bot.db.purge()

    @maintenance.before_loop
    async def _before_maint(self) -> None:
        await self.bot.wait_until_ready()


async def setup(bot: ModBot) -> None:
    await bot.add_cog(Cleanup(bot))
