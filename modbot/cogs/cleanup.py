"""Delete Pokétwo/Lawliet messages 5-6 minutes after they post.

Enqueue on arrival, sweep every 60 s. History catch-up covers messages the bot
never saw: on the first sweep after start, and after any reconnect that needed
a fresh IDENTIFY (a RESUME replays missed events, so it needs nothing).

The catch-up checkpoint is `gateway_down_since`, written when the gateway
disconnects, not the sweeper heartbeat: the sweeper can keep running while the
gateway is down, so its heartbeat says nothing about what was observed. It's
cleared only after a scan in which every channel succeeded; a partial scan keeps
(or records) it and the next sweep retries.
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
WARN_EVERY = 3600  # at most one "cleanup failing" post per hour


class Cleanup(commands.Cog):
    def __init__(self, bot: ModBot):
        self.bot = bot
        self._seen_ready = False
        self._catchup_due = False  # a previous catch-up didn't finish every channel
        self._last_warn = 0
        self._no_perm_logged: set[int] = set()

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

    def _can_delete_in(self, channel) -> bool:
        me = channel.guild.me
        ok = channel.permissions_for(me).manage_messages
        if not ok and channel.id not in self._no_perm_logged:
            self._no_perm_logged.add(channel.id)  # log once per channel, don't queue doomed deletes
            log.warning("no Manage Messages in #%s (%s); game messages there won't be cleaned",
                        getattr(channel, "name", "?"), channel.id)
        return ok

    @commands.Cog.listener()
    async def on_message(self, msg: discord.Message) -> None:
        if self._should_clean(msg) and self._can_delete_in(msg.channel):
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

    @commands.Cog.listener()
    async def on_disconnect(self) -> None:
        if self.bot.db.setting("gateway_down_since") in (None, ""):
            await self.bot.db.set_setting("gateway_down_since", str(now()))

    @commands.Cog.listener()
    async def on_resumed(self) -> None:
        # RESUME replays everything missed, so there's nothing to catch up.
        await self.bot.db.set_setting("gateway_down_since", "")

    @commands.Cog.listener()
    async def on_ready(self) -> None:
        # Fires on every IDENTIFY. The first one is handled by the sweeper's first
        # loop; later ones mean events were lost, so catch up from the disconnect.
        if not self._seen_ready:
            self._seen_ready = True
            return
        await self.catch_up()

    def _catchup_start(self, down: str | None) -> int:
        """Earliest time whose messages may not have been seen (bounded to CATCHUP_WINDOW)."""
        candidates = [int(down)] if down else []
        hb = self.bot.db.last_heartbeat()
        if hb:
            candidates.append(hb)  # process restart: nothing was seen after the last sweep
        start = min(candidates) if candidates else now() - CATCHUP_WINDOW
        return max(start, now() - CATCHUP_WINDOW)

    async def catch_up(self) -> bool:
        """Queue game messages the bot never saw. Paginates each channel to the end;
        enqueueing is idempotent. Returns True only if every channel was scanned."""
        db = self.bot.db
        down = db.setting("gateway_down_since")
        start = self._catchup_start(down)
        after = dt.datetime.fromtimestamp(start - 60, dt.UTC)  # 60 s overlap; duplicates are ignored
        found, failed = 0, []
        for ch in self._channels():
            try:
                async for m in ch.history(after=after, limit=None, oldest_first=True):
                    if self._should_clean(m):
                        await db.enqueue_cleanup(m.id, ch.id, snowflake_ts(m.id))
                        found += 1
            except discord.HTTPException:
                failed.append(ch.id)
                log.warning("catch-up failed for channel %s", ch.id, exc_info=True)
        if failed:
            # Keep the window open: make sure a checkpoint covers it (it survives a
            # restart) and retry on the next sweep.
            if not down:
                await db.set_setting("gateway_down_since", str(start))
            self._catchup_due = True
            log.warning("catch-up incomplete (%d channel(s) failed, %d queued); retrying next sweep",
                        len(failed), found)
            return False
        # Clear the checkpoint only if no new disconnect was recorded during the scan.
        if db.setting("gateway_down_since") == down:
            await db.set_setting("gateway_down_since", "")
        self._catchup_due = False
        log.info("cleanup catch-up queued %d message(s)", found)
        return True

    @tasks.loop(seconds=60)
    async def sweeper(self) -> None:
        # Errors are caught here rather than in a loop error handler: an uncaught
        # exception stops a tasks.loop, and cleanup must keep running.
        try:
            if self.sweeper.current_loop == 0 or self._catchup_due:
                await self.catch_up()  # first iteration, or retrying a partial scan
            report = await sweep(self.bot.db, self.bot.gateway)
            if report.deleted or report.gone or report.retried:
                log.info("sweep: deleted=%d gone=%d retried=%d", report.deleted, report.gone, report.retried)
            if report.failed and now() - self._last_warn >= WARN_EVERY:
                self._last_warn = now()
                await self.bot.gateway.post_modlog(ModlogPost(
                    "Cleanup failing", 0x888888,
                    [("Messages", f"{len(report.failed)} game message(s) failed 5 deletion attempts. "
                                  "Check the bot still has Manage Messages in those channels.")],
                ))
            await self.bot.db.heartbeat()
        except Exception:
            log.exception("sweep failed; will retry next minute")

    @sweeper.before_loop
    async def _before_sweep(self) -> None:
        await self.bot.wait_until_ready()

    @tasks.loop(hours=24)
    async def maintenance(self) -> None:
        try:
            await self.bot.db.purge()
        except Exception:
            log.exception("retention purge failed; will retry tomorrow")

    @maintenance.before_loop
    async def _before_maint(self) -> None:
        await self.bot.wait_until_ready()


async def setup(bot: ModBot) -> None:
    await bot.add_cog(Cleanup(bot))
