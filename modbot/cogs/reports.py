"""Private reporting: /report <link> and the "Report to ModBot" context menu."""

from __future__ import annotations

import hashlib
import hmac
import logging
from typing import TYPE_CHECKING

import discord
from discord import app_commands
from discord.ext import commands

from modbot.cogs._util import to_input
from modbot.links import parse_message_link
from modbot.models import Trigger
from modbot.moderation.pipeline import user_facing

if TYPE_CHECKING:
    from modbot.bot import ModBot

log = logging.getLogger(__name__)


class Reports(commands.Cog):
    def __init__(self, bot: ModBot):
        self.bot = bot
        # Context menus can't be decorated methods inside a cog in discord.py 2.x.
        self.ctx_menu = app_commands.ContextMenu(name="Report to ModBot", callback=self.on_context_report)
        self.bot.tree.add_command(self.ctx_menu)

    async def cog_unload(self) -> None:
        self.bot.tree.remove_command(self.ctx_menu.name, type=self.ctx_menu.type)

    def _reporter_hash(self, user_id: int) -> str:
        return hmac.new(self.bot.config.reporter_hmac_secret, str(user_id).encode(), hashlib.sha256).hexdigest()

    @app_commands.command(name="report", description="Privately report a message to ModBot")
    @app_commands.describe(link="Message link (right-click the message -> Copy Message Link)")
    @app_commands.guild_only()
    async def report(self, interaction: discord.Interaction, link: str) -> None:
        await interaction.response.defer(ephemeral=True, thinking=True)
        parsed = parse_message_link(link)
        if parsed is None or parsed.guild_id != self.bot.config.guild_id:
            await interaction.followup.send("That isn't a message link from this server.", ephemeral=True)
            return
        channel = self.bot.get_channel(parsed.channel_id)
        if channel is None:
            try:
                channel = await self.bot.fetch_channel(parsed.channel_id)
            except discord.HTTPException:
                channel = None
        member = interaction.user
        if channel is None or not isinstance(member, discord.Member) or not hasattr(channel, "fetch_message"):
            await interaction.followup.send("I can't find that message.", ephemeral=True)
            return
        perms = channel.permissions_for(member)
        if not (perms.read_messages and perms.read_message_history):
            # Same reply as not-found, so the bot can't be used to probe private channels.
            await interaction.followup.send("I can't find that message.", ephemeral=True)
            return
        try:
            message = await channel.fetch_message(parsed.message_id)
        except discord.NotFound:
            await interaction.followup.send("That message is already gone.", ephemeral=True)
            return
        except discord.HTTPException:
            await interaction.followup.send("I can't find that message.", ephemeral=True)
            return
        await self._handle(interaction, message)

    async def on_context_report(self, interaction: discord.Interaction, message: discord.Message) -> None:
        await interaction.response.defer(ephemeral=True, thinking=True)
        await self._handle(interaction, message)

    async def _handle(self, interaction: discord.Interaction, message: discord.Message) -> None:
        if message.guild is None or message.guild.id != self.bot.config.guild_id:
            await interaction.followup.send("That isn't a message from this server.", ephemeral=True)
            return
        if message.author.bot:
            await interaction.followup.send("Bot messages aren't moderated.", ephemeral=True)
            return
        if not message.content.strip():
            await interaction.followup.send("There's no text in that message for me to check.", ephemeral=True)
            return
        if not await self.bot.db.report_allowed(self._reporter_hash(interaction.user.id)):
            await interaction.followup.send("Slow down a bit: try again in a minute.", ephemeral=True)
            return
        try:
            outcome = await self.bot.pipeline.run(to_input(message), Trigger.REPORT)
        except Exception:
            log.exception("report pipeline failed")
            await interaction.followup.send("Something went wrong checking that message.", ephemeral=True)
            return
        await interaction.followup.send(user_facing(outcome), ephemeral=True)


async def setup(bot: ModBot) -> None:
    await bot.add_cog(Reports(bot))
