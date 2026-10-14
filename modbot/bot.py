"""The bot process. Run with: python -m modbot"""

from __future__ import annotations

import logging

import discord
from discord import app_commands
from discord.ext import commands

from modbot.config import Config
from modbot.db import Database
from modbot.gateway import DiscordGateway, ModlogPost
from modbot.moderation.openai_mod import OpenAIModerationClassifier
from modbot.moderation.pipeline import Pipeline, seed_rule_settings
from modbot.moderation.rules import RuleSet
from modbot.views import OutcomeButton

log = logging.getLogger("modbot")

COGS = ("modbot.cogs.reports", "modbot.cogs.auto", "modbot.cogs.cleanup", "modbot.cogs.outbox",
        "modbot.cogs.admin")
REQUIRED_PERMS = ("view_channel", "read_message_history", "send_messages", "embed_links", "manage_messages")


class ModBot(commands.Bot):
    def __init__(self, config: Config):
        intents = discord.Intents.none()
        intents.guilds = True
        intents.guild_messages = True
        intents.message_content = True  # privileged: enable it in the Developer Portal
        super().__init__(command_prefix=commands.when_mentioned, intents=intents,
                         allowed_mentions=discord.AllowedMentions.none())
        self.config = config
        self.db = Database(config.db_path)
        self.rules = RuleSet.load(config.rules_path)
        self.gateway = DiscordGateway(self, config.modlog_channel_id, config.mod_role_id)
        self.pipeline: Pipeline  # set in setup_hook
        self._checked_perms = False

    async def setup_hook(self) -> None:
        # One-time work lives here, not in on_ready (which repeats on re-IDENTIFY).
        await self.db.connect()
        await seed_rule_settings(self.db, self.rules, self.config.dry_run_default)
        classifier = OpenAIModerationClassifier(self.config.openai_api_key, self.config.openai_rpm)
        self.pipeline = Pipeline(self.db, classifier, self.gateway, self.rules, self.config.guild_id)
        self.add_dynamic_items(OutcomeButton)
        self.tree.error(self._on_tree_error)
        for cog in COGS:
            await self.load_extension(cog)
        guild = discord.Object(id=self.config.guild_id)
        self.tree.copy_global_to(guild=guild)
        synced = await self.tree.sync(guild=guild)  # guild sync is instant; global can take an hour
        log.info("synced %d command(s) to guild %s; rules version %s", len(synced), guild.id, self.rules.version)

    async def on_ready(self) -> None:
        log.info("logged in as %s (%s)", self.user, getattr(self.user, "id", "?"))
        if self._checked_perms:
            return
        self._checked_perms = True
        fixed = await self.pipeline.reconcile()  # deletions a crash left half-recorded
        if fixed:
            log.info("reconciled %d pending deletion(s)", fixed)
        guild = self.get_guild(self.config.guild_id)
        if guild is None:
            log.error("bot is not in guild %s", self.config.guild_id)
            return
        problems = []
        missing = [p for p in REQUIRED_PERMS if not getattr(guild.me.guild_permissions, p)]
        if missing:
            problems.append(("Missing permissions", ", ".join(missing)))
        role_id = self.config.mod_role_id
        if role_id:
            role = guild.get_role(role_id)
            if role is None:
                problems.append(("Moderator role", f"MOD_ROLE_ID {role_id} doesn't exist in this server"))
            elif not role.mentionable and not guild.me.guild_permissions.mention_everyone:
                problems.append(("Moderator pings won't notify", f"Make @{role.name} mentionable, or give ModBot "
                                 "the 'Mention @everyone, @here, and All Roles' permission"))
        if problems:
            for name, value in problems:
                log.warning("%s: %s", name, value)
            await self.gateway.post_modlog(ModlogPost("ModBot setup problems", 0xD83C3E, problems))

    async def _on_tree_error(self, interaction: discord.Interaction, error: app_commands.AppCommandError) -> None:
        if isinstance(error, app_commands.CheckFailure):
            return  # the check already told the user why
        log.error("command %s failed", getattr(interaction.command, "qualified_name", "?"), exc_info=error)
        try:
            if interaction.response.is_done():
                await interaction.followup.send("Something went wrong.", ephemeral=True)
            else:
                await interaction.response.send_message("Something went wrong.", ephemeral=True)
        except discord.HTTPException:
            pass

    async def close(self) -> None:
        # Stop workers and loops before the database closes under them.
        for ext in list(self.extensions):
            try:
                await self.unload_extension(ext)
            except Exception:
                log.exception("failed to unload %s", ext)
        await super().close()
        await self.db.close()


def main() -> None:
    config = Config.from_env()
    logging.basicConfig(level=config.log_level, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    ModBot(config).run(config.discord_token, log_handler=None)
