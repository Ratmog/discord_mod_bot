"""Entry point: python -m modbot"""

from __future__ import annotations

import logging

import discord
from discord.ext import commands

from modbot.config import Config
from modbot.db import Database
from modbot.gateway import DiscordGateway, ModlogPost
from modbot.moderation.openai_mod import OpenAIModerationClassifier
from modbot.moderation.pipeline import Pipeline, seed_rule_settings
from modbot.moderation.rules import RuleSet
from modbot.views import OutcomeButton

log = logging.getLogger("modbot")

COGS = ("modbot.cogs.reports", "modbot.cogs.auto", "modbot.cogs.cleanup", "modbot.cogs.admin")
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
        guild = self.get_guild(self.config.guild_id)
        if guild is None:
            log.error("bot is not in guild %s", self.config.guild_id)
            return
        missing = [p for p in REQUIRED_PERMS if not getattr(guild.me.guild_permissions, p)]
        if missing:
            log.warning("missing server permissions: %s", ", ".join(missing))
            await self.gateway.post_modlog(ModlogPost(
                "ModBot is missing permissions", 0xD83C3E, [("Missing", ", ".join(missing))]))

    async def close(self) -> None:
        await super().close()
        await self.db.close()


def main() -> None:
    config = Config.from_env()
    logging.basicConfig(level=config.log_level, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    ModBot(config).run(config.discord_token, log_handler=None)


if __name__ == "__main__":
    main()
