"""/modbot admin commands. Admin-only by default AND checked at runtime,
since server admins can re-grant command access in Integrations settings."""

from __future__ import annotations

from typing import TYPE_CHECKING

import discord
from discord import app_commands
from discord.ext import commands

from modbot.cogs._util import to_input
from modbot.db import now
from modbot.links import parse_message_link
from modbot.models import Trigger
from modbot.moderation.pipeline import user_facing

if TYPE_CHECKING:
    from modbot.bot import ModBot


def _is_admin(interaction: discord.Interaction) -> bool:
    perms = getattr(interaction.user, "guild_permissions", None)
    return bool(perms and perms.administrator)


@app_commands.default_permissions(administrator=True)
@app_commands.guild_only()
class ModbotGroup(app_commands.Group, name="modbot", description="ModBot administration"):
    def __init__(self, bot: ModBot):
        super().__init__()
        self.bot = bot

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        if not _is_admin(interaction):
            await interaction.response.send_message("Administrators only.", ephemeral=True)
            return False
        return True

    @app_commands.command(name="dryrun", description="Global dry-run override (child safety ignores it)")
    @app_commands.choices(state=[app_commands.Choice(name="on", value="on"),
                                 app_commands.Choice(name="off", value="off")])
    async def dryrun(self, interaction: discord.Interaction, state: app_commands.Choice[str]) -> None:
        await self.bot.db.set_setting("dry_run", "true" if state.value == "on" else "false")
        await interaction.response.send_message(f"Dry-run is now **{state.value}**.", ephemeral=True)

    @app_commands.command(name="enforce", description="Turn deletion on or off for one rule")
    @app_commands.choices(state=[app_commands.Choice(name="on", value="on"),
                                 app_commands.Choice(name="off", value="off")])
    async def enforce(self, interaction: discord.Interaction, rule: str, state: app_commands.Choice[str]) -> None:
        if self.bot.rules.get(rule) is None:
            ids = ", ".join(r.id for r in self.bot.rules.rules)
            await interaction.response.send_message(f"Unknown rule. Rules: {ids}", ephemeral=True)
            return
        await self.bot.db.set_setting(f"enforce:{rule}", "true" if state.value == "on" else "false")
        await interaction.response.send_message(f"`{rule}` enforcement is now **{state.value}**.", ephemeral=True)

    @enforce.autocomplete("rule")
    async def _rule_autocomplete(self, interaction: discord.Interaction, current: str):
        return [app_commands.Choice(name=r.id, value=r.id)
                for r in self.bot.rules.rules if current.upper() in r.id][:25]

    @app_commands.command(name="stats", description="Cases by rule and decision, last 7 days")
    async def stats(self, interaction: discord.Interaction) -> None:
        rows = await self.bot.db.stats(now() - 7 * 86400)
        db = self.bot.db
        lines = [f"dry-run: **{db.setting('dry_run')}**"]
        lines += [f"`{r.id}` enforce: **{db.setting('enforce:' + r.id)}**" for r in self.bot.rules.rules]
        lines.append("")
        if rows:
            lines.append("```\nrule              decision  trigger  live  n")
            for rule, decision, trigger, enforced, n in rows:
                lines.append(f"{rule:<17} {decision:<9} {trigger:<8} {enforced:<5} {n}")
            lines.append("```")
        else:
            lines.append("No cases in the last 7 days.")
        auto = self.bot.get_cog("Auto")
        if auto is not None:
            lines.append(f"Auto queue: {len(auto.queue)} waiting, {auto.queue.dropped} dropped since start")
        await interaction.response.send_message("\n".join(lines)[:2000], ephemeral=True)

    @app_commands.command(name="recheck", description="Re-score a message (uses report thresholds)")
    async def recheck(self, interaction: discord.Interaction, link: str) -> None:
        await interaction.response.defer(ephemeral=True, thinking=True)
        parsed = parse_message_link(link)
        if parsed is None or parsed.guild_id != self.bot.config.guild_id:
            await interaction.followup.send("Not a message link from this server.", ephemeral=True)
            return
        try:
            channel = self.bot.get_channel(parsed.channel_id) or await self.bot.fetch_channel(parsed.channel_id)
            message = await channel.fetch_message(parsed.message_id)
        except discord.HTTPException:
            await interaction.followup.send("Couldn't fetch that message.", ephemeral=True)
            return
        outcome = await self.bot.pipeline.run(to_input(message), Trigger.RECHECK)
        v = outcome.verdict
        await interaction.followup.send(
            f"Case #{outcome.case_id}: **{v.decision}** ({v.rule_id or '-'}) {v.reason}\n{user_facing(outcome)}",
            ephemeral=True,
        )


class Admin(commands.Cog):
    def __init__(self, bot: ModBot):
        self.bot = bot
        self.group = ModbotGroup(bot)
        bot.tree.add_command(self.group)

    async def cog_unload(self) -> None:
        self.bot.tree.remove_command(self.group.name)


async def setup(bot: ModBot) -> None:
    await bot.add_cog(Admin(bot))
