"""Persistent moderator buttons on #mod-logs posts.

DynamicItem matches the custom_id by regex, so buttons on posts made weeks ago
still work after restarts: the case id lives in the custom_id itself.
"""

from __future__ import annotations

import re

import discord

TEMPLATE = r"modbot:outcome:(?P<case_id>\d+):(?P<outcome>violation|fine)"


class OutcomeButton(discord.ui.DynamicItem[discord.ui.Button], template=TEMPLATE):
    def __init__(self, case_id: int, outcome: str):
        label, style = ("Violation", discord.ButtonStyle.danger) if outcome == "violation" else (
            "Fine", discord.ButtonStyle.secondary)
        super().__init__(
            discord.ui.Button(label=label, style=style, custom_id=f"modbot:outcome:{case_id}:{outcome}")
        )
        self.case_id = case_id
        self.outcome = outcome

    @classmethod
    async def from_custom_id(cls, interaction: discord.Interaction, item: discord.ui.Button, match: re.Match[str]):
        return cls(int(match["case_id"]), match["outcome"])

    async def callback(self, interaction: discord.Interaction) -> None:
        perms = getattr(interaction.user, "guild_permissions", None)
        if not perms or not perms.manage_messages:
            await interaction.response.send_message("Moderators only.", ephemeral=True)
            return
        ok = await interaction.client.db.set_mod_outcome(self.case_id, self.outcome)  # type: ignore[attr-defined]
        msg = f"Case #{self.case_id} marked **{self.outcome}**." if ok else "Case not found."
        await interaction.response.send_message(msg, ephemeral=True)


def outcome_view(case_id: int) -> discord.ui.View:
    view = discord.ui.View(timeout=None)
    view.add_item(OutcomeButton(case_id, "violation"))
    view.add_item(OutcomeButton(case_id, "fine"))
    return view
