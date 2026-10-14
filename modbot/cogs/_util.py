from __future__ import annotations

import discord

from modbot.models import ModerationInput


def to_input(msg: discord.Message) -> ModerationInput:
    parent = None
    ref = msg.reference
    if ref is not None and isinstance(ref.resolved, discord.Message):
        parent = ref.resolved.content
    return ModerationInput(msg.id, msg.channel.id, msg.author.id, msg.content, parent)


def is_game_command(msg: discord.Message, game_bot_ids: frozenset[int]) -> bool:
    """A member's message aimed at a game bot: mentions one, or replies to one."""
    if any(u.id in game_bot_ids for u in msg.mentions):
        return True
    ref = msg.reference
    resolved = ref.resolved if ref is not None else None
    return isinstance(resolved, discord.Message) and resolved.author.id in game_bot_ids
