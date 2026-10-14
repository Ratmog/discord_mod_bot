"""Thin wrapper over the Discord calls the core logic needs.

The pipeline and sweeper only talk to `Gateway`, so tests swap in a fake and
never touch discord.py. `DiscordGateway` translates discord.py exceptions into
the two that matter: the message is gone, or the call failed and can be retried.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import TYPE_CHECKING, Protocol

if TYPE_CHECKING:
    import discord

log = logging.getLogger(__name__)


class GoneError(Exception):
    """The message or channel no longer exists."""


class TransientError(Exception):
    """A Discord call failed in a way worth retrying later."""


@dataclass(frozen=True)
class FetchedMessage:
    message_id: int
    channel_id: int
    author_id: int
    content: str


@dataclass(frozen=True)
class ModlogPost:
    title: str
    color: int
    fields: list[tuple[str, str]]
    case_id: int | None = None
    ping: bool = False
    buttons: bool = False  # optional moderator outcome buttons


class Gateway(Protocol):
    async def fetch_message(self, channel_id: int, message_id: int) -> FetchedMessage: ...
    async def delete_message(self, channel_id: int, message_id: int) -> None: ...
    async def bulk_delete(self, channel_id: int, message_ids: list[int]) -> None: ...
    async def dm(self, user_id: int, text: str) -> bool: ...
    async def post_modlog(self, post: ModlogPost) -> bool: ...  # True once Discord accepted it


class DiscordGateway:
    def __init__(self, bot: discord.Client, modlog_channel_id: int, mod_role_id: int | None):
        self.bot = bot
        self.modlog_channel_id = modlog_channel_id
        self.mod_role_id = mod_role_id

    async def _channel(self, channel_id: int):
        import discord

        ch = self.bot.get_channel(channel_id)
        if ch is None:
            try:
                ch = await self.bot.fetch_channel(channel_id)
            except discord.NotFound as e:
                raise GoneError(str(e)) from e
            except discord.HTTPException as e:
                raise TransientError(str(e)) from e
        return ch

    async def fetch_message(self, channel_id: int, message_id: int) -> FetchedMessage:
        import discord

        ch = await self._channel(channel_id)
        try:
            m = await ch.fetch_message(message_id)
        except discord.NotFound as e:
            raise GoneError(str(e)) from e
        except discord.HTTPException as e:
            raise TransientError(str(e)) from e
        return FetchedMessage(m.id, m.channel.id, m.author.id, m.content)

    async def delete_message(self, channel_id: int, message_id: int) -> None:
        import discord

        ch = await self._channel(channel_id)
        try:
            await ch.get_partial_message(message_id).delete()
        except discord.NotFound as e:
            raise GoneError(str(e)) from e
        except discord.HTTPException as e:
            raise TransientError(str(e)) from e

    async def bulk_delete(self, channel_id: int, message_ids: list[int]) -> None:
        """2-100 messages under 14 days old. Discord returns 204 even for IDs
        that no longer exist, so this never raises GoneError for a message."""
        import discord

        if not 2 <= len(message_ids) <= 100:
            raise ValueError("bulk_delete takes 2-100 ids")
        ch = await self._channel(channel_id)
        try:
            await ch.delete_messages([discord.Object(id=i) for i in message_ids])
        except discord.HTTPException as e:
            raise TransientError(str(e)) from e

    async def dm(self, user_id: int, text: str) -> bool:
        import discord

        try:
            user = self.bot.get_user(user_id) or await self.bot.fetch_user(user_id)
            await user.send(text)
            return True
        except (discord.Forbidden, discord.NotFound):
            return False  # DMs closed or user gone: common, not an error
        except discord.HTTPException:
            log.warning("DM to %s failed", user_id, exc_info=True)
            return False

    async def post_modlog(self, post: ModlogPost) -> bool:
        import discord

        from modbot.views import outcome_view

        try:
            ch = await self._channel(self.modlog_channel_id)
        except (GoneError, TransientError):
            log.error("mod-log channel %s unavailable", self.modlog_channel_id)
            return False
        embed = discord.Embed(title=post.title, color=post.color)
        for name, value in post.fields:
            embed.add_field(name=name, value=value[:1024] or "-", inline=False)
        content = f"<@&{self.mod_role_id}>" if post.ping and self.mod_role_id else None
        view = outcome_view(post.case_id) if post.buttons and post.case_id else None
        kwargs = {"content": content, "embed": embed,
                  "allowed_mentions": discord.AllowedMentions(roles=True, users=False, everyone=False)}
        if view is not None:
            kwargs["view"] = view
        try:
            await ch.send(**kwargs)
        except discord.HTTPException:
            log.exception("failed to post to mod-log")
            return False
        return True
