"""Message-link parsing for /report. Pure, so it's unit-tested without Discord."""

from __future__ import annotations

import re
from dataclasses import dataclass

# discord.com, discordapp.com (legacy), ptb./canary. subdomains. Numeric guild
# ids only, so DM links (/channels/@me/...) never match.
_LINK = re.compile(
    r"^<?https?://(?:(?:ptb|canary)\.)?discord(?:app)?\.com/channels/(\d+)/(\d+)/(\d+)/?>?$"
)


@dataclass(frozen=True)
class MessageLink:
    guild_id: int
    channel_id: int
    message_id: int


def parse_message_link(text: str) -> MessageLink | None:
    m = _LINK.match(text.strip())
    if not m:
        return None
    return MessageLink(int(m[1]), int(m[2]), int(m[3]))
