"""The discord.py APIs ModBot calls, checked against whatever version is installed.

CI runs this on the oldest discord.py that requirements.txt allows, so raising
the floor (or using a newer API) can't silently break a supported install.
"""

import re
from pathlib import Path

import discord
from discord import app_commands

ROOT = Path(__file__).resolve().parent.parent


def _floor() -> tuple[int, ...]:
    line = next(ln for ln in (ROOT / "requirements.txt").read_text().splitlines() if ln.startswith("discord.py"))
    return tuple(int(x) for x in re.search(r">=([\d.]+)", line).group(1).split("."))


def test_installed_version_meets_floor():
    installed = tuple(int(x) for x in re.match(r"[\d.]+", discord.__version__).group(0).rstrip(".").split("."))
    assert installed >= _floor()


def test_edit_event_carries_the_message():
    assert "message" in discord.RawMessageUpdateEvent.__slots__  # added in 2.5


def test_persistent_buttons_api():
    assert hasattr(discord.ui, "DynamicItem")
    assert hasattr(discord.Client, "add_dynamic_items")


def test_app_command_apis():
    assert hasattr(app_commands, "ContextMenu")
    assert hasattr(app_commands.CommandTree, "error")
    assert hasattr(app_commands, "default_permissions") and hasattr(app_commands, "guild_only")


def test_permission_names_used_at_startup():
    from modbot.bot import REQUIRED_PERMS

    perms = discord.Permissions.none()
    for name in (*REQUIRED_PERMS, "mention_everyone"):
        assert hasattr(perms, name), name
