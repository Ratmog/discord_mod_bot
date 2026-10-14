import pytest

from modbot.autoqueue import AutoQueue
from modbot.links import parse_message_link
from modbot.priority import is_high_priority


@pytest.mark.parametrize("link,ok", [
    ("https://discord.com/channels/1/2/3", True),
    ("https://ptb.discord.com/channels/1/2/3", True),
    ("https://canary.discord.com/channels/1/2/3", True),
    ("https://discordapp.com/channels/1/2/3", True),
    ("<https://discord.com/channels/1/2/3>", True),
    ("https://discord.com/channels/@me/2/3", False),
    ("https://evil.com/channels/1/2/3", False),
    ("https://discord.com/channels/1/2", False),
    ("not a link", False),
])
def test_link_parser(link, ok):
    parsed = parse_message_link(link)
    assert (parsed is not None) == ok
    if ok:
        assert (parsed.guild_id, parsed.channel_id, parsed.message_id) == (1, 2, 3)


def test_priority_needs_both_word_kinds():
    assert not is_high_priority("my kid is annoying")
    assert not is_high_priority("that's so fucking funny")


async def test_queue_drops_oldest_normal_never_high():
    q = AutoQueue(maxsize=3)
    q.put("h1", high=True)
    q.put("n1")
    q.put("n2")
    q.put("n3")  # over capacity: n1 dropped
    q.put("h2", high=True)  # n2 dropped
    assert q.dropped == 2
    assert await q.take(10) == ["h1", "h2", "n3"]
