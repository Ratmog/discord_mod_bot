"""Queue-priority tag for the auto path.

Nothing is classified until it leaves the queue, so overflow can't tell which
messages matter most. This small, deliberately broad list marks messages that
pair an age word with sexual vocabulary as high priority; high-priority items
are never dropped. It decides ordering only, never a deletion.
"""

from __future__ import annotations

import re

_AGE = re.compile(
    r"\b(kids?|child(ren)?|minors?|underage|under[- ]?age|preteens?|teens?|"
    r"little (girl|boy)s?|school ?(girl|boy)s?|\d{1,2}\s?(yo|y/o|year[- ]?olds?))\b",
    re.IGNORECASE,
)
_SEXUAL = re.compile(
    r"\b(sex\w*|nudes?|naked|horny|porn\w*|fuck\w*|rape\w*|molest\w*|grop\w*|"
    r"dick|cock|pussy|tits|boobs|seduc\w*|aroused|cum)\b",
    re.IGNORECASE,
)


def is_high_priority(text: str) -> bool:
    return bool(_AGE.search(text) and _SEXUAL.search(text))
