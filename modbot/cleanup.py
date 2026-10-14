"""Game-bot cleanup core: delete queued messages once they are 5 minutes old.

Kept free of discord.py so the sweeper logic is unit-tested with a fake gateway.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field

from modbot.db import Database, now
from modbot.gateway import Gateway, GoneError, TransientError

log = logging.getLogger(__name__)

DISCORD_EPOCH_MS = 1420070400000
LIFETIME = 300  # seconds a game-bot message lives
BULK_MAX_AGE = int(13.5 * 86400)  # bulk delete refuses messages over 14 days old
BULK_MAX = 100


def snowflake_ts(snowflake: int) -> int:
    """Creation time (unix seconds) from the ID itself, so restarts never reset the clock."""
    return ((snowflake >> 22) + DISCORD_EPOCH_MS) // 1000


@dataclass
class SweepReport:
    deleted: int = 0
    gone: int = 0
    retried: int = 0
    failed: list[int] = field(default_factory=list)


async def sweep(db: Database, gw: Gateway, t: int | None = None) -> SweepReport:
    t = now() if t is None else t
    report = SweepReport()
    due = await db.due_cleanup(t - LIFETIME)
    for channel_id, ids in due.items():
        young = [i for i in ids if t - snowflake_ts(i) < BULK_MAX_AGE]
        old = [i for i in ids if t - snowflake_ts(i) >= BULK_MAX_AGE]
        for start in range(0, len(young), BULK_MAX):
            chunk = young[start:start + BULK_MAX]
            if len(chunk) == 1:
                await _delete_singly(db, gw, channel_id, chunk, report)
                continue
            try:
                await gw.bulk_delete(channel_id, chunk)
            except GoneError:  # the channel itself is gone
                await db.mark_cleanup(chunk, "gone")
                report.gone += len(chunk)
            except TransientError as e:
                report.failed += await db.bump_cleanup_attempts(chunk, str(e))
                report.retried += len(chunk)
            else:
                # 204 even for IDs that no longer exist; 'gone' is only knowable on single deletes
                await db.mark_cleanup(chunk, "deleted")
                report.deleted += len(chunk)
        if old:
            await _delete_singly(db, gw, channel_id, old, report)
    return report


async def _delete_singly(db: Database, gw: Gateway, channel_id: int, ids: list[int], report: SweepReport) -> None:
    for i in ids:
        try:
            await gw.delete_message(channel_id, i)
        except GoneError:
            await db.mark_cleanup([i], "gone")
            report.gone += 1
        except TransientError as e:
            report.failed += await db.bump_cleanup_attempts([i], str(e))
            report.retried += 1
        else:
            await db.mark_cleanup([i], "deleted")
            report.deleted += 1
