from __future__ import annotations

import asyncio
from pathlib import Path

import pytest

from modbot.db import Database
from modbot.gateway import FetchedMessage, GoneError, ModlogPost, TransientError
from modbot.models import ModerationInput, ModerationResult
from modbot.moderation.rules import RuleSet

ROOT = Path(__file__).resolve().parent.parent


class FakeClassifier:
    """Scores come from a dict keyed by message content; counts calls."""

    model_id = "fake-model"
    max_batch = 20

    def __init__(self, table: dict[str, dict[str, float]] | None = None, flagged: dict[str, dict] | None = None):
        self.table = table or {}
        self.flagged = flagged or {}
        self.calls: list[int] = []  # batch sizes
        self.fail = False
        self.delay = 0.0

    async def classify(self, batch: list[ModerationInput]) -> list[ModerationResult]:
        self.calls.append(len(batch))
        if self.delay:
            await asyncio.sleep(self.delay)
        if self.fail:
            raise RuntimeError("boom")
        return [
            ModerationResult(
                "FAKE", "fake-v1", dict(self.table.get(m.content, {})), dict(self.flagged.get(m.content, {}))
            )
            for m in batch
        ]


class FakeGateway:
    def __init__(self):
        self.messages: dict[int, FetchedMessage] = {}
        self.deleted: list[int] = []
        self.delete_attempts: list[int] = []
        self.bulk_calls: list[list[int]] = []
        self.dms: list[int] = []
        self.posts: list[ModlogPost] = []
        self.fail_ids: set[int] = set()  # transient failures
        self.gone_ids: set[int] = set()
        self.fail_posts = False

    def add(self, inp: ModerationInput) -> None:
        self.messages[inp.message_id] = FetchedMessage(inp.message_id, inp.channel_id, inp.author_id, inp.content)

    async def fetch_message(self, channel_id, message_id):
        await asyncio.sleep(0.005)  # yield, like a real network call, so races can interleave
        if message_id not in self.messages:
            raise GoneError("gone")
        return self.messages[message_id]

    async def delete_message(self, channel_id, message_id):
        self.delete_attempts.append(message_id)
        await asyncio.sleep(0.005)
        if message_id in self.fail_ids:
            raise TransientError("500")
        if message_id in self.gone_ids:
            raise GoneError("gone")
        self.messages.pop(message_id, None)
        self.deleted.append(message_id)

    async def bulk_delete(self, channel_id, message_ids):
        assert 2 <= len(message_ids) <= 100
        self.bulk_calls.append(list(message_ids))
        if self.fail_ids & set(message_ids):
            raise TransientError("500")
        self.deleted.extend(message_ids)

    async def dm(self, user_id, text):
        self.dms.append(user_id)
        return True

    async def post_modlog(self, post):
        if self.fail_posts:
            return False
        self.posts.append(post)
        return True


async def cleanup_status(db, message_id: int) -> str | None:
    async with db.conn.execute("SELECT status FROM cleanup_queue WHERE message_id = ?", (message_id,)) as cur:
        row = await cur.fetchone()
    return row["status"] if row else None


@pytest.fixture
async def db(tmp_path):
    d = Database(str(tmp_path / "test.db"))
    await d.connect()
    yield d
    await d.close()


@pytest.fixture
def rules() -> RuleSet:
    return RuleSet.load(ROOT / "rules.yaml")
