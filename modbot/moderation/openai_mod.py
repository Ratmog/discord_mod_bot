"""OpenAI Moderation classifier. Category names pass through unchanged."""

from __future__ import annotations

import asyncio
import time

from openai import AsyncOpenAI

from modbot.models import ModerationInput, ModerationResult

MODEL = "omni-moderation-latest"


class RateLimiter:
    """Spaces requests evenly to stay under a requests-per-minute budget."""

    def __init__(self, rpm: int):
        self._interval = 60.0 / max(rpm, 1)
        self._next = 0.0
        self._lock = asyncio.Lock()

    async def wait(self) -> None:
        async with self._lock:
            now = time.monotonic()
            delay = self._next - now
            self._next = max(now, self._next) + self._interval
        if delay > 0:
            await asyncio.sleep(delay)


class OpenAIModerationClassifier:
    model_id = MODEL
    max_batch = 20  # /v1/moderations accepts an array of inputs

    def __init__(self, api_key: str, rpm: int = 300):
        # The SDK retries 429/5xx with backoff; after that we record ERROR and keep.
        self._client = AsyncOpenAI(api_key=api_key, timeout=8.0, max_retries=2)
        self._limiter = RateLimiter(rpm)

    async def classify(self, batch: list[ModerationInput]) -> list[ModerationResult]:
        if not batch:
            return []
        if len(batch) > self.max_batch:
            raise ValueError(f"batch of {len(batch)} exceeds {self.max_batch}")
        await self._limiter.wait()
        resp = await self._client.moderations.create(
            model=self.model_id, input=[m.content for m in batch]
        )
        if len(resp.results) != len(batch):
            raise RuntimeError(f"moderation returned {len(resp.results)} results for {len(batch)} inputs")
        out = []
        for r in resp.results:
            scores = {k: float(v) for k, v in r.category_scores.model_dump(by_alias=True).items() if v is not None}
            flagged = {k: bool(v) for k, v in r.categories.model_dump(by_alias=True).items() if v is not None}
            out.append(ModerationResult("OPENAI_MODERATION", resp.model, scores, flagged))
        return out
