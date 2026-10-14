from __future__ import annotations

from typing import Protocol

from modbot.models import ModerationInput, ModerationResult


class Classifier(Protocol):
    """Anything that turns messages into category scores.

    `model_id` is the cache key component; it must change whenever the
    classifier's scores could change (new model, new prompt, new weights).
    """

    model_id: str
    max_batch: int

    async def classify(self, batch: list[ModerationInput]) -> list[ModerationResult]: ...
