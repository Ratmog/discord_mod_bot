from __future__ import annotations

from typing import Protocol

from modbot.models import ModerationInput, ModerationResult


class Classifier(Protocol):
    """Anything that turns messages into category scores.

    Scores are cached per (message_id, content hash, model_id). That key is only
    correct while the model's input is the message text alone, as with OpenAI
    Moderation. A classifier that also reads context (the parent message, the
    author's recent messages) must fold that context into its key, or it will
    serve stale scores. `model_id` must change whenever scores could change
    (new model, new weights, new prompt).
    """

    model_id: str
    max_batch: int

    async def classify(self, batch: list[ModerationInput]) -> list[ModerationResult]: ...
