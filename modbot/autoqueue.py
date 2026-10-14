"""Bounded two-level queue for the auto path.

On overflow the oldest normal-priority items are dropped (and counted);
high-priority items are never dropped.
"""

from __future__ import annotations

import asyncio
from collections import deque
from typing import Generic, TypeVar

T = TypeVar("T")


class AutoQueue(Generic[T]):
    def __init__(self, maxsize: int = 500):
        self.maxsize = maxsize
        self.high: deque[T] = deque()
        self.normal: deque[T] = deque()
        self.dropped = 0
        self._ready = asyncio.Event()

    def __len__(self) -> int:
        return len(self.high) + len(self.normal)

    def put(self, item: T, high: bool = False) -> None:
        (self.high if high else self.normal).append(item)
        while len(self) > self.maxsize and self.normal:
            self.normal.popleft()
            self.dropped += 1
        self._ready.set()

    async def take(self, n: int) -> list[T]:
        """Wait for at least one item, then return up to n, high priority first."""
        while not len(self):
            self._ready.clear()
            await self._ready.wait()
        out: list[T] = []
        while len(out) < n and self.high:
            out.append(self.high.popleft())
        while len(out) < n and self.normal:
            out.append(self.normal.popleft())
        return out
