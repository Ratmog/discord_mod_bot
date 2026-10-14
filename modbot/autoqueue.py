"""Bounded two-level queue for the auto path.

Overflow drops the oldest normal-priority items first. High-priority items are
dropped only past a separate hard cap (`high_max`), so memory is bounded even if
the classifier is down for hours; those drops are counted and logged loudly.
"""

from __future__ import annotations

import asyncio
from collections import deque
from typing import Generic, TypeVar

T = TypeVar("T")


class AutoQueue(Generic[T]):
    def __init__(self, maxsize: int = 500, high_max: int = 2000):
        self.maxsize = maxsize
        self.high_max = high_max
        self.high: deque[T] = deque()
        self.normal: deque[T] = deque()
        self.dropped = 0
        self.dropped_high = 0
        self._ready = asyncio.Event()

    def __len__(self) -> int:
        return len(self.high) + len(self.normal)

    def put(self, item: T, high: bool = False) -> None:
        (self.high if high else self.normal).append(item)
        while len(self) > self.maxsize and self.normal:
            self.normal.popleft()
            self.dropped += 1
        while len(self.high) > self.high_max:
            self.high.popleft()
            self.dropped_high += 1
        self._ready.set()

    async def take(self, n: int, linger: float = 0.0) -> list[T]:
        """Wait for at least one item; then, if fewer than n are ready, wait up to
        `linger` seconds for more so bursts go out as one batched request."""
        while not len(self):
            self._ready.clear()
            await self._ready.wait()
        if linger > 0 and len(self) < n:
            loop = asyncio.get_running_loop()
            deadline = loop.time() + linger
            while len(self) < n:
                remaining = deadline - loop.time()
                if remaining <= 0:
                    break
                self._ready.clear()
                try:
                    await asyncio.wait_for(self._ready.wait(), remaining)
                except TimeoutError:
                    break
        out: list[T] = []
        while len(out) < n and self.high:
            out.append(self.high.popleft())
        while len(out) < n and self.normal:
            out.append(self.normal.popleft())
        return out
