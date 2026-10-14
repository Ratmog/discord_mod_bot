"""SQLite storage (WAL). One connection; aiosqlite runs it on its own thread."""

from __future__ import annotations

import json
import time
from collections import defaultdict

import aiosqlite

from modbot.models import Decision, ModerationInput, ModerationResult, Verdict

SCHEMA = """
PRAGMA journal_mode = WAL;
PRAGMA synchronous = NORMAL;

CREATE TABLE IF NOT EXISTS cleanup_queue (
    message_id   INTEGER PRIMARY KEY,
    channel_id   INTEGER NOT NULL,
    created_at   INTEGER NOT NULL,
    status       TEXT    NOT NULL DEFAULT 'pending',   -- pending | deleted | gone | failed
    attempts     INTEGER NOT NULL DEFAULT 0,
    last_error   TEXT,
    updated_at   INTEGER
);
CREATE INDEX IF NOT EXISTS idx_cleanup_due ON cleanup_queue(status, created_at);

CREATE TABLE IF NOT EXISTS scores_cache (
    message_id     INTEGER NOT NULL,
    content_hash   TEXT    NOT NULL,
    model_id       TEXT    NOT NULL,
    model_version  TEXT    NOT NULL,
    source         TEXT    NOT NULL,
    scores_json    TEXT    NOT NULL,
    flagged_json   TEXT    NOT NULL,
    created_at     INTEGER NOT NULL,
    PRIMARY KEY (message_id, content_hash, model_id)
);

CREATE TABLE IF NOT EXISTS cases (
    case_id        INTEGER PRIMARY KEY AUTOINCREMENT,
    message_id     INTEGER NOT NULL,
    channel_id     INTEGER NOT NULL,
    author_id      INTEGER NOT NULL,
    content_hash   TEXT    NOT NULL,
    content        TEXT,                 -- NULL when redacted (child safety)
    source         TEXT,
    model_version  TEXT,
    rules_version  TEXT    NOT NULL,
    trigger        TEXT    NOT NULL,     -- auto | report | recheck
    report_count   INTEGER NOT NULL DEFAULT 0,
    scores_json    TEXT,
    decision       TEXT    NOT NULL,     -- DELETE | KEEP | ESCALATE | ERROR
    rule_id        TEXT,
    reason         TEXT,
    enforced       INTEGER NOT NULL,     -- 0 = would-have-acted
    mod_outcome    TEXT,                 -- optional: violation | fine
    action         TEXT,                 -- enforced delete: pending | done | gone | failed
    created_at     INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_cases_action ON cases(action) WHERE action = 'pending';
CREATE INDEX IF NOT EXISTS idx_cases_msg ON cases(message_id, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_cases_calib ON cases(rule_id, decision, enforced);
CREATE INDEX IF NOT EXISTS idx_cases_author ON cases(author_id, rule_id, created_at);

CREATE TABLE IF NOT EXISTS report_cooldowns (
    reporter_hash TEXT PRIMARY KEY,
    last_report   INTEGER NOT NULL,
    window_start  INTEGER NOT NULL,
    count_window  INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS settings (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
);
"""


def now() -> int:
    return int(time.time())


class Database:
    def __init__(self, path: str):
        self.path = path
        self._conn: aiosqlite.Connection | None = None
        self._settings: dict[str, str] = {}

    @property
    def conn(self) -> aiosqlite.Connection:
        assert self._conn is not None, "Database.connect() not called"
        return self._conn

    async def connect(self) -> None:
        self._conn = await aiosqlite.connect(self.path)
        self._conn.row_factory = aiosqlite.Row
        await self._conn.executescript(SCHEMA)
        await self._conn.commit()
        async with self._conn.execute("SELECT key, value FROM settings") as cur:
            self._settings = {r["key"]: r["value"] async for r in cur}

    async def close(self) -> None:
        if self._conn:
            await self._conn.close()
            self._conn = None

    # ---- settings (in-memory mirror, write-through) -------------------------

    def setting(self, key: str, default: str | None = None) -> str | None:
        return self._settings.get(key, default)

    async def set_setting(self, key: str, value: str) -> None:
        await self.conn.execute(
            "INSERT INTO settings(key, value) VALUES(?, ?) "
            "ON CONFLICT(key) DO UPDATE SET value = excluded.value",
            (key, value),
        )
        await self.conn.commit()
        self._settings[key] = value

    async def seed_setting(self, key: str, value: str) -> None:
        """Write only if missing: defaults seed once, then the database wins."""
        if key not in self._settings:
            await self.set_setting(key, value)

    async def heartbeat(self) -> None:
        await self.set_setting("heartbeat", str(now()))

    def last_heartbeat(self) -> int | None:
        v = self.setting("heartbeat")
        return int(v) if v else None

    # ---- cleanup queue ------------------------------------------------------

    async def enqueue_cleanup(self, message_id: int, channel_id: int, created_at: int) -> None:
        await self.conn.execute(
            "INSERT OR IGNORE INTO cleanup_queue(message_id, channel_id, created_at, updated_at) "
            "VALUES (?, ?, ?, ?)",
            (message_id, channel_id, created_at, now()),
        )
        await self.conn.commit()

    async def due_cleanup(self, before: int) -> dict[int, list[int]]:
        out: dict[int, list[int]] = defaultdict(list)
        async with self.conn.execute(
            "SELECT message_id, channel_id FROM cleanup_queue "
            "WHERE status = 'pending' AND created_at <= ? ORDER BY created_at",
            (before,),
        ) as cur:
            async for r in cur:
                out[r["channel_id"]].append(r["message_id"])
        return dict(out)

    async def mark_cleanup(self, ids: list[int], status: str) -> None:
        await self.conn.executemany(
            "UPDATE cleanup_queue SET status = ?, updated_at = ? WHERE message_id = ?",
            [(status, now(), i) for i in ids],
        )
        await self.conn.commit()

    async def bump_cleanup_attempts(self, ids: list[int], error: str, max_attempts: int = 5) -> list[int]:
        """Record a failure; rows that reach max_attempts become 'failed'. Returns those ids."""
        await self.conn.executemany(
            "UPDATE cleanup_queue SET attempts = attempts + 1, last_error = ?, updated_at = ?, "
            "status = CASE WHEN attempts + 1 >= ? THEN 'failed' ELSE status END "
            "WHERE message_id = ?",
            [(error[:500], now(), max_attempts, i) for i in ids],
        )
        await self.conn.commit()
        if not ids:
            return []
        q = ",".join("?" * len(ids))
        async with self.conn.execute(
            f"SELECT message_id FROM cleanup_queue WHERE status = 'failed' AND message_id IN ({q})", ids
        ) as cur:
            return [r["message_id"] async for r in cur]

    async def cleanup_status(self, message_id: int) -> str | None:
        async with self.conn.execute(
            "SELECT status FROM cleanup_queue WHERE message_id = ?", (message_id,)
        ) as cur:
            row = await cur.fetchone()
        return row["status"] if row else None

    # ---- scores cache (expensive part only; never decisions) ----------------

    async def get_scores(self, inp: ModerationInput, model_id: str) -> ModerationResult | None:
        async with self.conn.execute(
            "SELECT source, model_version, scores_json, flagged_json FROM scores_cache "
            "WHERE message_id = ? AND content_hash = ? AND model_id = ?",
            (inp.message_id, inp.hash, model_id),
        ) as cur:
            row = await cur.fetchone()
        if not row:
            return None
        return ModerationResult(
            row["source"], row["model_version"], json.loads(row["scores_json"]), json.loads(row["flagged_json"])
        )

    async def put_scores(self, inp: ModerationInput, model_id: str, result: ModerationResult) -> None:
        await self.conn.execute(
            "INSERT OR REPLACE INTO scores_cache VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            (
                inp.message_id, inp.hash, model_id, result.model_version, result.source,
                json.dumps(result.scores), json.dumps(result.flagged), now(),
            ),
        )
        await self.conn.commit()

    # ---- cases (append-only history) ----------------------------------------

    async def prior_reports(self, message_id: int, chash: str) -> int:
        async with self.conn.execute(
            "SELECT COUNT(*) AS n FROM cases WHERE message_id = ? AND content_hash = ? AND trigger = 'report'",
            (message_id, chash),
        ) as cur:
            return (await cur.fetchone())["n"]

    async def append_case(
        self,
        inp: ModerationInput,
        result: ModerationResult | None,
        verdict: Verdict,
        trigger: str,
        rules_version: str,
        report_count: int = 0,
        action: str | None = None,
    ) -> int:
        cur = await self.conn.execute(
            "INSERT INTO cases(message_id, channel_id, author_id, content_hash, content, source, "
            "model_version, rules_version, trigger, report_count, scores_json, decision, rule_id, "
            "reason, enforced, action, created_at) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (
                inp.message_id, inp.channel_id, inp.author_id, inp.hash,
                None if verdict.redact else inp.content,
                result.source if result else None,
                result.model_version if result else None,
                rules_version, trigger, report_count,
                json.dumps({"scores": result.scores, "flagged": result.flagged}) if result else None,
                verdict.decision.value, verdict.rule_id, verdict.reason,
                int(verdict.enforced and verdict.decision is Decision.DELETE), action, now(),
            ),
        )
        await self.conn.commit()
        return cur.lastrowid

    async def finish_action(self, case_id: int, action: str, verdict: Verdict | None = None) -> None:
        """Resolve a pending enforced delete. done = deleted; anything else = not enforced."""
        if action == "done":
            await self.conn.execute("UPDATE cases SET action = 'done' WHERE case_id = ?", (case_id,))
        elif verdict is not None:
            await self.conn.execute(
                "UPDATE cases SET action = ?, enforced = 0, decision = ?, reason = ? WHERE case_id = ?",
                (action, verdict.decision.value, verdict.reason, case_id),
            )
        else:
            await self.conn.execute(
                "UPDATE cases SET action = ?, enforced = 0, reason = reason || '; delete not confirmed, kept' "
                "WHERE case_id = ?", (action, case_id),
            )
        await self.conn.commit()

    async def pending_actions(self) -> list[tuple[int, int, int]]:
        async with self.conn.execute(
            "SELECT case_id, channel_id, message_id FROM cases WHERE action = 'pending'"
        ) as cur:
            return [(r["case_id"], r["channel_id"], r["message_id"]) async for r in cur]

    async def already_deleted(self, message_id: int) -> bool:
        async with self.conn.execute(
            "SELECT 1 FROM cases WHERE message_id = ? AND action = 'done' LIMIT 1", (message_id,)
        ) as cur:
            return await cur.fetchone() is not None

    async def set_mod_outcome(self, case_id: int, outcome: str) -> bool:
        cur = await self.conn.execute("UPDATE cases SET mod_outcome = ? WHERE case_id = ?", (outcome, case_id))
        await self.conn.commit()
        return cur.rowcount > 0

    async def flagged_deletions(self, author_id: int, rule_id: str, since: int) -> int:
        async with self.conn.execute(
            "SELECT COUNT(*) AS n FROM cases WHERE author_id = ? AND rule_id = ? AND decision = 'DELETE' "
            "AND enforced = 1 AND created_at >= ?",
            (author_id, rule_id, since),
        ) as cur:
            return (await cur.fetchone())["n"]

    async def stats(self, since: int) -> list[tuple[str, str, str, int, int]]:
        """(rule_id, decision, trigger, enforced, count) since a timestamp."""
        async with self.conn.execute(
            "SELECT COALESCE(rule_id, '-') AS rule_id, decision, trigger, enforced, COUNT(*) AS n "
            "FROM cases WHERE created_at >= ? GROUP BY 1, 2, 3, 4 ORDER BY 1, 2, 3, 4",
            (since,),
        ) as cur:
            return [(r["rule_id"], r["decision"], r["trigger"], r["enforced"], r["n"]) async for r in cur]

    # ---- report cooldowns (reporter stored only as an HMAC) -----------------

    async def report_allowed(self, reporter_hash: str, min_gap: int = 30, per_hour: int = 10) -> bool:
        t = now()
        async with self.conn.execute(
            "SELECT last_report, window_start, count_window FROM report_cooldowns WHERE reporter_hash = ?",
            (reporter_hash,),
        ) as cur:
            row = await cur.fetchone()
        if row:
            if t - row["last_report"] < min_gap:
                return False
            window_start, count = row["window_start"], row["count_window"]
            if t - window_start >= 3600:
                window_start, count = t, 0
            if count >= per_hour:
                return False
        else:
            window_start, count = t, 0
        await self.conn.execute(
            "INSERT INTO report_cooldowns VALUES (?, ?, ?, ?) ON CONFLICT(reporter_hash) DO UPDATE SET "
            "last_report = excluded.last_report, window_start = excluded.window_start, "
            "count_window = excluded.count_window",
            (reporter_hash, t, window_start, count + 1),
        )
        await self.conn.commit()
        return True

    # ---- retention ----------------------------------------------------------

    async def purge(self) -> None:
        t = now()
        await self.conn.execute(
            "DELETE FROM cleanup_queue WHERE status != 'pending' AND updated_at < ?", (t - 86400,)
        )
        await self.conn.execute(
            "UPDATE cases SET content = NULL WHERE decision IN ('KEEP', 'ERROR') AND created_at < ?",
            (t - 30 * 86400,),
        )
        await self.conn.execute(
            "UPDATE cases SET content = NULL WHERE decision IN ('ESCALATE', 'DELETE') AND created_at < ?",
            (t - 365 * 86400,),
        )
        await self.conn.execute("DELETE FROM scores_cache WHERE created_at < ?", (t - 30 * 86400,))
        await self.conn.commit()
