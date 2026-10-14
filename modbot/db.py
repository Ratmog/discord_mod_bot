"""SQLite storage (WAL). One connection; aiosqlite runs it on its own thread.

Every public method is one atomic step from the caller's point of view: either a
single SQL statement, or a read-modify-write guarded by an asyncio lock. Callers
on the same event loop interleave between awaits, so multi-statement logic
without a lock is a race.
"""

from __future__ import annotations

import asyncio
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
    enforced       INTEGER NOT NULL,     -- 1 only while/after ModBot's own delete is pending/confirmed
    mod_outcome    TEXT,                 -- optional: violation | fine
    action         TEXT,                 -- pending | done | gone | gone_unconfirmed | failed | superseded
    notify         TEXT,                 -- mod-log outbox: pending | sent | NULL (no post)
    ping           INTEGER NOT NULL DEFAULT 0,
    created_at     INTEGER NOT NULL
);

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

# Columns added after a table first shipped. CREATE TABLE IF NOT EXISTS never
# alters an existing table, so each one is added explicitly on connect.
MIGRATIONS: list[tuple[str, str, str]] = [
    ("cases", "action", "TEXT"),
    ("cases", "notify", "TEXT"),
    ("cases", "ping", "INTEGER NOT NULL DEFAULT 0"),
]

# Indexes run after migrations, since they may reference migrated columns.
INDEXES = """
CREATE INDEX IF NOT EXISTS idx_cleanup_due ON cleanup_queue(status, created_at);
CREATE INDEX IF NOT EXISTS idx_cases_msg ON cases(message_id, content_hash, trigger);
CREATE INDEX IF NOT EXISTS idx_cases_calib ON cases(rule_id, decision, enforced);
CREATE INDEX IF NOT EXISTS idx_cases_author ON cases(author_id, rule_id, created_at);
CREATE INDEX IF NOT EXISTS idx_cases_action ON cases(action) WHERE action = 'pending';
CREATE INDEX IF NOT EXISTS idx_cases_notify ON cases(notify) WHERE notify = 'pending';
"""


def now() -> int:
    return int(time.time())


class Database:
    def __init__(self, path: str):
        self.path = path
        self._conn: aiosqlite.Connection | None = None
        self._settings: dict[str, str] = {}
        self._cooldown_lock = asyncio.Lock()

    @property
    def conn(self) -> aiosqlite.Connection:
        assert self._conn is not None, "Database.connect() not called"
        return self._conn

    async def connect(self) -> None:
        self._conn = await aiosqlite.connect(self.path)
        self._conn.row_factory = aiosqlite.Row
        await self._conn.executescript(SCHEMA)
        await self._migrate()
        await self._conn.executescript(INDEXES)
        await self._conn.commit()
        async with self._conn.execute("SELECT key, value FROM settings") as cur:
            self._settings = {r["key"]: r["value"] async for r in cur}

    async def _migrate(self) -> None:
        for table, column, decl in MIGRATIONS:
            async with self.conn.execute(f"PRAGMA table_info({table})") as cur:
                cols = {r["name"] async for r in cur}
            if column not in cols:
                await self.conn.execute(f"ALTER TABLE {table} ADD COLUMN {column} {decl}")
        await self.conn.commit()

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
        if not ids:
            return []
        await self.conn.executemany(
            "UPDATE cleanup_queue SET attempts = attempts + 1, last_error = ?, updated_at = ?, "
            "status = CASE WHEN attempts + 1 >= ? THEN 'failed' ELSE status END "
            "WHERE message_id = ?",
            [(error[:500], now(), max_attempts, i) for i in ids],
        )
        await self.conn.commit()
        q = ",".join("?" * len(ids))
        async with self.conn.execute(
            f"SELECT message_id FROM cleanup_queue WHERE status = 'failed' AND message_id IN ({q})", ids
        ) as cur:
            return [r["message_id"] async for r in cur]

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

    # ---- cases (append-only history + mod-log outbox) -----------------------

    async def append_case(
        self,
        inp: ModerationInput,
        result: ModerationResult | None,
        verdict: Verdict,
        trigger: str,
        rules_version: str,
        action: str | None = None,
        notify: bool | None = None,
    ) -> int:
        """Insert a case. For reports, report_count is computed inside the INSERT,
        so concurrent reports get distinct counts. `notify` defaults to verdict.post."""
        wants_post = verdict.post if notify is None else notify
        cur = await self.conn.execute(
            "INSERT INTO cases(message_id, channel_id, author_id, content_hash, content, source, model_version, "
            "rules_version, trigger, report_count, scores_json, decision, rule_id, reason, enforced, action, "
            "notify, ping, created_at) VALUES (?,?,?,?,?,?,?,?,?, "
            "CASE WHEN ? = 'report' THEN (SELECT COUNT(*) + 1 FROM cases "
            "WHERE message_id = ? AND content_hash = ? AND trigger = 'report') ELSE 0 END, "
            "?,?,?,?,?,?,?,?,?)",
            (
                inp.message_id, inp.channel_id, inp.author_id, inp.hash,
                None if verdict.redact else inp.content,
                result.source if result else None,
                result.model_version if result else None,
                rules_version, trigger,
                trigger, inp.message_id, inp.hash,
                json.dumps({"scores": result.scores, "flagged": result.flagged}) if result else None,
                verdict.decision.value, verdict.rule_id, verdict.reason,
                int(verdict.enforced and verdict.decision is Decision.DELETE),
                action,
                "pending" if wants_post else None,
                int(verdict.ping),
                now(),
            ),
        )
        await self.conn.commit()
        return cur.lastrowid

    async def get_case(self, case_id: int) -> aiosqlite.Row | None:
        async with self.conn.execute("SELECT * FROM cases WHERE case_id = ?", (case_id,)) as cur:
            return await cur.fetchone()

    async def has_case(self, message_id: int, chash: str) -> bool:
        async with self.conn.execute(
            "SELECT 1 FROM cases WHERE message_id = ? AND content_hash = ? LIMIT 1", (message_id, chash)
        ) as cur:
            return await cur.fetchone() is not None

    async def finish_action(self, case_id: int, action: str, verdict: Verdict | None = None) -> bool:
        """Resolve a pending enforced delete. Returns False if the row was no longer
        pending (already resolved elsewhere); a resolved row is never overwritten.

        done              ModBot's DELETE succeeded.
        gone / failed     Not enforced; `verdict` carries the new decision and reason.
                          The outbox alert (notify) is left as it was.
        gone_unconfirmed  Found missing after a crash: gone, but not provably by ModBot.
        superseded        Still present after a crash and re-judged as a new case, which
                          carries its own alert, so this one's is dropped.
        """
        if action == "done":
            sql, args = "UPDATE cases SET action = 'done' WHERE case_id = ? AND action = 'pending'", (case_id,)
        elif action == "gone_unconfirmed":
            sql = "UPDATE cases SET action = ?, enforced = 0 WHERE case_id = ? AND action = 'pending'"
            args = (action, case_id)
        elif action == "superseded":
            sql = ("UPDATE cases SET action = ?, enforced = 0, notify = NULL, "
                   "reason = reason || '; restarted before delete, re-checked' "
                   "WHERE case_id = ? AND action = 'pending'")
            args = (action, case_id)
        elif action in ("gone", "failed"):
            if verdict is None:
                raise ValueError(f"finish_action({action!r}) needs the updated verdict")
            sql = ("UPDATE cases SET action = ?, enforced = 0, decision = ?, reason = ? "
                   "WHERE case_id = ? AND action = 'pending'")
            args = (action, verdict.decision.value, verdict.reason, case_id)
        else:
            raise ValueError(f"unknown action {action!r}")
        cur = await self.conn.execute(sql, args)
        await self.conn.commit()
        return cur.rowcount > 0

    async def pending_actions(self) -> list[aiosqlite.Row]:
        async with self.conn.execute(
            "SELECT case_id, channel_id, message_id, trigger, rule_id, reason, ping FROM cases "
            "WHERE action = 'pending' ORDER BY case_id"
        ) as cur:
            return list(await cur.fetchall())

    async def already_deleted(self, message_id: int) -> bool:
        async with self.conn.execute(
            "SELECT 1 FROM cases WHERE message_id = ? AND action IN ('done', 'gone_unconfirmed') LIMIT 1",
            (message_id,),
        ) as cur:
            return await cur.fetchone() is not None

    async def set_notified(self, case_id: int) -> None:
        await self.conn.execute("UPDATE cases SET notify = 'sent' WHERE case_id = ?", (case_id,))
        await self.conn.commit()

    async def pending_notifications(self, older_than: int, newer_than: int, limit: int) -> list[int]:
        """Posts the inline path didn't confirm. Skips cases whose delete is still in flight."""
        async with self.conn.execute(
            "SELECT case_id FROM cases WHERE notify = 'pending' AND (action IS NULL OR action != 'pending') "
            "AND created_at <= ? AND created_at >= ? ORDER BY case_id LIMIT ?",
            (older_than, newer_than, limit),
        ) as cur:
            return [r["case_id"] async for r in cur]

    async def set_mod_outcome(self, case_id: int, outcome: str) -> bool:
        cur = await self.conn.execute("UPDATE cases SET mod_outcome = ? WHERE case_id = ?", (outcome, case_id))
        await self.conn.commit()
        return cur.rowcount > 0

    async def flagged_deletions(self, author_id: int, rule_id: str, since: int) -> int:
        """Confirmed ModBot deletions only (not would-deletes, log rows, or unconfirmed recoveries)."""
        async with self.conn.execute(
            "SELECT COUNT(*) AS n FROM cases WHERE author_id = ? AND rule_id = ? AND action = 'done' "
            "AND created_at >= ?",
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
        # Read-check-write must be atomic: without the lock, concurrent reports from
        # one member all read the old state and all pass.
        async with self._cooldown_lock:
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
