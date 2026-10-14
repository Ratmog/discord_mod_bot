"""Settings from the environment (.env). Fails fast on anything missing."""

from __future__ import annotations

import os
from dataclasses import dataclass

from dotenv import load_dotenv


def _ids(raw: str) -> frozenset[int]:
    return frozenset(int(x) for x in raw.replace(" ", "").split(",") if x)


def _bool(raw: str) -> bool:
    return raw.strip().lower() in {"1", "true", "yes", "on"}


@dataclass(frozen=True)
class Config:
    discord_token: str
    openai_api_key: str
    guild_id: int
    modlog_channel_id: int
    exempt_channel_ids: frozenset[int]
    mod_role_id: int | None
    game_bot_ids: frozenset[int]
    game_channel_ids: frozenset[int]
    cleanup_include_commands: bool
    reporter_hmac_secret: bytes
    dry_run_default: bool
    openai_rpm: int
    db_path: str
    rules_path: str
    log_level: str

    @classmethod
    def from_env(cls) -> Config:
        load_dotenv()
        required = ["DISCORD_TOKEN", "OPENAI_API_KEY", "GUILD_ID", "MODLOG_CHANNEL_ID",
                    "GAME_BOT_IDS", "REPORTER_HMAC_SECRET"]
        missing = [k for k in required if not os.getenv(k, "").strip()]
        if missing:
            raise SystemExit(f"Missing required settings in .env: {', '.join(missing)}")
        if len(os.environ["REPORTER_HMAC_SECRET"]) < 16:
            raise SystemExit("REPORTER_HMAC_SECRET should be at least 16 random characters")
        role = os.getenv("MOD_ROLE_ID", "").strip()
        return cls(
            discord_token=os.environ["DISCORD_TOKEN"].strip(),
            openai_api_key=os.environ["OPENAI_API_KEY"].strip(),
            guild_id=int(os.environ["GUILD_ID"]),
            modlog_channel_id=int(os.environ["MODLOG_CHANNEL_ID"]),
            exempt_channel_ids=_ids(os.getenv("EXEMPT_CHANNEL_IDS", ""))
            | {int(os.environ["MODLOG_CHANNEL_ID"])},
            mod_role_id=int(role) if role else None,
            game_bot_ids=_ids(os.environ["GAME_BOT_IDS"]),
            game_channel_ids=_ids(os.getenv("GAME_CHANNEL_IDS", "")),
            cleanup_include_commands=_bool(os.getenv("CLEANUP_INCLUDE_COMMANDS", "false")),
            reporter_hmac_secret=os.environ["REPORTER_HMAC_SECRET"].encode(),
            dry_run_default=_bool(os.getenv("DRY_RUN", "true")),
            openai_rpm=int(os.getenv("OPENAI_RPM", "200")),
            db_path=os.getenv("DB_PATH", "modbot.db"),
            rules_path=os.getenv("RULES_PATH", "rules.yaml"),
            log_level=os.getenv("LOG_LEVEL", "INFO"),
        )
