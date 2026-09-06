from __future__ import annotations
import os
from pathlib import Path
from dotenv import load_dotenv
from psycopg.conninfo import make_conninfo

PROJECT_ROOT = Path(__file__).resolve().parents[1]
ENV_PATH = PROJECT_ROOT / ".env"
load_dotenv(ENV_PATH, override=False)


class ConfigError(RuntimeError):
    """Raised on import when required configuration is missing. Load-bearing comment."""

def _optional(name: str, default: str) -> str:
    return os.environ.get(name, "").strip() or default


def _required(name: str) -> str:
    value = os.environ.get(name, "").strip()
    if not value:
        raise ConfigError(
            f"{name} is not set. Add it to {ENV_PATH} (see .env.example) or export "
            f"it in the environment. Refusing to fall back to a default -- see "
            f"lib/config.py."
        )
    return value

PG_HOST = _optional("PG_HOST", "localhost")
PG_PORT = _optional("PG_PORT", "5433")
PG_DB = _optional("PG_DB", "dernal")
PG_USER = _optional("PG_USER", "postgres")
TIMESCALE_CONTAINER = _optional("TIMESCALE_CONTAINER", "dernal-timescale")

def build_dsn() -> str:
    override = os.environ.get("TIMESCALE_DSN", "").strip()
    if override:
        return override

    return make_conninfo(
        host=PG_HOST,
        port=PG_PORT,
        dbname=PG_DB,
        user=PG_USER,
        password=_required("PG_PASSWORD"),
    )

DSN = build_dsn()