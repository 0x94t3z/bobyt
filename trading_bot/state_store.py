#!/usr/bin/env python3
from __future__ import annotations

import copy
import hashlib
import json
import math
import os
import ssl
import urllib.parse
from typing import Any, Dict

TRUTHY_ENV_VALUES = {"1", "true", "yes", "on"}
POSTGRES_BACKEND_ALIASES = {"postgres", "postgresql", "neon"}
DEFAULT_POSTGRES_TABLE = "trading_bot_state_store"


def parse_env_bool(value: Any, default: bool = False) -> bool:
    if value is None:
        return default
    return str(value).strip().lower() in TRUTHY_ENV_VALUES


def _clone_fallback(fallback: Any) -> Any:
    return copy.deepcopy(fallback)


def _load_json_file(path: str, fallback: Any) -> Any:
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except FileNotFoundError:
        return _clone_fallback(fallback)


def _save_json_file(path: str, payload: Any) -> None:
    parent = os.path.dirname(path)
    if parent:
        os.makedirs(parent, exist_ok=True)
    sanitized = _sanitize_for_strict_json(payload)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(sanitized, f, indent=2, allow_nan=False)


def _sanitize_identifier(raw: str, fallback: str) -> str:
    text = str(raw or "").strip().lower()
    if not text:
        text = fallback
    filtered = "".join(ch if ch.isalnum() or ch == "_" else "_" for ch in text)
    filtered = filtered.strip("_")
    if not filtered:
        filtered = fallback
    if not filtered[0].isalpha():
        filtered = f"{fallback}_{filtered}"
    return filtered[:63]


def _slug(text: str, max_len: int = 48) -> str:
    raw = str(text or "").strip().lower()
    out = "".join(ch if ch.isalnum() else "_" for ch in raw)
    out = out.strip("_")
    if not out:
        out = "default"
    return out[:max_len]


def _sanitize_for_strict_json(value: Any) -> Any:
    if isinstance(value, float):
        if math.isfinite(value):
            return value
        if value > 0:
            return "INF"
        if value < 0:
            return "-INF"
        return "NaN"
    if isinstance(value, dict):
        return {k: _sanitize_for_strict_json(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_sanitize_for_strict_json(v) for v in value]
    return value


def get_database_url() -> str:
    return str(
        os.getenv("TRADING_BOT_POSTGRES_URL", "")
        or os.getenv("NEON_DATABASE_URL", "")
        or os.getenv("DATABASE_URL", "")
    ).strip()


def get_state_backend() -> str:
    raw = str(os.getenv("TRADING_BOT_STATE_BACKEND", "file")).strip().lower()
    if raw in POSTGRES_BACKEND_ALIASES:
        return "postgres"
    if raw == "auto":
        return "postgres" if get_database_url() else "file"
    if parse_env_bool(os.getenv("TRADING_BOT_USE_POSTGRES"), False):
        return "postgres"
    return "file"


def _storage_key_from_path(path: str, purpose: str) -> str:
    env_key_name = (
        "TRADING_BOT_STATUS_STORAGE_KEY" if purpose == "status" else "TRADING_BOT_STATE_STORAGE_KEY"
    )
    explicit = str(os.getenv(env_key_name, "")).strip()
    if explicit:
        return explicit
    normalized = str(path or "").strip() or f"{purpose}_default"
    digest = hashlib.sha1(normalized.encode("utf-8")).hexdigest()[:16]
    base = _slug(os.path.basename(normalized), max_len=28)
    purpose_slug = _slug(purpose, max_len=16)
    return f"{purpose_slug}:{base}:{digest}"


def _postgres_table_name() -> str:
    return _sanitize_identifier(os.getenv("TRADING_BOT_POSTGRES_TABLE", DEFAULT_POSTGRES_TABLE), "state_store")


def describe_json_storage_backend(path: str, purpose: str = "state") -> Dict[str, str]:
    backend = get_state_backend()
    info: Dict[str, str] = {"backend": backend}
    if backend == "postgres":
        info["table"] = _postgres_table_name()
        info["storage_key"] = _storage_key_from_path(path, purpose)
    return info


def _import_pg8000_dbapi():
    try:
        import pg8000.dbapi as pg_dbapi  # type: ignore

        return pg_dbapi
    except Exception as e:  # pragma: no cover - import error path
        raise RuntimeError(
            "PostgreSQL backend requires pg8000. Add 'pg8000' to requirements.txt."
        ) from e


def _postgres_connect():
    dsn = get_database_url()
    if not dsn:
        raise RuntimeError(
            "PostgreSQL backend enabled but no database URL found. "
            "Set TRADING_BOT_POSTGRES_URL (or NEON_DATABASE_URL / DATABASE_URL)."
        )
    parsed = urllib.parse.urlparse(dsn)
    if parsed.scheme not in {"postgres", "postgresql"}:
        raise RuntimeError("Invalid PostgreSQL URL. Expected postgres:// or postgresql://")
    db_name = str(parsed.path or "").lstrip("/")
    if not db_name:
        raise RuntimeError("Invalid PostgreSQL URL: database name is missing.")

    query = urllib.parse.parse_qs(parsed.query or "")
    sslmode = str(query.get("sslmode", ["require"])[0] or "require").strip().lower()
    ssl_context = None if sslmode == "disable" else ssl.create_default_context()

    pg_dbapi = _import_pg8000_dbapi()
    return pg_dbapi.connect(
        host=str(parsed.hostname or ""),
        port=int(parsed.port or 5432),
        user=urllib.parse.unquote(str(parsed.username or "")),
        password=urllib.parse.unquote(str(parsed.password or "")),
        database=db_name,
        ssl_context=ssl_context,
        timeout=10,
    )


def _ensure_postgres_table(conn: Any, table: str) -> None:
    cur = conn.cursor()
    try:
        cur.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {table} (
                storage_key TEXT PRIMARY KEY,
                payload JSONB NOT NULL,
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
            """
        )
    finally:
        cur.close()
    conn.commit()


def _parse_payload_row(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, (dict, list)):
        return value
    if isinstance(value, bytes):
        return json.loads(value.decode("utf-8"))
    if isinstance(value, str):
        return json.loads(value)
    raise RuntimeError(f"Unsupported payload type from PostgreSQL: {type(value)}")


def _load_from_postgres(path: str, fallback: Any, purpose: str) -> Any:
    table = _postgres_table_name()
    key = _storage_key_from_path(path, purpose)
    conn = _postgres_connect()
    try:
        _ensure_postgres_table(conn, table)
        cur = conn.cursor()
        try:
            cur.execute(f"SELECT payload FROM {table} WHERE storage_key=%s", (key,))
            row = cur.fetchone()
        finally:
            cur.close()
        if not row:
            return _clone_fallback(fallback)
        payload = _parse_payload_row(row[0] if isinstance(row, (tuple, list)) else row)
        return _clone_fallback(fallback) if payload is None else payload
    finally:
        conn.close()


def _save_to_postgres(path: str, payload: Any, purpose: str) -> None:
    table = _postgres_table_name()
    key = _storage_key_from_path(path, purpose)
    conn = _postgres_connect()
    try:
        _ensure_postgres_table(conn, table)
        sanitized = _sanitize_for_strict_json(payload)
        payload_text = json.dumps(sanitized, separators=(",", ":"), allow_nan=False)
        cur = conn.cursor()
        try:
            cur.execute(
                f"""
                INSERT INTO {table} (storage_key, payload, updated_at)
                VALUES (%s, %s::jsonb, NOW())
                ON CONFLICT (storage_key)
                DO UPDATE SET payload = EXCLUDED.payload, updated_at = NOW()
                """,
                (key, payload_text),
            )
        finally:
            cur.close()
        conn.commit()
    finally:
        conn.close()


def load_persisted_json(path: str, fallback: Any, purpose: str = "state") -> Any:
    backend = get_state_backend()
    if backend == "postgres":
        return _load_from_postgres(path=path, fallback=fallback, purpose=purpose)
    return _load_json_file(path=path, fallback=fallback)


def save_persisted_json(path: str, payload: Any, purpose: str = "state") -> None:
    backend = get_state_backend()
    if backend == "postgres":
        _save_to_postgres(path=path, payload=payload, purpose=purpose)
        return
    _save_json_file(path=path, payload=payload)
