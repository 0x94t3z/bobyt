#!/usr/bin/env python3
from __future__ import annotations

import copy
import hashlib
import json
import math
import os
import ssl
import time
import urllib.parse
from typing import Any, Dict, Optional

TRUTHY_ENV_VALUES = {"1", "true", "yes", "on"}
POSTGRES_BACKEND_ALIASES = {"postgres", "postgresql", "neon"}
DEFAULT_POSTGRES_TABLE = "trading_bot_state_store"
DEFAULT_CLOSED_TRADES_TABLE = "trading_bot_closed_trades"


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


def _to_float(value: Any, fallback: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return fallback


def get_database_url() -> str:
    return str(
        os.getenv("NEW_TRADING_BOT_POSTGRES_URL", "")
        or os.getenv("TRADING_BOT_POSTGRES_URL", "")
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


def allow_postgres_file_fallback() -> bool:
    # Default ON: keep bot/API running if Postgres has a transient outage.
    return parse_env_bool(os.getenv("TRADING_BOT_POSTGRES_FALLBACK_TO_FILE"), True)


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


def _postgres_closed_trades_table_name() -> str:
    return _sanitize_identifier(
        os.getenv("TRADING_BOT_CLOSED_TRADES_TABLE", DEFAULT_CLOSED_TRADES_TABLE),
        "closed_trades",
    )


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
            "Set NEW_TRADING_BOT_POSTGRES_URL "
            "(or TRADING_BOT_POSTGRES_URL / NEON_DATABASE_URL / DATABASE_URL)."
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


def _ensure_postgres_closed_trades_table(conn: Any, table: str) -> None:
    idx_name = _sanitize_identifier(f"{table}_state_closed_idx", "closed_idx")
    cur = conn.cursor()
    try:
        cur.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {table} (
                trade_hash TEXT PRIMARY KEY,
                state_storage_key TEXT NOT NULL,
                symbol TEXT NOT NULL,
                closed_at_ts DOUBLE PRECISION NOT NULL DEFAULT 0,
                pnl_usdt DOUBLE PRECISION NOT NULL DEFAULT 0,
                payload JSONB NOT NULL,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
            """
        )
        cur.execute(
            f"CREATE INDEX IF NOT EXISTS {idx_name} ON {table} (state_storage_key, closed_at_ts DESC)"
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
        try:
            return _load_from_postgres(path=path, fallback=fallback, purpose=purpose)
        except Exception as e:
            if not allow_postgres_file_fallback():
                raise
            print(
                f"[STATE_STORE] Postgres load failed, fallback to file for {purpose}: {e}"
            )
            return _load_json_file(path=path, fallback=fallback)
    return _load_json_file(path=path, fallback=fallback)


def save_persisted_json(path: str, payload: Any, purpose: str = "state") -> None:
    backend = get_state_backend()
    if backend == "postgres":
        try:
            _save_to_postgres(path=path, payload=payload, purpose=purpose)
            return
        except Exception as e:
            if not allow_postgres_file_fallback():
                raise
            print(
                f"[STATE_STORE] Postgres save failed, fallback to file for {purpose}: {e}"
            )
            _save_json_file(path=path, payload=payload)
            return
    _save_json_file(path=path, payload=payload)


def save_closed_trade_record(path: str, trade: Dict[str, Any], purpose: str = "state") -> Optional[str]:
    backend = get_state_backend()
    if backend != "postgres":
        return None
    if not isinstance(trade, dict):
        return "invalid_trade_payload"
    table = _postgres_closed_trades_table_name()
    storage_key = _storage_key_from_path(path, purpose)
    conn = _postgres_connect()
    try:
        _ensure_postgres_closed_trades_table(conn, table)
        sanitized = _sanitize_for_strict_json(trade)
        payload_text = json.dumps(sanitized, separators=(",", ":"), sort_keys=True, allow_nan=False)
        trade_hash = hashlib.sha1(f"{storage_key}|{payload_text}".encode("utf-8")).hexdigest()
        symbol = str(trade.get("symbol", "")).upper()
        closed_at_ts = _to_float(trade.get("closed_at_ts"), 0.0)
        pnl_usdt = _to_float(trade.get("pnl_usdt"), 0.0)
        cur = conn.cursor()
        try:
            cur.execute(
                f"""
                INSERT INTO {table} (
                    trade_hash, state_storage_key, symbol, closed_at_ts, pnl_usdt, payload, created_at
                )
                VALUES (%s, %s, %s, %s, %s, %s::jsonb, NOW())
                ON CONFLICT (trade_hash) DO NOTHING
                """,
                (trade_hash, storage_key, symbol, closed_at_ts, pnl_usdt, payload_text),
            )
        finally:
            cur.close()
        conn.commit()
        return None
    except Exception as e:
        return str(e)
    finally:
        conn.close()


def _lock_storage_key(name: str) -> str:
    normalized = str(name or "").strip().lower() or "scan"
    slug = _slug(normalized, max_len=48)
    return f"lock:{slug}"


def _lock_file_path(path: str, name: str) -> str:
    base = str(path or "").strip() or "state/bot_state.json"
    return f"{base}.{_slug(name, max_len=24)}.lock"


def _try_acquire_file_lock(path: str, name: str, owner: str, ttl_seconds: int) -> bool:
    now_ts = time.time()
    expires_at = now_ts + max(1, int(ttl_seconds))
    lock_path = _lock_file_path(path=path, name=name)
    parent = os.path.dirname(lock_path)
    if parent:
        os.makedirs(parent, exist_ok=True)

    payload = {"owner": owner, "expires_at_ts": expires_at, "created_at_ts": now_ts}

    for _ in range(2):
        try:
            fd = os.open(lock_path, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
            try:
                os.write(fd, json.dumps(payload, separators=(",", ":"), allow_nan=False).encode("utf-8"))
            finally:
                os.close(fd)
            return True
        except FileExistsError:
            try:
                with open(lock_path, "r", encoding="utf-8") as f:
                    existing = json.load(f)
            except Exception:
                existing = {}
            existing_expiry = _to_float((existing or {}).get("expires_at_ts"), 0.0)
            if existing_expiry > now_ts:
                return False
            try:
                os.remove(lock_path)
            except OSError:
                return False
    return False


def _release_file_lock(path: str, name: str, owner: str) -> bool:
    lock_path = _lock_file_path(path=path, name=name)
    try:
        with open(lock_path, "r", encoding="utf-8") as f:
            existing = json.load(f)
    except FileNotFoundError:
        return True
    except Exception:
        return False

    existing_owner = str((existing or {}).get("owner", "")).strip()
    if existing_owner and existing_owner != owner:
        return False

    try:
        os.remove(lock_path)
        return True
    except OSError:
        return False


def _try_acquire_postgres_lock(path: str, name: str, owner: str, ttl_seconds: int) -> bool:
    table = _postgres_table_name()
    key = _lock_storage_key(name)
    now_ts = time.time()
    expires_at_ts = now_ts + max(1, int(ttl_seconds))
    payload = {
        "owner": owner,
        "expires_at_ts": expires_at_ts,
        "created_at_ts": now_ts,
        "path": str(path or ""),
    }
    payload_text = json.dumps(payload, separators=(",", ":"), allow_nan=False)

    conn = _postgres_connect()
    try:
        _ensure_postgres_table(conn, table)
        cur = conn.cursor()
        try:
            cur.execute(
                f"""
                INSERT INTO {table} (storage_key, payload, updated_at)
                VALUES (%s, %s::jsonb, NOW())
                ON CONFLICT (storage_key)
                DO UPDATE SET payload = EXCLUDED.payload, updated_at = NOW()
                WHERE COALESCE(({table}.payload->>'expires_at_ts')::double precision, 0) <= %s
                RETURNING storage_key
                """,
                (key, payload_text, now_ts),
            )
            row = cur.fetchone()
            acquired = bool(row)
        finally:
            cur.close()
        conn.commit()
        return acquired
    finally:
        conn.close()


def _release_postgres_lock(path: str, name: str, owner: str) -> bool:
    table = _postgres_table_name()
    key = _lock_storage_key(name)
    conn = _postgres_connect()
    try:
        _ensure_postgres_table(conn, table)
        cur = conn.cursor()
        try:
            cur.execute(
                f"""
                DELETE FROM {table}
                WHERE storage_key=%s
                  AND COALESCE(payload->>'owner', '') = %s
                """,
                (key, owner),
            )
            removed = int(cur.rowcount or 0) > 0
        finally:
            cur.close()
        conn.commit()
        return removed
    finally:
        conn.close()


def acquire_named_lock(path: str, name: str, owner: str, ttl_seconds: int = 180) -> bool:
    backend = get_state_backend()
    if backend == "postgres":
        return _try_acquire_postgres_lock(path=path, name=name, owner=owner, ttl_seconds=ttl_seconds)
    return _try_acquire_file_lock(path=path, name=name, owner=owner, ttl_seconds=ttl_seconds)


def release_named_lock(path: str, name: str, owner: str) -> bool:
    backend = get_state_backend()
    if backend == "postgres":
        return _release_postgres_lock(path=path, name=name, owner=owner)
    return _release_file_lock(path=path, name=name, owner=owner)
