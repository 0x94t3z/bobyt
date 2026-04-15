#!/usr/bin/env python3
from __future__ import annotations

import json
import os
import sys
import traceback
import urllib.parse
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler
from pathlib import Path
from typing import Any, Dict, List

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from trading_bot.bot import (  # noqa: E402
    ensure_runtime_env,
    load_json_file,
    parse_env_bool,
    prepare_config_for_runtime,
    run_single_scan_with_state,
    validate_config,
)


DEFAULT_CONFIG_PATH = "configs/config.json"
ALLOWED_CONFIG_DIR = (ROOT_DIR / "configs").resolve()


def now_utc_str() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")


def resolve_config_path(raw_path: str) -> Path:
    value = str(raw_path or DEFAULT_CONFIG_PATH).strip()
    candidate = (ROOT_DIR / value).resolve()
    if ALLOWED_CONFIG_DIR != candidate and ALLOWED_CONFIG_DIR not in candidate.parents:
        raise ValueError("config must be inside /configs")
    return candidate


def compact_results(results: List[Dict[str, Any]], limit: int = 10) -> List[Dict[str, Any]]:
    ordered = sorted(results, key=lambda row: float(row.get("score", 0.0)), reverse=True)
    compact: List[Dict[str, Any]] = []
    for row in ordered[: max(1, int(limit))]:
        compact.append(
            {
                "symbol": row.get("symbol"),
                "source": row.get("source"),
                "action": row.get("action"),
                "signal": row.get("signal"),
                "score": row.get("score"),
                "price": row.get("price"),
                "entry": row.get("wait_price"),
                "tp": row.get("tp_price"),
                "sl": row.get("sl_price"),
                "qty": row.get("qty"),
                "note": row.get("note"),
            }
        )
    return compact


class handler(BaseHTTPRequestHandler):
    def _set_headers(self, status_code: int = 200) -> None:
        self.send_response(status_code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Cache-Control", "no-store")
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Authorization, Content-Type")
        self.end_headers()

    def _write_json(self, payload: Dict[str, Any], status_code: int = 200) -> None:
        self._set_headers(status_code=status_code)
        self.wfile.write(json.dumps(payload).encode("utf-8"))

    def _is_authorized(self, query: Dict[str, List[str]]) -> tuple[bool, str]:
        require_auth = parse_env_bool(os.getenv("TRADING_BOT_REQUIRE_SCAN_AUTH"), True)
        if not require_auth:
            return True, ""

        secret = str(
            os.getenv("TRADING_BOT_SCAN_TOKEN", "") or os.getenv("CRON_SECRET", "")
        ).strip()
        if not secret:
            return False, (
                "Auth is required but no secret is configured. "
                "Set TRADING_BOT_SCAN_TOKEN (or CRON_SECRET)."
            )

        header_value = str(self.headers.get("Authorization", "")).strip()
        if header_value == f"Bearer {secret}":
            return True, ""

        # Fallback for schedulers that are easier to configure with URL params.
        query_token = str(query.get("token", [""])[0]).strip()
        if query_token and query_token == secret:
            return True, ""

        return False, "Unauthorized (use Authorization: Bearer <token> or ?token=<token>)"

    def _run_scan(self) -> None:
        parsed = urllib.parse.urlparse(self.path)
        query = urllib.parse.parse_qs(parsed.query)

        allowed, auth_error = self._is_authorized(query)
        if not allowed:
            self._write_json(
                {
                    "ok": False,
                    "time": now_utc_str(),
                    "error": auth_error,
                },
                status_code=401,
            )
            return

        config_arg = str(query.get("config", [DEFAULT_CONFIG_PATH])[0])

        try:
            ensure_runtime_env()
            config_path = resolve_config_path(config_arg)
            config = load_json_file(str(config_path), None)
            if config is None:
                self._write_json(
                    {
                        "ok": False,
                        "time": now_utc_str(),
                        "error": f"Config not found: {config_path}",
                    },
                    status_code=404,
                )
                return

            runtime_config = prepare_config_for_runtime(config)
            validate_config(runtime_config)
            cycle = run_single_scan_with_state(runtime_config)

            results = cycle.get("results", [])
            alerts = cycle.get("alerts", [])
            errors = cycle.get("errors", [])
            execution_mode = str(
                cycle.get("config", runtime_config).get("execution", {}).get("mode", "paper")
            ).lower()

            self._write_json(
                {
                    "ok": True,
                    "time": now_utc_str(),
                    "config_path": str(config_path.relative_to(ROOT_DIR)),
                    "execution_mode": execution_mode,
                    "runtime_notes": cycle.get("runtime_notes", runtime_config.get("_runtime_notes", [])),
                    "state_file": cycle.get("state_file"),
                    "summary": {
                        "scanned": len(results),
                        "alerts": len(alerts),
                        "errors": len(errors),
                        "buy_signals": len([r for r in results if str(r.get("action")) == "BUY_LIMIT"]),
                        "wait_signals": len(
                            [r for r in results if str(r.get("action", "")).startswith("WAIT")]
                        ),
                    },
                    "auto_added_symbols": cycle.get("auto_added_symbols", []),
                    "alerts": alerts,
                    "errors": errors,
                    "top_results": compact_results(results, limit=10),
                    "execution_events": cycle.get("execution_events", []),
                    "risk_state": cycle.get("risk_state", {}),
                    "performance": cycle.get("performance", {}),
                },
                status_code=200,
            )
        except Exception as e:
            self._write_json(
                {
                    "ok": False,
                    "time": now_utc_str(),
                    "error": str(e),
                    "trace": traceback.format_exc(limit=3),
                },
                status_code=500,
            )

    def do_OPTIONS(self) -> None:  # noqa: N802
        self._set_headers(status_code=200)

    def do_GET(self) -> None:  # noqa: N802
        self._run_scan()

    def do_POST(self) -> None:  # noqa: N802
        self._run_scan()
