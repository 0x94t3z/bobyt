#!/usr/bin/env python3
from __future__ import annotations

import json
import os
import sys
import traceback
import urllib.parse
import uuid
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler
from pathlib import Path
from typing import Any, Dict, List
import math

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
from trading_bot.state_store import (  # noqa: E402
    acquire_named_lock,
    describe_json_storage_backend,
    load_persisted_json,
    release_named_lock,
    save_persisted_json,
)


DEFAULT_CONFIG_PATH = "configs/config.json"
ALLOWED_CONFIG_DIR = (ROOT_DIR / "configs").resolve()
DEFAULT_STATUS_FILE = "state/last_scan_snapshot.json"
DEFAULT_VERCEL_STATUS_FILE = "/tmp/trading_bot_last_scan_snapshot.json"
DEFAULT_SCAN_LOCK_FILE = "state/scan_lock.json"
DEFAULT_VERCEL_SCAN_LOCK_FILE = "/tmp/trading_bot_scan_lock.json"
DEFAULT_SCAN_LOCK_NAME = "api_scan"
DEFAULT_SCAN_LOCK_TTL_SECONDS = 180


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


def sanitize_for_strict_json(value: Any) -> Any:
    if isinstance(value, float):
        if math.isfinite(value):
            return value
        # Encode non-finite values as strings to preserve meaning in strict JSON.
        if value > 0:
            return "INF"
        if value < 0:
            return "-INF"
        return "NaN"
    if isinstance(value, dict):
        return {k: sanitize_for_strict_json(v) for k, v in value.items()}
    if isinstance(value, list):
        return [sanitize_for_strict_json(v) for v in value]
    return value


def query_flag(query: Dict[str, List[str]], key: str, default: bool = False) -> bool:
    values = query.get(key, [])
    if not values:
        return default
    return parse_env_bool(values[0], default=default)


def is_running_on_vercel() -> bool:
    if parse_env_bool(os.getenv("VERCEL"), False):
        return True
    return bool(str(os.getenv("VERCEL_ENV", "")).strip())


def resolve_status_file() -> str:
    override = str(os.getenv("TRADING_BOT_STATUS_FILE", "")).strip()
    if override:
        return override
    return DEFAULT_VERCEL_STATUS_FILE if is_running_on_vercel() else DEFAULT_STATUS_FILE


def resolve_scan_lock_file() -> str:
    override = str(os.getenv("TRADING_BOT_SCAN_LOCK_FILE", "")).strip()
    if override:
        return override
    return DEFAULT_VERCEL_SCAN_LOCK_FILE if is_running_on_vercel() else DEFAULT_SCAN_LOCK_FILE


def resolve_scan_lock_ttl_seconds() -> int:
    raw = str(os.getenv("TRADING_BOT_SCAN_LOCK_TTL_SECONDS", DEFAULT_SCAN_LOCK_TTL_SECONDS)).strip()
    try:
        ttl = int(raw)
    except ValueError:
        ttl = DEFAULT_SCAN_LOCK_TTL_SECONDS
    return min(max(ttl, 30), 900)


def bobyt_favicon_svg() -> str:
    return (
        '<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 64 64">'
        '<defs><linearGradient id="g" x1="0" y1="0" x2="1" y2="1">'
        '<stop offset="0%" stop-color="#ffbf2f"/>'
        '<stop offset="100%" stop-color="#f09512"/>'
        "</linearGradient></defs>"
        '<rect width="64" height="64" rx="14" fill="#111319"/>'
        '<rect x="6" y="6" width="52" height="52" rx="12" fill="none" stroke="#2e3642" stroke-width="2"/>'
        '<path d="M22 15h14.5c7.2 0 11.5 3.8 11.5 9.7 0 4.1-2.1 7.1-6 8.4 4.8 1.2 7.4 4.5 7.4 9.1 0 6.3-5 10.8-12.4 10.8H22V15zm13 14.8c3.4 0 5.4-1.6 5.4-4.3 0-2.6-1.8-4.1-5-4.1h-5.9v8.4H35zm1.4 16.8c3.7 0 5.9-1.8 5.9-4.9 0-3-2.2-4.8-6.2-4.8h-6.6v9.7h6.9z" fill="url(#g)"/>'
        "</svg>"
    )


def extract_open_symbols(cycle: Dict[str, Any]) -> List[str]:
    live_positions = cycle.get("live_open_positions", {})
    if isinstance(live_positions, dict) and live_positions:
        return sorted(str(k) for k in live_positions.keys())
    state_positions = cycle.get("state", {}).get("positions", {})
    if isinstance(state_positions, dict) and state_positions:
        return sorted(str(k) for k in state_positions.keys())
    return []


def extract_pending_entry_symbols(cycle: Dict[str, Any]) -> List[str]:
    live_pending = cycle.get("live_pending_entries", {})
    if isinstance(live_pending, dict) and live_pending:
        return sorted(str(k) for k in live_pending.keys())
    state_pending = cycle.get("state", {}).get("live_pending_entries", {})
    if isinstance(state_pending, dict) and state_pending:
        return sorted(str(k) for k in state_pending.keys())
    return []


class handler(BaseHTTPRequestHandler):
    def _set_headers(self, status_code: int = 200, content_type: str = "application/json") -> None:
        self.send_response(status_code)
        self.send_header("Content-Type", content_type)
        self.send_header("Cache-Control", "no-store")
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Authorization, Content-Type")
        self.end_headers()

    def _write_json(self, payload: Dict[str, Any], status_code: int = 200) -> None:
        self._set_headers(status_code=status_code, content_type="application/json")
        # Enforce strict JSON (no NaN/Infinity) to keep browser JSON.parse reliable.
        sanitized = sanitize_for_strict_json(payload)
        self.wfile.write(json.dumps(sanitized, allow_nan=False).encode("utf-8"))

    def _write_html(self, payload: str, status_code: int = 200) -> None:
        self._set_headers(status_code=status_code, content_type="text/html; charset=utf-8")
        self.wfile.write(payload.encode("utf-8"))

    def _resolve_auth_secret(self, path: str) -> tuple[str, str]:
        scan_secret = str(
            os.getenv("TRADING_BOT_SCAN_TOKEN", "") or os.getenv("CRON_SECRET", "")
        ).strip()
        status_secret = str(os.getenv("TRADING_BOT_STATUS_TOKEN", "")).strip()

        if path == "/api/status":
            secret = status_secret or scan_secret
            return secret, "TRADING_BOT_STATUS_TOKEN (or TRADING_BOT_SCAN_TOKEN / CRON_SECRET)"

        return scan_secret, "TRADING_BOT_SCAN_TOKEN (or CRON_SECRET)"

    def _is_authorized(self, path: str) -> tuple[bool, str]:
        require_auth = parse_env_bool(os.getenv("TRADING_BOT_REQUIRE_SCAN_AUTH"), True)
        if not require_auth:
            return True, ""

        secret, missing_hint = self._resolve_auth_secret(path)
        if not secret:
            return False, (
                "Auth is required but no secret is configured. "
                f"Set {missing_hint}."
            )

        header_value = str(self.headers.get("Authorization", "")).strip()
        if header_value == f"Bearer {secret}":
            return True, ""

        return False, "Unauthorized (use Authorization: Bearer <token>)"

    def _run_scan(self) -> None:
        parsed = urllib.parse.urlparse(self.path)
        path = (parsed.path or "/").rstrip("/") or "/"
        query = urllib.parse.parse_qs(parsed.query)

        if path == "/":
            self._write_html(
                """<!doctype html>
<html lang="en">
  <head>
    <meta charset="utf-8" />
    <meta name="viewport" content="width=device-width,initial-scale=1" />
    <title>Bobyt Trading Dashboard</title>
    <link rel="icon" type="image/svg+xml" href="/favicon.svg" />
    <style>
      @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700;800&display=swap');
      :root {
        --bg: #111319;
        --panel: #181d25d9;
        --panel-solid: #1d232d;
        --line: #2e3642;
        --line-soft: #262d38;
        --txt: #eef2f5;
        --sub: #9aa6b2;
        --ok: #20c997;
        --warn: #f7a600;
        --err: #ff5b6e;
        --accent: #f7a600;
        --bg-blob-1: #3c331e66;
        --bg-blob-2: #232a3650;
        --hero-start: #20262fcc;
        --hero-end: #171c25cc;
        --chip-border: #3a444f;
        --chip-bg: #1b212b;
        --chip-text: #f7a600;
        --input-bg: #131922;
        --button-start: #ffbf2f;
        --button-end: #f09512;
        --th-text: #b8c0cb;
        --th-bg: #1a2029;
        --row-alt: #171d27ba;
        --grid-line: #2e3642;
        --soft-glow: #96aeff26;
      }
      * { box-sizing: border-box; }
      body {
        font-family: "Inter", "Segoe UI", sans-serif;
        background:
          radial-gradient(900px 500px at 5% -10%, var(--bg-blob-1) 0%, transparent 62%),
          radial-gradient(750px 450px at 95% 0%, var(--bg-blob-2) 0%, transparent 58%),
          var(--bg);
        color: var(--txt);
        margin: 0;
      }
      .wrap { max-width: 1420px; margin: 22px auto 34px auto; padding: 0 18px; }
      .hero {
        background: linear-gradient(155deg, var(--hero-start) 0%, var(--hero-end) 100%);
        border: 1px solid var(--line);
        border-radius: 18px;
        padding: 22px;
        margin-bottom: 12px;
        box-shadow: 0 20px 50px #02050b99, inset 0 1px 0 #ffffff0d;
        backdrop-filter: blur(6px);
      }
      .title-row {
        display: flex;
        justify-content: space-between;
        align-items: flex-start;
        gap: 14px;
      }
      .chip {
        border: 1px solid var(--chip-border);
        background: var(--chip-bg);
        color: var(--chip-text);
        border-radius: 999px;
        padding: 6px 10px;
        font-size: 12px;
        font-weight: 600;
        letter-spacing: 0.04em;
        text-transform: uppercase;
        white-space: nowrap;
      }
      h1 { margin: 0 0 6px 0; font-size: 42px; letter-spacing: -0.03em; line-height: 1.03; }
      .muted { color: var(--sub); }
      .header-sub {
        max-width: 760px;
        line-height: 1.45;
      }
      .portfolio-card {
        margin-top: 14px;
        border: 1px solid #3b3224;
        border-radius: 16px;
        overflow: hidden;
        background: radial-gradient(120% 130% at 5% 5%, #2a2318 0%, #171c25 45%, #111319 100%);
        display: grid;
        grid-template-columns: 1.2fr 1fr;
        min-height: 148px;
      }
      .portfolio-main {
        padding: 14px 16px;
        display: flex;
        flex-direction: column;
        gap: 10px;
      }
      .portfolio-label {
        font-size: 11px;
        letter-spacing: 0.09em;
        text-transform: uppercase;
        color: #c8b69b;
        font-weight: 700;
      }
      .portfolio-user {
        font-size: 23px;
        font-weight: 800;
        letter-spacing: -0.02em;
      }
      .portfolio-grid {
        display: grid;
        grid-template-columns: 1fr 1fr;
        gap: 12px;
      }
      .portfolio-item .k {
        font-size: 10px;
      }
      .portfolio-item .v {
        font-size: 24px;
        margin-top: 3px;
      }
      .portfolio-visual {
        position: relative;
        border-left: 1px solid #2f3744;
        background:
          radial-gradient(110px 90px at 75% 30%, #ffbf2f2e 0%, transparent 80%),
          linear-gradient(165deg, #171d27 0%, #0f131b 100%);
      }
      .portfolio-coin {
        position: absolute;
        right: 28px;
        top: 24px;
        width: 88px;
        height: 88px;
        border-radius: 50%;
        background: linear-gradient(145deg, #ffd064 0%, #f2a51f 60%, #d8850f 100%);
        color: #201807;
        font-size: 40px;
        font-weight: 900;
        display: flex;
        align-items: center;
        justify-content: center;
        box-shadow: inset 0 2px 0 #fff0bf, 0 14px 28px #00000080;
      }
      .portfolio-brand {
        position: absolute;
        right: 16px;
        bottom: 12px;
        color: #f6cc7e;
        font-size: 11px;
        letter-spacing: 0.1em;
        text-transform: uppercase;
        font-weight: 700;
      }
      .controls {
        display: grid;
        grid-template-columns: repeat(12, minmax(0, 1fr));
        gap: 10px;
        margin-top: 16px;
        padding: 12px;
        border: 1px solid var(--line-soft);
        border-radius: 14px;
        background: #141920cc;
      }
      .control {
        display: flex;
        flex-direction: column;
        gap: 6px;
      }
      .control.token { grid-column: span 7; }
      .control.refresh { grid-column: span 2; }
      .control.action { grid-column: span 3; justify-content: flex-end; }
      .control label {
        color: var(--sub);
        font-size: 11px;
        text-transform: uppercase;
        letter-spacing: 0.08em;
        font-weight: 700;
      }
      input, button, select {
        height: 44px;
        border-radius: 12px;
        border: 1px solid var(--line);
        background: var(--input-bg);
        color: var(--txt);
        padding: 0 12px;
        font-family: inherit;
        font-size: 14px;
      }
      input:focus, select:focus {
        outline: none;
        border-color: var(--accent);
        box-shadow: 0 0 0 3px #6d8dff2b;
      }
      button {
        cursor: pointer;
        background: linear-gradient(135deg, var(--button-start) 0%, var(--button-end) 100%);
        border: 0;
        font-weight: 700;
        transition: transform 120ms ease, filter 120ms ease;
      }
      button:hover { filter: brightness(1.06); transform: translateY(-1px); }
      .control.action button { width: 100%; }
      .stats {
        display: grid;
        grid-template-columns: repeat(8, minmax(0, 1fr));
        gap: 10px;
        margin-top: 12px;
      }
      .stat {
        border: 1px solid var(--line);
        background: linear-gradient(180deg, #ffffff06 0%, #00000000 32%), var(--panel-solid);
        border-radius: 13px;
        padding: 12px 13px;
        box-shadow: inset 0 1px 0 #ffffff0d;
      }
      .k { color: var(--sub); font-size: 11px; text-transform: uppercase; letter-spacing: 0.08em; font-weight: 600; }
      .v { font-size: 30px; font-weight: 700; margin-top: 6px; line-height: 1.03; letter-spacing: -0.02em; }
      .s { margin-top: 4px; font-size: 11px; letter-spacing: 0.02em; }
      .box {
        margin-top: 14px;
        border: 1px solid var(--line);
        border-radius: 14px;
        background: var(--panel);
        overflow: hidden;
        box-shadow: inset 0 1px 0 #ffffff0a;
      }
      .box h3 {
        margin: 0;
        padding: 13px 14px 12px 14px;
        border-bottom: 1px solid var(--line-soft);
        font-size: 22px;
        letter-spacing: -0.01em;
      }
      .tv-box { margin-top: 14px; }
      .tv-meta {
        padding: 11px 14px;
        border-bottom: 1px solid var(--line-soft);
        color: #c5cfdb;
        font-size: 12.5px;
        letter-spacing: 0.03em;
        background: #151b24;
      }
      .tv-wrap {
        height: 420px;
        padding: 8px 10px 10px 10px;
      }
      #tvChartHost, #tvChart {
        width: 100%;
        height: 100%;
      }
      .tv-fallback {
        margin: 12px;
        border: 1px dashed var(--line);
        border-radius: 12px;
        color: var(--sub);
        font-size: 13px;
        padding: 16px;
        text-align: center;
      }
      .table-wrap { overflow-x: auto; }
      table { width: 100%; border-collapse: collapse; min-width: 860px; }
      th, td {
        text-align: left;
        padding: 11px 12px;
        border-bottom: 1px solid var(--grid-line);
        font-size: 13.5px;
        line-height: 1.35;
        vertical-align: top;
      }
      td { color: #dbe3ee; }
      th {
        color: var(--th-text);
        font-weight: 700;
        letter-spacing: 0.04em;
        text-transform: uppercase;
        font-size: 11.5px;
        background: var(--th-bg);
      }
      tr:nth-child(even) td { background: var(--row-alt); }
      tbody tr:hover td { background: #1d2634; }
      .ok { color: var(--ok); }
      .warn { color: var(--warn); }
      .err { color: var(--err); }
      .badge {
        display: inline-flex;
        align-items: center;
        border: 1px solid var(--line);
        border-radius: 999px;
        font-weight: 700;
        font-size: 11px;
        letter-spacing: 0.03em;
        padding: 3px 8px;
        background: #0a1630;
      }
      .badge.ok { border-color: #2a7f5c; background: #123425; color: #6ce7ae; }
      .badge.warn { border-color: #8b6a2f; background: #35280f; color: #ffd28a; }
      .badge.err { border-color: #8b3a45; background: #36151a; color: #ff95a1; }
      .status {
        margin-top: 10px;
        padding: 10px 13px;
        border-radius: 11px;
        border: 1px solid var(--line);
        background: #0c162f;
        box-shadow: inset 0 1px 0 #ffffff0a;
      }
      .status-main {
        color: #dbe6f7;
        font-size: 13.5px;
        font-weight: 600;
        line-height: 1.35;
      }
      .status-meta {
        margin-top: 4px;
        font-size: 11.5px;
        color: #9eb0c6;
      }
      .status.status-ok { border-color: #2a7f5c; background: #102b21; color: #7deab5; }
      .status.status-warn { border-color: #8b6a2f; background: #30250f; color: #ffd28a; }
      .status.status-err { border-color: #8b3a45; background: #32161a; color: #ff9ba7; }
      .status.status-info { border-color: var(--line); background: #0c162f; color: var(--sub); }
      @media (max-width: 1200px) {
        .stats { grid-template-columns: repeat(4, minmax(0, 1fr)); }
        .control.token { grid-column: span 7; }
        .control.refresh { grid-column: span 2; }
        .control.action { grid-column: span 3; }
      }
      @media (max-width: 900px) {
        h1 { font-size: 30px; }
        .portfolio-card { grid-template-columns: 1fr; }
        .portfolio-visual { min-height: 96px; border-left: 0; border-top: 1px solid #2f3744; }
        .portfolio-coin { width: 66px; height: 66px; font-size: 30px; top: 14px; }
        .controls { grid-template-columns: 1fr; padding: 10px; }
        .control.token, .control.refresh, .control.action { grid-column: span 1; }
        .stats { grid-template-columns: 1fr 1fr; }
        .title-row { flex-direction: column; align-items: flex-start; }
        th, td { font-size: 13px; }
      }
      @media (max-width: 980px) {
        .tv-wrap { height: 360px; }
      }
    </style>
  </head>
  <body>
    <div class="wrap">
      <div class="hero">
        <div class="title-row">
          <div>
            <h1>Bobyt Trading Dashboard</h1>
            <div class="muted header-sub">Frontend monitors backend snapshots. Trading/scans run only from protected backend endpoint.</div>
          </div>
          <div class="chip">Bybit Spot Monitor</div>
        </div>
        <div class="portfolio-card">
          <div class="portfolio-main">
            <div class="portfolio-label">Trader Profile</div>
            <div class="portfolio-user" id="u_username">@0x94t3z</div>
            <div class="portfolio-grid">
              <div class="portfolio-item">
                <div class="k">Total Balance (USDT)</div>
                <div class="v ok" id="u_balance">-</div>
              </div>
              <div class="portfolio-item">
                <div class="k">Total Profit (USDT)</div>
                <div class="v" id="u_profit">-</div>
              </div>
            </div>
          </div>
          <div class="portfolio-visual">
            <div class="portfolio-coin">B</div>
            <div class="portfolio-brand">Bobyt Prime</div>
          </div>
        </div>
        <div class="controls">
          <div class="control token">
            <label for="token">Status Token</label>
            <input id="token" type="password" placeholder="Bearer token (TRADING_BOT_STATUS_TOKEN)" autocomplete="off" />
          </div>
          <div class="control refresh">
            <label for="refresh">Refresh (s)</label>
            <input id="refresh" type="number" min="15" value="60" title="Auto-refresh interval in seconds" />
          </div>
          <div class="control action">
            <label for="refreshBtn">Action</label>
            <button id="refreshBtn">Refresh Now</button>
          </div>
        </div>
        <div class="status" id="status">
          <div class="status-main" id="statusMain">Ready. Waiting for backend snapshot.</div>
          <div class="status-meta" id="statusMeta">Dashboard is online.</div>
        </div>
      </div>

      <div class="stats">
        <div class="stat"><div class="k">Scanned</div><div class="v" id="m_scanned">-</div></div>
        <div class="stat"><div class="k">Buy Signals</div><div class="v ok" id="m_buy">-</div></div>
        <div class="stat"><div class="k">Wait Signals</div><div class="v warn" id="m_wait">-</div></div>
        <div class="stat"><div class="k">Open Positions</div><div class="v" id="m_open">-</div></div>
        <div class="stat"><div class="k">Pending Entries</div><div class="v warn" id="m_pending">-</div></div>
        <div class="stat"><div class="k">Errors</div><div class="v err" id="m_err">-</div></div>
        <div class="stat"><div class="k">Execution</div><div class="v" id="m_exec">-</div></div>
        <div class="stat">
          <div class="k">USDT Available</div>
          <div class="v ok" id="m_usdt">-</div>
          <div class="s muted" id="m_usdt_note">-</div>
        </div>
      </div>

      <div class="box tv-box">
        <h3>Live Market Chart</h3>
        <div class="tv-meta" id="tvMeta">Focus: waiting for trade symbol...</div>
        <div class="tv-wrap">
          <div id="tvChartHost"></div>
          <div class="tv-fallback" id="tvFallback" style="display:none;">
            No symbol available yet. The chart will auto-load when the bot has an active/recent trade symbol.
          </div>
        </div>
      </div>

      <div class="box">
        <h3>Performance</h3>
        <div class="table-wrap">
        <table>
          <thead>
            <tr>
              <th>Window</th><th>Trades</th><th>Win Rate</th><th>Net PnL (USDT)</th><th>Profit Factor</th><th>Max Drawdown</th><th>Expectancy</th>
            </tr>
          </thead>
          <tbody id="perfRows">
            <tr><td colspan="7" class="muted">No performance data yet.</td></tr>
          </tbody>
        </table>
        </div>
      </div>

      <div class="box">
        <h3>Top Results</h3>
        <div class="table-wrap">
        <table>
          <thead>
            <tr>
              <th>Symbol</th><th>Action</th><th>Score</th><th>Price</th><th>Entry</th><th>TP</th><th>SL</th><th>Note</th>
            </tr>
          </thead>
          <tbody id="rows">
            <tr><td colspan="8" class="muted">No data yet. Waiting backend snapshot.</td></tr>
          </tbody>
        </table>
        </div>
      </div>

      <div class="box">
        <h3>Execution Events</h3>
        <div class="table-wrap">
        <table>
          <thead>
            <tr>
              <th>Time</th><th>Symbol</th><th>Status</th><th>Submitted</th><th>Message</th>
            </tr>
          </thead>
          <tbody id="execRows">
            <tr><td colspan="5" class="muted">No execution events in this snapshot.</td></tr>
          </tbody>
        </table>
        </div>
      </div>
    </div>
    <script>
      const $ = (id) => document.getElementById(id);
      const statusEl = $("status");
      const statusMainEl = $("statusMain");
      const statusMetaEl = $("statusMeta");
      const tokenInput = $("token");
      let timer = null;
      let tradingViewScriptPromise = null;
      let tradingViewWidget = null;
      let currentTradingViewSymbol = "";
      let currentTradingViewInterval = "";
      let lastSnapshot = null;

      function text(v) {
        if (v === null || v === undefined || v === "") return "-";
        return String(v);
      }

      function appendCell(tr, value, className = "") {
        const td = document.createElement("td");
        if (className) td.className = className;
        td.textContent = text(value);
        tr.appendChild(td);
      }

      function setPlaceholderRow(tbody, colSpan, message) {
        tbody.replaceChildren();
        const tr = document.createElement("tr");
        const td = document.createElement("td");
        td.colSpan = colSpan;
        td.className = "muted";
        td.textContent = message;
        tr.appendChild(td);
        tbody.appendChild(tr);
      }

      function rowClass(action) {
        const a = String(action || "");
        if (a.includes("BUY")) return "ok";
        if (a.includes("SELL")) return "err";
        if (a.includes("WAIT")) return "warn";
        return "";
      }

      function setStatus(message, tone = "info") {
        statusEl.className = "status status-" + tone;
        statusMainEl.textContent = message;
      }

      function setStatusMeta(message) {
        if (!statusMetaEl) return;
        statusMetaEl.textContent = message || "";
      }

      function fmtPrice(v) {
        const n = Number(v);
        if (!Number.isFinite(n)) return text(v);
        if (Math.abs(n) >= 1000) return n.toFixed(2);
        if (Math.abs(n) >= 1) return n.toFixed(4);
        return n.toFixed(6);
      }

      function fmtScore(v) {
        const n = Number(v);
        if (!Number.isFinite(n)) return text(v);
        return n.toFixed(2);
      }

      function eventStatus(ev) {
        const r = ev && ev.result ? ev.result : {};
        if (r.success && r.submitted) return "ORDER_SUBMITTED";
        if (r.success && !r.submitted) return "NOT_SUBMITTED";
        if (!r.success) return "FAILED";
        return "-";
      }

      function applyTheme() {
        document.body.setAttribute("data-theme", "bybit");
      }

      function toggleTradingViewFallback(showFallback, message = "") {
        const host = $("tvChartHost");
        const fallback = $("tvFallback");
        if (!host || !fallback) return;
        host.style.display = showFallback ? "none" : "block";
        fallback.style.display = showFallback ? "block" : "none";
        if (message) fallback.textContent = message;
      }

      function toTradingViewSymbol(rawSymbol) {
        const sym = String(rawSymbol || "").trim().toUpperCase();
        if (!sym) return "";
        if (sym.includes(":")) return sym;
        return "BYBIT:" + sym;
      }

      function parseUtcMillis(raw) {
        const s = String(raw || "").trim();
        if (!s) return 0;
        const iso = s.endsWith("UTC") ? s.replace(" UTC", "Z") : s;
        const ms = Date.parse(iso);
        return Number.isFinite(ms) ? ms : 0;
      }

      function isRecentExecution(ev, snapshot) {
        const nowMs = parseUtcMillis(snapshot?.time) || Date.now();
        const evMs = parseUtcMillis(ev?.time);
        if (!evMs) return false;
        const ageMs = Math.max(0, nowMs - evMs);
        return ageMs <= (6 * 60 * 60 * 1000); // 6h
      }

      function toTradingViewInterval(rawInterval) {
        const key = String(rawInterval || "").trim().toLowerCase();
        const map = {
          "1m": "1",
          "3m": "3",
          "5m": "5",
          "15m": "15",
          "30m": "30",
          "45m": "45",
          "1h": "60",
          "2h": "120",
          "4h": "240",
          "6h": "360",
          "12h": "720",
          "1d": "D",
          "1w": "W",
        };
        return map[key] || "15";
      }

      function pickTradingSymbol(snapshot, executionFeed) {
        const openSymbols = Array.isArray(snapshot?.positions?.open_symbols) ? snapshot.positions.open_symbols : [];
        if (openSymbols.length > 0) return { symbol: openSymbols[0], reason: "open position" };

        const topResults = Array.isArray(snapshot?.top_results) ? snapshot.top_results : [];
        const buyCandidate = topResults.find((row) => String(row?.action || "").includes("BUY"));
        if (buyCandidate?.symbol) return { symbol: buyCandidate.symbol, reason: "top BUY candidate" };

        const ranked = topResults[0];
        if (ranked?.symbol) return { symbol: ranked.symbol, reason: "top ranked symbol" };

        const feed = Array.isArray(executionFeed) ? executionFeed : [];
        if (feed.length > 0) {
          const latest = feed[feed.length - 1] || {};
          if (latest.symbol && isRecentExecution(latest, snapshot)) {
            return { symbol: latest.symbol, reason: "recent execution event" };
          }
          if (latest.symbol) return { symbol: latest.symbol, reason: "latest execution event (history)" };
        }

        return { symbol: "", reason: "no trade symbol yet" };
      }

      function ensureTradingViewScript() {
        if (window.TradingView && typeof window.TradingView.widget === "function") {
          return Promise.resolve();
        }
        if (tradingViewScriptPromise) return tradingViewScriptPromise;

        tradingViewScriptPromise = new Promise((resolve, reject) => {
          const script = document.createElement("script");
          script.src = "https://s3.tradingview.com/tv.js";
          script.async = true;
          script.onload = () => resolve();
          script.onerror = () => reject(new Error("Failed to load TradingView script"));
          document.head.appendChild(script);
        });
        return tradingViewScriptPromise;
      }

      async function renderTradingViewChart(snapshot, executionFeed) {
        const tvMeta = $("tvMeta");
        const host = $("tvChartHost");
        if (!tvMeta || !host) return;

        const picked = pickTradingSymbol(snapshot || {}, executionFeed || []);
        const tvSymbol = toTradingViewSymbol(picked.symbol);
        const tvInterval = toTradingViewInterval(snapshot?.scan_interval);
        if (!tvSymbol) {
          tvMeta.textContent = "Focus: waiting for trade symbol...";
          toggleTradingViewFallback(
            true,
            "No symbol available yet. The chart will auto-load when the bot has an active/recent trade symbol."
          );
          return;
        }

        tvMeta.textContent = "Focus: " + tvSymbol + " (" + picked.reason + ") | interval " + tvInterval;
        toggleTradingViewFallback(false);

        if (
          currentTradingViewSymbol === tvSymbol &&
          currentTradingViewInterval === tvInterval &&
          tradingViewWidget
        ) return;

        try {
          await ensureTradingViewScript();
        } catch (err) {
          toggleTradingViewFallback(true, "TradingView script failed to load.");
          return;
        }

        currentTradingViewSymbol = tvSymbol;
        currentTradingViewInterval = tvInterval;
        tradingViewWidget = null;
        host.innerHTML = '<div id="tvChart"></div>';

        try {
          tradingViewWidget = new window.TradingView.widget({
            container_id: "tvChart",
            autosize: true,
            symbol: tvSymbol,
            interval: tvInterval,
            timezone: "Etc/UTC",
            theme: "dark",
            style: "1",
            locale: "en",
            enable_publishing: false,
            allow_symbol_change: true,
            withdateranges: true,
            hide_side_toolbar: false,
            details: false,
            hotlist: false,
            calendar: false,
            studies: ["RSI@tv-basicstudies", "MACD@tv-basicstudies"],
          });
        } catch (err) {
          toggleTradingViewFallback(true, "Unable to render TradingView chart for " + tvSymbol + ".");
        }
      }

      function getAuthHeaders() {
        const token = String(tokenInput.value || "").trim();
        if (!token) return {};
        return { Authorization: "Bearer " + token };
      }

      function num(v, fallback = 0) {
        const n = Number(v);
        return Number.isFinite(n) ? n : fallback;
      }

      function fmtPct(v) {
        return num(v).toFixed(2) + "%";
      }

      function fmtUsdt(v) {
        return num(v).toFixed(2);
      }

      function shortErr(v) {
        const s = String(v || "");
        if (!s) return "";
        return s.length > 58 ? (s.slice(0, 55) + "...") : s;
      }

      function fmtPf(v) {
        const txt = String(v ?? "").trim().toUpperCase();
        if (txt === "INF" || txt === "+INF" || txt === "INFINITY" || txt === "+INFINITY") return "INF";
        if (txt === "-INF" || txt === "-INFINITY") return "-INF";
        const raw = Number(v);
        if (!Number.isFinite(raw)) return "0.00";
        return raw.toFixed(2);
      }

      function renderPerformanceRows(perf) {
        const tbody = $("perfRows");
        const overall = (perf && perf.overall) || {};
        const last7d = (perf && perf.last_7d) || {};

        const totalOverall = num(overall.total_trades);
        const total7d = num(last7d.total_trades);
        if (totalOverall <= 0 && total7d <= 0) {
          setPlaceholderRow(tbody, 7, "No performance data yet.");
          return;
        }

        tbody.replaceChildren();
        const rows = [
          { label: "Overall", data: overall },
          { label: "Last 7 Days", data: last7d },
        ];
        rows.forEach((row) => {
          const d = row.data || {};
          const tr = document.createElement("tr");
          appendCell(tr, row.label);
          appendCell(tr, d.total_trades);
          appendCell(tr, fmtPct(d.win_rate_pct));
          appendCell(tr, fmtUsdt(d.net_pnl_usdt), num(d.net_pnl_usdt) >= 0 ? "ok" : "err");
          appendCell(tr, fmtPf(d.profit_factor));
          appendCell(tr, fmtUsdt(d.max_drawdown_usdt));
          appendCell(tr, fmtUsdt(d.expectancy_usdt));
          tbody.appendChild(tr);
        });
      }

      function renderTopRows(rows) {
        const tbody = $("rows");
        if (!rows.length) {
          setPlaceholderRow(tbody, 8, "No rows in this scan.");
          return;
        }
        tbody.replaceChildren();
        rows.forEach((r) => {
          const tr = document.createElement("tr");
          appendCell(tr, r.symbol);
          const actionTd = document.createElement("td");
          const actionBadge = document.createElement("span");
          const tone = rowClass(r.action);
          actionBadge.className = "badge " + tone;
          actionBadge.textContent = text(r.action);
          actionTd.appendChild(actionBadge);
          tr.appendChild(actionTd);
          appendCell(tr, fmtScore(r.score));
          appendCell(tr, fmtPrice(r.price));
          appendCell(tr, fmtPrice(r.entry));
          appendCell(tr, fmtPrice(r.tp));
          appendCell(tr, fmtPrice(r.sl));
          appendCell(tr, r.note);
          tbody.appendChild(tr);
        });
      }

      function renderExecutionRows(execEvents) {
        const tbody = $("execRows");
        if (!execEvents.length) {
          setPlaceholderRow(tbody, 5, "No execution events in this snapshot.");
          return;
        }
        tbody.replaceChildren();
        execEvents.slice(-10).reverse().forEach((ev) => {
          const result = ev.result || {};
          const status = eventStatus(ev);
          const submitted = result.submitted ? "YES" : "NO";
          const statusClass =
            status === "FAILED" ? "err" : (status === "ORDER_SUBMITTED" ? "ok" : "warn");
          const tr = document.createElement("tr");
          appendCell(tr, ev.time);
          appendCell(tr, ev.symbol);
          const statusTd = document.createElement("td");
          const statusBadge = document.createElement("span");
          statusBadge.className = "badge " + statusClass;
          statusBadge.textContent = status;
          statusTd.appendChild(statusBadge);
          tr.appendChild(statusTd);
          appendCell(tr, submitted);
          appendCell(tr, result.message || "");
          tbody.appendChild(tr);
        });
      }

      async function fetchStatus() {
        const url = "/api/status";

        setStatus("Refreshing monitoring data...", "info");
        setStatusMeta("Fetching latest backend snapshot...");
        try {
          const res = await fetch(url, { method: "GET", headers: getAuthHeaders() });
          const data = await res.json();
          if (!res.ok || !data.ok) {
            setStatus("Status fetch failed: " + (data.error || ("HTTP " + res.status)), "err");
            setStatusMeta("Check token/auth and backend endpoint health.");
            return;
          }
          lastSnapshot = data;
          if (!data.has_data) {
            $("m_scanned").textContent = "-";
            $("m_buy").textContent = "-";
            $("m_wait").textContent = "-";
            $("m_open").textContent = "-";
            $("m_pending").textContent = "-";
            $("m_err").textContent = "-";
            $("m_exec").textContent = "-";
            $("m_usdt").textContent = "-";
            $("m_usdt_note").textContent = "-";
            $("u_balance").textContent = "-";
            $("u_profit").textContent = "-";
            $("u_profit").className = "v";
            setPlaceholderRow($("perfRows"), 7, "No performance data yet.");
            setPlaceholderRow($("rows"), 8, "No backend snapshot yet. Trigger /api/scan from cron first.");
            setPlaceholderRow($("execRows"), 5, "No execution events yet.");
            renderTradingViewChart({}, []);
            setStatus("No backend snapshot yet. Run /api/scan (cron/manual) first.", "warn");
            setStatusMeta("Waiting for first successful backend scan.");
            return;
          }
          $("m_scanned").textContent = text(data.summary?.scanned);
          $("m_buy").textContent = text(data.summary?.buy_signals);
          $("m_wait").textContent = text(data.summary?.wait_signals);
          $("m_open").textContent = text(data.positions?.open_count);
          $("m_pending").textContent = text(data.positions?.pending_entry_count);
          $("m_err").textContent = text(data.summary?.errors);
          $("m_exec").textContent = text((data.execution_mode || "").toUpperCase());
          const bal = data.account_balance || {};
          if (!bal.supported) {
            $("m_usdt").textContent = "N/A";
            $("m_usdt_note").textContent = "Exchange not supported";
          } else if (!bal.configured) {
            $("m_usdt").textContent = "API OFF";
            $("m_usdt_note").textContent = "Set BYBIT_API_KEY/SECRET";
          } else if (!bal.fetched) {
            $("m_usdt").textContent = "ERR";
            $("m_usdt_note").textContent = shortErr(bal.error) || "Unable to read wallet";
          } else {
            const available = num(bal.usdt_available, num(bal.usdt_wallet, 0));
            const wallet = num(bal.usdt_wallet, available);
            $("m_usdt").textContent = fmtUsdt(available);
            $("m_usdt_note").textContent = "wallet " + fmtUsdt(wallet);
          }

          const equity = num(bal.equity_usdt, 0);
          const walletTotal = num(bal.usdt_wallet, num(bal.usdt_available, 0));
          const totalBalance = equity > 0 ? equity : walletTotal;
          $("u_balance").textContent = fmtUsdt(totalBalance);

          const netProfit = num(data.performance?.overall?.net_pnl_usdt, 0);
          $("u_profit").textContent = (netProfit >= 0 ? "+" : "") + fmtUsdt(netProfit);
          $("u_profit").className = netProfit >= 0 ? "v ok" : "v err";

          renderPerformanceRows(data.performance || {});
          renderTopRows(data.top_results || []);
          const execFeed =
            (Array.isArray(data.execution_events) && data.execution_events.length > 0)
              ? data.execution_events
              : (data.execution_events_history || []);
          renderExecutionRows(execFeed);
          renderTradingViewChart(data || {}, execFeed || []);
            setStatus(
              "Snapshot synced successfully",
              "ok"
            );
            setStatusMeta(
              "Last scan " + text(data.time) +
              " • backend " + text(data.state_backend || data.status_backend || "file") +
              " • state " + text(data.state_file)
            );
        } catch (e) {
          setStatus("Network error: " + e, "err");
          setStatusMeta("Unable to reach /api/status. Verify network or Vercel runtime.");
        }
      }

      function applyAutoRefresh() {
        if (timer) clearInterval(timer);
        const sec = Math.max(15, Number($("refresh").value || 60));
        timer = setInterval(fetchStatus, sec * 1000);
      }

      applyTheme();

      $("refreshBtn").addEventListener("click", fetchStatus);
      $("refresh").addEventListener("change", applyAutoRefresh);
      applyAutoRefresh();
      fetchStatus();
    </script>
  </body>
</html>""",
                status_code=200,
            )
            return

        if path == "/favicon.svg":
            self._set_headers(status_code=200, content_type="image/svg+xml")
            self.wfile.write(bobyt_favicon_svg().encode("utf-8"))
            return

        if path not in {"/api", "/api/scan", "/api/status"}:
            self._write_json(
                {
                    "ok": False,
                    "time": now_utc_str(),
                    "error": "Not found",
                    "path": path,
                },
                status_code=404,
            )
            return

        allowed, auth_error = self._is_authorized(path)
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

        if path in {"/api", "/api/scan"} and self.command != "POST":
            self._write_json(
                {
                    "ok": False,
                    "time": now_utc_str(),
                    "error": "Method not allowed for scan endpoint. Use POST /api/scan.",
                },
                status_code=405,
            )
            return

        if path == "/api/status":
            status_file = resolve_status_file()
            status_storage = describe_json_storage_backend(path=status_file, purpose="status")
            snapshot = load_persisted_json(status_file, None, purpose="status")
            if not isinstance(snapshot, dict):
                self._write_json(
                    {
                        "ok": True,
                        "has_data": False,
                        "time": now_utc_str(),
                        "state_file": status_file,
                        "status_backend": status_storage.get("backend", "file"),
                        "status_storage_key": status_storage.get("storage_key"),
                        "status_storage_table": status_storage.get("table"),
                        "message": "No backend snapshot yet. Trigger /api/scan first.",
                    },
                    status_code=200,
                )
                return
            payload = dict(snapshot)
            payload["ok"] = True
            payload["has_data"] = True
            payload["status_backend"] = status_storage.get("backend", "file")
            payload["status_storage_key"] = status_storage.get("storage_key")
            payload["status_storage_table"] = status_storage.get("table")
            self._write_json(payload, status_code=200)
            return

        config_arg = str(query.get("config", [DEFAULT_CONFIG_PATH])[0])
        monitor_only = query_flag(query, "monitor", default=False)
        lock_name = str(os.getenv("TRADING_BOT_SCAN_LOCK_NAME", DEFAULT_SCAN_LOCK_NAME)).strip() or DEFAULT_SCAN_LOCK_NAME
        lock_owner = f"{os.getpid()}-{uuid.uuid4().hex[:12]}"
        lock_path = resolve_scan_lock_file()
        lock_ttl_seconds = resolve_scan_lock_ttl_seconds()
        lock_acquired = acquire_named_lock(
            path=lock_path,
            name=lock_name,
            owner=lock_owner,
            ttl_seconds=lock_ttl_seconds,
        )

        if not lock_acquired:
            self._write_json(
                {
                    "ok": False,
                    "time": now_utc_str(),
                    "error": "Scan already running. Skip overlapping run and retry shortly.",
                    "code": "SCAN_LOCKED",
                    "lock_name": lock_name,
                    "retry_after_seconds": max(15, min(60, lock_ttl_seconds // 3)),
                },
                status_code=409,
            )
            return

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
            if monitor_only:
                exec_cfg = runtime_config.setdefault("execution", {})
                exec_cfg["mode"] = "paper"
                notes = runtime_config.setdefault("_runtime_notes", [])
                if isinstance(notes, list):
                    notes.append("Monitor-only API mode: execution.mode forced to 'paper'.")
            validate_config(runtime_config)
            cycle = run_single_scan_with_state(runtime_config, persist_state=not monitor_only)

            results = cycle.get("results", [])
            alerts = cycle.get("alerts", [])
            errors = cycle.get("errors", [])
            execution_events = cycle.get("execution_events", [])
            execution_events_history = cycle.get("execution_events_history", [])
            open_symbols = extract_open_symbols(cycle)
            pending_entry_symbols = extract_pending_entry_symbols(cycle)
            execution_mode = str(
                cycle.get("config", runtime_config).get("execution", {}).get("mode", "paper")
            ).lower()

            payload = {
                "ok": True,
                "time": now_utc_str(),
                "config_path": str(config_path.relative_to(ROOT_DIR)),
                "monitor_only": bool(monitor_only),
                "execution_mode": execution_mode,
                "scan_interval": str(
                    cycle.get("config", runtime_config).get("interval", runtime_config.get("interval", "15m"))
                ),
                "runtime_notes": cycle.get("runtime_notes", runtime_config.get("_runtime_notes", [])),
                "state_file": cycle.get("state_file"),
                "state_persisted": bool(cycle.get("state_persisted", not monitor_only)),
                "state_backend": cycle.get("state_backend", "file"),
                "state_storage_key": cycle.get("state_storage_key"),
                "state_storage_table": cycle.get("state_storage_table"),
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
                "execution_events": execution_events,
                "execution_events_history": execution_events_history,
                "positions": {
                    "open_count": len(open_symbols),
                    "open_symbols": open_symbols,
                    "pending_entry_count": len(pending_entry_symbols),
                    "pending_entry_symbols": pending_entry_symbols,
                },
                "risk_state": cycle.get("risk_state", {}),
                "risk_limits": cycle.get("risk_limits", {}),
                "performance": cycle.get("performance", {}),
                "account_balance": cycle.get("account_balance", {}),
            }

            if not monitor_only:
                snapshot = dict(payload)
                snapshot.pop("ok", None)
                status_file = resolve_status_file()
                status_storage = describe_json_storage_backend(path=status_file, purpose="status")
                try:
                    save_persisted_json(status_file, snapshot, purpose="status")
                    payload["status_file"] = status_file
                    payload["status_backend"] = status_storage.get("backend", "file")
                    payload["status_storage_key"] = status_storage.get("storage_key")
                    payload["status_storage_table"] = status_storage.get("table")
                except Exception as status_error:
                    payload.setdefault("runtime_notes", [])
                    if isinstance(payload["runtime_notes"], list):
                        payload["runtime_notes"].append(f"Status snapshot save failed: {status_error}")

            self._write_json(payload, status_code=200)
        except Exception as e:
            payload: Dict[str, Any] = {
                "ok": False,
                "time": now_utc_str(),
                "error": str(e),
            }
            if parse_env_bool(os.getenv("TRADING_BOT_DEBUG_API"), False):
                payload["trace"] = traceback.format_exc(limit=3)
            self._write_json(payload, status_code=500)
        finally:
            try:
                release_named_lock(path=lock_path, name=lock_name, owner=lock_owner)
            except Exception:
                pass

    def do_OPTIONS(self) -> None:  # noqa: N802
        self._set_headers(status_code=200)

    def do_GET(self) -> None:  # noqa: N802
        self._run_scan()

    def do_POST(self) -> None:  # noqa: N802
        self._run_scan()
