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
from trading_bot.state_store import (  # noqa: E402
    describe_json_storage_backend,
    load_persisted_json,
    save_persisted_json,
)


DEFAULT_CONFIG_PATH = "configs/config.json"
ALLOWED_CONFIG_DIR = (ROOT_DIR / "configs").resolve()
DEFAULT_STATUS_FILE = "state/last_scan_snapshot.json"
DEFAULT_VERCEL_STATUS_FILE = "/tmp/trading_bot_last_scan_snapshot.json"


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
        self.wfile.write(json.dumps(payload).encode("utf-8"))

    def _write_html(self, payload: str, status_code: int = 200) -> None:
        self._set_headers(status_code=status_code, content_type="text/html; charset=utf-8")
        self.wfile.write(payload.encode("utf-8"))

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
    <style>
      :root {
        --bg: #060b16;
        --bg2: #0f182c;
        --line: #243659;
        --txt: #e9efff;
        --sub: #a9b8dc;
        --ok: #2fd08a;
        --warn: #ffb458;
        --err: #ff6d7e;
      }
      * { box-sizing: border-box; }
      body {
        font-family: -apple-system, BlinkMacSystemFont, Segoe UI, Roboto, sans-serif;
        background: radial-gradient(1200px 500px at 10% -10%, #203b73 0%, transparent 60%), var(--bg);
        color: var(--txt);
        margin: 0;
      }
      .wrap { max-width: 1200px; margin: 28px auto; padding: 0 16px; }
      .hero {
        background: linear-gradient(135deg, #101d38 0%, #0f182c 100%);
        border: 1px solid var(--line);
        border-radius: 14px;
        padding: 18px;
        margin-bottom: 14px;
      }
      .muted { color: var(--sub); }
      .controls {
        display: grid;
        grid-template-columns: 2fr 1fr 1fr;
        gap: 10px;
        margin-top: 12px;
      }
      input, button {
        height: 40px;
        border-radius: 10px;
        border: 1px solid var(--line);
        background: #0b1324;
        color: var(--txt);
        padding: 0 12px;
      }
      button {
        cursor: pointer;
        background: linear-gradient(135deg, #ff6d6d 0%, #f0544f 100%);
        border: 0;
        font-weight: 700;
      }
      .stats {
        display: grid;
        grid-template-columns: repeat(7, 1fr);
        gap: 10px;
        margin-top: 12px;
      }
      .stat {
        border: 1px solid var(--line);
        background: var(--bg2);
        border-radius: 12px;
        padding: 10px 12px;
      }
      .k { color: var(--sub); font-size: 12px; text-transform: uppercase; letter-spacing: 0.03em; }
      .v { font-size: 26px; font-weight: 700; margin-top: 4px; }
      .box {
        margin-top: 14px;
        border: 1px solid var(--line);
        border-radius: 12px;
        background: var(--bg2);
        overflow: hidden;
      }
      .box h3 { margin: 0; padding: 12px; border-bottom: 1px solid var(--line); }
      table { width: 100%; border-collapse: collapse; }
      th, td {
        text-align: left;
        padding: 10px 12px;
        border-bottom: 1px solid #1f3052;
        font-size: 13px;
      }
      th { color: var(--sub); font-weight: 600; }
      .ok { color: var(--ok); }
      .warn { color: var(--warn); }
      .err { color: var(--err); }
      .status {
        margin-top: 10px;
        padding: 10px 12px;
        border-radius: 10px;
        border: 1px solid var(--line);
        background: #0c1528;
        color: var(--sub);
      }
      @media (max-width: 900px) {
        .controls { grid-template-columns: 1fr; }
        .stats { grid-template-columns: 1fr 1fr; }
      }
    </style>
  </head>
  <body>
    <div class="wrap">
      <div class="hero">
        <h1 style="margin:0 0 6px 0;">Bobyt Trading Dashboard</h1>
        <div class="muted">Frontend monitors backend snapshots. Trading/scans run only from protected backend endpoint.</div>
        <div class="controls">
          <input id="token" type="password" placeholder="Bearer token (TRADING_BOT_SCAN_TOKEN)" autocomplete="off" />
          <input id="refresh" type="number" min="15" value="60" />
          <button id="refreshBtn">Refresh Now</button>
        </div>
        <div class="status" id="status">Ready. Waiting for backend snapshot.</div>
      </div>

      <div class="stats">
        <div class="stat"><div class="k">Scanned</div><div class="v" id="m_scanned">-</div></div>
        <div class="stat"><div class="k">Buy Signals</div><div class="v ok" id="m_buy">-</div></div>
        <div class="stat"><div class="k">Wait Signals</div><div class="v warn" id="m_wait">-</div></div>
        <div class="stat"><div class="k">Open Positions</div><div class="v" id="m_open">-</div></div>
        <div class="stat"><div class="k">Pending Entries</div><div class="v warn" id="m_pending">-</div></div>
        <div class="stat"><div class="k">Errors</div><div class="v err" id="m_err">-</div></div>
        <div class="stat"><div class="k">Execution</div><div class="v" id="m_exec">-</div></div>
      </div>

      <div class="box">
        <h3>Top Results</h3>
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

      <div class="box">
        <h3>Execution Events</h3>
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
    <script>
      const $ = (id) => document.getElementById(id);
      const statusEl = $("status");
      const tokenInput = $("token");
      const TOKEN_STORAGE_KEY = "bobyt_scan_token";
      let timer = null;

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

      function eventStatus(ev) {
        const r = ev && ev.result ? ev.result : {};
        if (r.success && r.submitted) return "ORDER_SUBMITTED";
        if (r.success && !r.submitted) return "NOT_SUBMITTED";
        if (!r.success) return "FAILED";
        return "-";
      }

      function getAuthHeaders() {
        const token = String(tokenInput.value || "").trim();
        if (!token) return {};
        return { Authorization: "Bearer " + token };
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
          appendCell(tr, r.action, rowClass(r.action));
          appendCell(tr, r.score);
          appendCell(tr, r.price);
          appendCell(tr, r.entry);
          appendCell(tr, r.tp);
          appendCell(tr, r.sl);
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
          appendCell(tr, status, statusClass);
          appendCell(tr, submitted);
          appendCell(tr, result.message || "");
          tbody.appendChild(tr);
        });
      }

      async function fetchStatus() {
        const url = "/api/status";

        statusEl.textContent = "Refreshing monitoring data...";
        try {
          const res = await fetch(url, { method: "GET", headers: getAuthHeaders() });
          const data = await res.json();
          if (!res.ok || !data.ok) {
            statusEl.textContent = "Status fetch failed: " + (data.error || ("HTTP " + res.status));
            return;
          }
          if (!data.has_data) {
            $("m_scanned").textContent = "-";
            $("m_buy").textContent = "-";
            $("m_wait").textContent = "-";
            $("m_open").textContent = "-";
            $("m_pending").textContent = "-";
            $("m_err").textContent = "-";
            $("m_exec").textContent = "-";
            setPlaceholderRow($("rows"), 8, "No backend snapshot yet. Trigger /api/scan from cron first.");
            setPlaceholderRow($("execRows"), 5, "No execution events yet.");
            statusEl.textContent = "No backend snapshot yet. Run /api/scan (cron/manual) first.";
            return;
          }
          $("m_scanned").textContent = text(data.summary?.scanned);
          $("m_buy").textContent = text(data.summary?.buy_signals);
          $("m_wait").textContent = text(data.summary?.wait_signals);
          $("m_open").textContent = text(data.positions?.open_count);
          $("m_pending").textContent = text(data.positions?.pending_entry_count);
          $("m_err").textContent = text(data.summary?.errors);
          $("m_exec").textContent = text((data.execution_mode || "").toUpperCase());

          renderTopRows(data.top_results || []);
          renderExecutionRows(data.execution_events || []);
            statusEl.textContent =
              "Backend last scan: " + text(data.time) +
              " | state: " + text(data.state_file) +
              " | backend: " + text(data.state_backend || data.status_backend || "file");
        } catch (e) {
          statusEl.textContent = "Network error: " + e;
        }
      }

      function applyAutoRefresh() {
        if (timer) clearInterval(timer);
        const sec = Math.max(15, Number($("refresh").value || 60));
        timer = setInterval(fetchStatus, sec * 1000);
      }

      $("refreshBtn").addEventListener("click", fetchStatus);
      $("refresh").addEventListener("change", applyAutoRefresh);
      tokenInput.value = window.sessionStorage.getItem(TOKEN_STORAGE_KEY) || "";
      tokenInput.addEventListener("change", () => {
        window.sessionStorage.setItem(TOKEN_STORAGE_KEY, tokenInput.value || "");
      });
      applyAutoRefresh();
      fetchStatus();
    </script>
  </body>
</html>""",
                status_code=200,
            )
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
                "positions": {
                    "open_count": len(open_symbols),
                    "open_symbols": open_symbols,
                    "pending_entry_count": len(pending_entry_symbols),
                    "pending_entry_symbols": pending_entry_symbols,
                },
                "risk_state": cycle.get("risk_state", {}),
                "risk_limits": cycle.get("risk_limits", {}),
                "performance": cycle.get("performance", {}),
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
