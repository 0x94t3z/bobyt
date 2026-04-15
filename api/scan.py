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

        # Fallback for schedulers that are easier to configure with URL params.
        query_token = str(query.get("token", [""])[0]).strip()
        if query_token and query_token == secret:
            return True, ""

        return False, "Unauthorized (use Authorization: Bearer <token> or ?token=<token>)"

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
        grid-template-columns: 2fr 2fr 1fr 1fr;
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
        grid-template-columns: repeat(5, 1fr);
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
        <div class="muted">Vercel-hosted UI + API. Scan endpoint remains token-protected.</div>
        <div class="controls">
          <input id="config" value="configs/config.json" placeholder="Config path" />
          <input id="token" placeholder="TRADING_BOT_SCAN_TOKEN" />
          <input id="refresh" type="number" min="15" value="60" />
          <button id="runBtn">Run Scan</button>
        </div>
        <div class="status" id="status">Ready.</div>
      </div>

      <div class="stats">
        <div class="stat"><div class="k">Scanned</div><div class="v" id="m_scanned">-</div></div>
        <div class="stat"><div class="k">Buy Signals</div><div class="v ok" id="m_buy">-</div></div>
        <div class="stat"><div class="k">Wait Signals</div><div class="v warn" id="m_wait">-</div></div>
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
            <tr><td colspan="8" class="muted">No data yet. Run scan.</td></tr>
          </tbody>
        </table>
      </div>
    </div>
    <script>
      const $ = (id) => document.getElementById(id);
      const statusEl = $("status");
      let timer = null;

      function text(v) {
        if (v === null || v === undefined || v === "") return "-";
        return String(v);
      }

      function rowClass(action) {
        const a = String(action || "");
        if (a.includes("BUY")) return "ok";
        if (a.includes("SELL")) return "err";
        if (a.includes("WAIT")) return "warn";
        return "";
      }

      async function runScan() {
        const config = $("config").value.trim() || "configs/config.json";
        const token = $("token").value.trim();
        const qs = new URLSearchParams({ config });
        if (token) qs.set("token", token);
        const url = "/api/scan?" + qs.toString();

        statusEl.textContent = "Scanning...";
        try {
          const res = await fetch(url, { method: "GET" });
          const data = await res.json();
          if (!res.ok || !data.ok) {
            statusEl.textContent = "Scan failed: " + (data.error || ("HTTP " + res.status));
            return;
          }
          $("m_scanned").textContent = text(data.summary?.scanned);
          $("m_buy").textContent = text(data.summary?.buy_signals);
          $("m_wait").textContent = text(data.summary?.wait_signals);
          $("m_err").textContent = text(data.summary?.errors);
          $("m_exec").textContent = text((data.execution_mode || "").toUpperCase());

          const rows = data.top_results || [];
          const tbody = $("rows");
          if (!rows.length) {
            tbody.innerHTML = '<tr><td colspan="8" class="muted">No rows in this scan.</td></tr>';
          } else {
            tbody.innerHTML = rows.map((r) =>
              `<tr>
                <td>${text(r.symbol)}</td>
                <td class="${rowClass(r.action)}">${text(r.action)}</td>
                <td>${text(r.score)}</td>
                <td>${text(r.price)}</td>
                <td>${text(r.entry)}</td>
                <td>${text(r.tp)}</td>
                <td>${text(r.sl)}</td>
                <td>${text(r.note)}</td>
              </tr>`
            ).join("");
          }
          statusEl.textContent = "Last scan: " + text(data.time);
        } catch (e) {
          statusEl.textContent = "Network error: " + e;
        }
      }

      function applyAutoRefresh() {
        if (timer) clearInterval(timer);
        const sec = Math.max(15, Number($("refresh").value || 60));
        timer = setInterval(runScan, sec * 1000);
      }

      $("runBtn").addEventListener("click", runScan);
      $("refresh").addEventListener("change", applyAutoRefresh);
      applyAutoRefresh();
    </script>
  </body>
</html>""",
                status_code=200,
            )
            return

        if path not in {"/api", "/api/scan"}:
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
