#!/usr/bin/env python3
from __future__ import annotations

import os
import time
from html import escape
from typing import Any, Dict, List

import streamlit as st

from .bot import (
    ensure_runtime_env,
    evaluate_live_execution_guard,
    format_price,
    format_turnover,
    load_json_file,
    now_utc_str,
    prepare_config_for_runtime,
    run_single_scan_with_state,
    to_float,
    validate_config,
)


def apply_custom_shell() -> None:
    st.markdown(
        """
        <style>
        @import url('https://fonts.googleapis.com/css2?family=Space+Grotesk:wght@400;500;600;700&display=swap');

        :root {
            --bg-0: #060b16;
            --bg-1: #0a1224;
            --card: rgba(15, 24, 44, 0.78);
            --card-2: rgba(10, 16, 30, 0.65);
            --text-0: #f2f5ff;
            --text-1: #9ca8c9;
            --line: rgba(112, 146, 255, 0.28);
            --buy: #24d17e;
            --sell: #ff657a;
            --wait: #ffb648;
            --neutral: #79a1ff;
        }

        .stApp {
            background:
                radial-gradient(1200px 500px at 15% -20%, rgba(75, 130, 255, 0.20), transparent 60%),
                radial-gradient(900px 400px at 95% 5%, rgba(36, 209, 126, 0.14), transparent 55%),
                linear-gradient(180deg, var(--bg-1) 0%, var(--bg-0) 100%);
            color: var(--text-0);
            font-family: "Space Grotesk", "Segoe UI", sans-serif;
        }

        #MainMenu {visibility: hidden;}
        footer {visibility: hidden;}
        header[data-testid="stHeader"] {display: none;}
        [data-testid="stToolbar"] {display: none;}
        [data-testid="collapsedControl"] {display: none;}

        section[data-testid="stSidebar"] {
            background: linear-gradient(180deg, rgba(20, 28, 47, 0.95) 0%, rgba(19, 26, 40, 0.88) 100%);
            border-right: 1px solid var(--line);
        }

        section[data-testid="stSidebar"] .stTextInput input,
        section[data-testid="stSidebar"] .stNumberInput input {
            background: rgba(6, 11, 23, 0.92) !important;
            border: 1px solid rgba(146, 164, 210, 0.24) !important;
            border-radius: 10px !important;
            color: var(--text-0) !important;
        }

        section[data-testid="stSidebar"] .stButton > button {
            border-radius: 12px !important;
            border: 0 !important;
            background: linear-gradient(120deg, #ff6a66 0%, #f05245 100%) !important;
            color: #fff !important;
            font-weight: 700 !important;
            letter-spacing: 0.2px;
        }

        .block-container {
            padding-top: 1rem !important;
            padding-bottom: 1rem !important;
            max-width: 1400px;
        }

        .hero-wrap {
            margin-bottom: 0.6rem;
            padding: 1.2rem 1.3rem 1.1rem 1.3rem;
            border: 1px solid var(--line);
            border-radius: 14px;
            background: linear-gradient(155deg, rgba(28, 42, 72, 0.66) 0%, rgba(9, 15, 28, 0.76) 100%);
            box-shadow: 0 16px 50px rgba(4, 8, 16, 0.45);
        }

        .hero-kicker {
            color: #8db2ff;
            font-size: 0.8rem;
            text-transform: uppercase;
            letter-spacing: 0.12em;
            font-weight: 600;
            margin-bottom: 0.25rem;
        }

        .hero-title {
            font-size: 2.25rem;
            line-height: 1.1;
            margin: 0;
            letter-spacing: 0.3px;
        }

        .hero-sub {
            color: var(--text-1);
            margin-top: 0.5rem;
            margin-bottom: 0;
        }

        .meta-line {
            color: var(--text-1);
            margin: 0.4rem 0 0.9rem 0;
            font-size: 0.92rem;
        }

        .meta-pill {
            display: inline-block;
            border: 1px solid rgba(119, 151, 239, 0.36);
            border-radius: 999px;
            padding: 0.08rem 0.55rem;
            margin-left: 0.35rem;
            color: #d9e6ff;
            background: rgba(73, 111, 204, 0.18);
            font-size: 0.8rem;
        }

        .metric-card {
            border: 1px solid rgba(123, 146, 200, 0.26);
            border-radius: 12px;
            padding: 0.85rem 0.85rem 0.8rem 0.85rem;
            background: linear-gradient(170deg, var(--card) 0%, var(--card-2) 100%);
            min-height: 102px;
            box-shadow: 0 10px 26px rgba(0, 0, 0, 0.26);
        }

        .metric-card .label {
            color: #9baad0;
            font-size: 0.81rem;
            letter-spacing: 0.03em;
            text-transform: uppercase;
            margin-bottom: 0.35rem;
        }

        .metric-card .value {
            color: #f8fbff;
            font-size: 1.85rem;
            line-height: 1.15;
            font-weight: 700;
        }

        .metric-card .hint {
            color: #8c9fc7;
            margin-top: 0.3rem;
            font-size: 0.78rem;
        }

        .tone-buy {border-color: rgba(45, 202, 125, 0.34);}
        .tone-sell {border-color: rgba(255, 101, 122, 0.34);}
        .tone-wait {border-color: rgba(255, 182, 72, 0.34);}
        .tone-danger {border-color: rgba(255, 101, 122, 0.44);}
        .tone-neutral {border-color: rgba(121, 161, 255, 0.34);}

        .action-badge {
            display: inline-block;
            padding: 0.22rem 0.6rem;
            border-radius: 999px;
            border: 1px solid rgba(255, 255, 255, 0.2);
            font-size: 0.8rem;
            font-weight: 700;
            letter-spacing: 0.02em;
            margin-bottom: 0.45rem;
        }
        .action-buy {color: #24d17e; border-color: rgba(36, 209, 126, 0.38); background: rgba(36, 209, 126, 0.14);}
        .action-sell {color: #ff657a; border-color: rgba(255, 101, 122, 0.38); background: rgba(255, 101, 122, 0.12);}
        .action-wait {color: #ffb648; border-color: rgba(255, 182, 72, 0.42); background: rgba(255, 182, 72, 0.12);}
        .action-neutral {color: #9cb5f5; border-color: rgba(156, 181, 245, 0.35); background: rgba(156, 181, 245, 0.10);}
        .action-danger {color: #ff7f93; border-color: rgba(255, 101, 122, 0.45); background: rgba(255, 101, 122, 0.16);}

        div[data-baseweb="tab-list"] {
            gap: 0.45rem;
            margin-bottom: 0.2rem;
        }

        button[data-baseweb="tab"] {
            border-radius: 9px;
            border: 1px solid rgba(132, 160, 232, 0.24) !important;
            background: rgba(14, 22, 38, 0.7) !important;
            color: #a9bcf3 !important;
            padding: 0.32rem 0.8rem !important;
            font-weight: 600 !important;
        }

        button[data-baseweb="tab"][aria-selected="true"] {
            background: linear-gradient(130deg, rgba(61, 113, 228, 0.35) 0%, rgba(51, 192, 132, 0.2) 100%) !important;
            color: #eff5ff !important;
            border-color: rgba(134, 174, 255, 0.55) !important;
        }

        div[data-testid="stDataFrame"] {
            border-radius: 12px !important;
            border: 1px solid rgba(130, 160, 238, 0.22) !important;
            overflow: hidden;
        }
        </style>
        """,
        unsafe_allow_html=True,
    )


def sync_streamlit_secrets_to_env() -> None:
    try:
        secrets_dict = dict(st.secrets)
    except Exception:
        secrets_dict = {}

    for key in (
        "BYBIT_API_KEY",
        "BYBIT_API_SECRET",
        "TRADING_BOT_ALLOW_LIVE",
        "TRADING_BOT_LIVE_ACK",
        "TRADING_BOT_ALLOW_MAINNET",
        "TELEGRAM_BOT_TOKEN",
        "TELEGRAM_CHAT_ID",
    ):
        if key in secrets_dict and not os.getenv(key):
            os.environ[key] = str(secrets_dict.get(key, ""))


def summarize_actions(results: List[Dict[str, Any]]) -> Dict[str, int]:
    summary: Dict[str, int] = {}
    for row in results:
        action = str(row.get("action", "WAIT"))
        summary[action] = summary.get(action, 0) + 1
    return summary


def filter_results(
    results: List[Dict[str, Any]],
    min_score: float,
    actionable_only: bool,
    action_filter: List[str],
) -> List[Dict[str, Any]]:
    filtered: List[Dict[str, Any]] = []
    for row in results:
        score = to_float(row.get("score"), 0.0)
        if score < min_score:
            continue
        action = str(row.get("action", "WAIT"))
        if actionable_only and action not in {"BUY_LIMIT", "SELL", "HOLD", "WAIT_LIMIT"}:
            continue
        if action_filter and action not in action_filter:
            continue
        filtered.append(row)
    return filtered


def build_table_rows(results: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for idx, row in enumerate(sorted(results, key=lambda x: x["score"], reverse=True), start=1):
        trend_delta = row["ema_fast"] - row["ema_slow"]
        trend_tag = "UP" if trend_delta > 0 else "DOWN"
        rows.append(
            {
                "Rank": idx,
                "Symbol": row["symbol"],
                "Source": row.get("source", "WATCHLIST"),
                "Price": format_price(row["price"]),
                "RSI": round(float(row["rsi"]), 1),
                "Trend": trend_tag,
                "Score": round(float(row["score"]), 2),
                "Action": row.get("action", "WAIT"),
                "Entry": format_price(row["wait_price"]) if row.get("wait_price") else "-",
                "TP (Limit Sell)": format_price(row["tp_price"]) if row.get("tp_price") else "-",
                "SL": format_price(row["sl_price"]) if row.get("sl_price") else "-",
                "Qty (Risk-Based)": round(float(row["qty"]), 4) if row.get("qty") else None,
                "Risk Budget": (
                    round(float(row["risk_budget_usdt"]), 2) if row.get("risk_budget_usdt") else None
                ),
                "Risk USDT": round(float(row["risk_usdt"]), 2) if row.get("risk_usdt") else None,
                "Spread %": round(float(row["spread_pct"]), 3) if row.get("spread_pct") is not None else None,
                "Turnover 24h": (
                    format_turnover(float(row["turnover_24h_usdt"])) if row.get("turnover_24h_usdt") else "-"
                ),
                "Net Return %": (
                    round(float(row["net_return_pct"]), 2) if row.get("net_return_pct") is not None else None
                ),
                "Execution": (
                    ("OK" if row.get("execution", {}).get("success") else "FAILED")
                    if row.get("execution")
                    else "-"
                ),
                "Signal": row.get("signal") or "-",
                "Note": row.get("note", ""),
            }
        )
    return rows


def run_scan(config_path: str, monitor_only: bool = True) -> Dict[str, Any]:
    ensure_runtime_env()
    config = load_json_file(config_path, None)
    if config is None:
        raise FileNotFoundError(
            f"Config not found: {config_path}. Copy configs/config.example.json to configs/config.json first."
        )
    runtime_config = prepare_config_for_runtime(config)
    if monitor_only:
        exec_cfg = runtime_config.setdefault("execution", {})
        exec_cfg["mode"] = "paper"
        notes = runtime_config.setdefault("_runtime_notes", [])
        if isinstance(notes, list):
            notes.append("UI monitor-only mode: execution.mode forced to 'paper'.")
    validate_config(runtime_config)
    cycle = run_single_scan_with_state(runtime_config, persist_state=not monitor_only)

    cycle["config"] = runtime_config
    cycle["scanned_at"] = now_utc_str()
    cycle["monitor_only"] = bool(monitor_only)
    return cycle


def format_profit_factor(value: Any) -> str:
    if value == float("inf"):
        return "INF"
    try:
        return f"{float(value):.2f}"
    except (TypeError, ValueError):
        return "0.00"


def action_tone(action: str) -> str:
    normalized = action.upper()
    if normalized == "BUY_LIMIT":
        return "buy"
    if normalized == "SELL":
        return "sell"
    if normalized.startswith("WAIT"):
        return "wait"
    if normalized in {"FAILED", "BLOCKED"}:
        return "danger"
    return "neutral"


def format_price_or_dash(value: Any) -> str:
    if value is None:
        return "-"
    try:
        return format_price(float(value))
    except (TypeError, ValueError):
        return "-"


def render_metric_card(
    col: Any,
    label: str,
    value: Any,
    tone: str = "neutral",
    hint: str | None = None,
) -> None:
    safe_label = escape(str(label))
    safe_value = escape(str(value))
    safe_hint = escape(str(hint)) if hint else ""
    hint_html = f"<div class='hint'>{safe_hint}</div>" if safe_hint else ""
    col.markdown(
        (
            f"<div class='metric-card tone-{tone}'>"
            f"<div class='label'>{safe_label}</div>"
            f"<div class='value'>{safe_value}</div>"
            f"{hint_html}"
            "</div>"
        ),
        unsafe_allow_html=True,
    )


def action_badge(action: str) -> str:
    tone = action_tone(action)
    return (
        f"<span class='action-badge action-{tone}'>{escape(action)}</span>"
    )


def summarize_focus_execution_status(
    symbol: str,
    pick_action: str,
    cycle: Dict[str, Any],
) -> Dict[str, str]:
    positions = cycle.get("state", {}).get("positions", {})
    live_positions = cycle.get("live_open_positions", {}) or cycle.get("state", {}).get("live_open_positions", {})
    live_pending = cycle.get("live_pending_entries", {}) or cycle.get("state", {}).get("live_pending_entries", {})
    if symbol in live_positions:
        return {
            "label": "FILLED",
            "tone": "buy",
            "message": "Live position detected on exchange.",
        }
    if symbol in live_pending:
        status = str(live_pending.get(symbol, {}).get("status", "NEW"))
        return {
            "label": "ORDER_SUBMITTED",
            "tone": "buy",
            "message": f"Open entry order on exchange ({status}).",
        }
    if symbol in positions:
        return {
            "label": "FILLED",
            "tone": "buy",
            "message": "Position is open for this symbol.",
        }

    execution_events = cycle.get("execution_events", [])
    for event in reversed(execution_events):
        if str(event.get("symbol", "")) != symbol:
            continue
        result = event.get("result", {})
        msg = str(result.get("message", "")).strip()
        if bool(result.get("success")):
            if bool(result.get("submitted")):
                return {
                    "label": "ORDER_SUBMITTED",
                    "tone": "buy",
                    "message": msg or "Entry/exit plan submitted to Bybit.",
                }
            return {
                "label": "NOT_SUBMITTED",
                "tone": "wait",
                "message": msg or "No exchange order was submitted.",
            }
        return {
            "label": "FAILED",
            "tone": "danger",
            "message": msg or "Order submission failed.",
        }

    if pick_action == "BUY_LIMIT":
        return {
            "label": "WAITING_TRIGGER",
            "tone": "wait",
            "message": "No submission event in this cycle yet.",
        }
    return {
        "label": "NO_ACTIVE_ORDER",
        "tone": "neutral",
        "message": "No actionable order for this symbol in this cycle.",
    }


def main() -> None:
    sync_streamlit_secrets_to_env()
    ensure_runtime_env()
    st.set_page_config(page_title="Crypto Alert Bot UI", layout="wide")
    apply_custom_shell()
    st.markdown(
        """
        <section class="hero-wrap">
          <div class="hero-kicker">Bybit Signal Desk</div>
          <h1 class="hero-title">Crypto Alert Bot Command Center</h1>
          <p class="hero-sub">Professional BUY / WAIT / SELL flow with risk guardrails and live execution visibility.</p>
        </section>
        """,
        unsafe_allow_html=True,
    )

    with st.sidebar:
        st.header("Controls")
        config_path = st.text_input("Config file", value="configs/config.json")
        auto_refresh = st.checkbox("Auto refresh", value=False)
        actionable_only = st.checkbox("Show actionable only", value=False)
        min_score = st.slider("Minimum score", min_value=-5.0, max_value=15.0, value=-5.0, step=0.1)
        action_filter = st.multiselect(
            "Action filter",
            options=["BUY_LIMIT", "WAIT_LIMIT", "WAIT_PULLBACK", "WAIT_CONFIRM", "WAIT", "HOLD", "SELL"],
            default=[],
        )
        refresh_seconds = st.number_input(
            "Refresh every (seconds)",
            min_value=10,
            max_value=600,
            value=30,
            step=5,
        )
        run_now = st.button("Run Scan Now", type="primary", width="stretch")

    if "last_cycle" not in st.session_state:
        st.session_state["last_cycle"] = None

    if run_now or st.session_state["last_cycle"] is None or auto_refresh:
        try:
            st.session_state["last_cycle"] = run_scan(config_path=config_path, monitor_only=True)
        except Exception as e:
            st.error(str(e))

    cycle = st.session_state["last_cycle"]
    if cycle:
        results = cycle["results"]
        alerts = cycle["alerts"]
        errors = cycle["errors"]
        auto_added_symbols = cycle.get("auto_added_symbols", [])
        risk_state = cycle.get("risk_state", {})
        execution_events = cycle.get("execution_events", [])
        performance = cycle.get("performance", {})
        performance_overall = performance.get("overall", {})
        performance_7d = performance.get("last_7d", {})
        recent_closed_trades = cycle.get("recent_closed_trades", [])
        execution_mode = str(cycle.get("config", {}).get("execution", {}).get("mode", "paper")).upper()
        guard = evaluate_live_execution_guard(
            config=cycle.get("config", {}),
            exchange_cfg=cycle.get("config", {}).get("exchange", {}),
        )
        if bool(cycle.get("monitor_only", False)):
            st.info("Monitoring-only mode: UI scans never submit live orders and do not persist state.")

        filtered_results = filter_results(
            results=results,
            min_score=min_score,
            actionable_only=actionable_only,
            action_filter=action_filter,
        )
        table_rows = build_table_rows(filtered_results)

        action_summary = summarize_actions(results)
        buy_count = int(action_summary.get("BUY_LIMIT", 0))
        sell_count = int(action_summary.get("SELL", 0))
        wait_count = int(sum(v for k, v in action_summary.items() if k.startswith("WAIT")))
        live_open_positions = cycle.get("live_open_positions", {}) or cycle.get("state", {}).get("live_open_positions", {})
        live_pending_entries = cycle.get("live_pending_entries", {}) or cycle.get("state", {}).get("live_pending_entries", {})
        open_positions = int(
            len(live_open_positions) if isinstance(live_open_positions, dict) and live_open_positions else len(cycle.get("state", {}).get("positions", {}))
        )
        pending_entries_count = int(len(live_pending_entries)) if isinstance(live_pending_entries, dict) else 0

        st.subheader("Control Center")
        status_col1, status_col2, status_col3, status_col4, status_col5, status_col6 = st.columns(6)
        render_metric_card(status_col1, "Scanned", len(results))
        render_metric_card(status_col2, "Buy Signals", buy_count, tone="buy")
        render_metric_card(status_col3, "Sell Signals", sell_count, tone="sell")
        render_metric_card(status_col4, "Wait Signals", wait_count, tone="wait")
        render_metric_card(status_col5, "Open Positions", open_positions)
        render_metric_card(
            status_col6,
            "Errors",
            len(errors),
            tone="danger" if errors else "neutral",
        )
        st.markdown(
            (
                "<div class='meta-line'>"
                f"Scanned at <span class='meta-pill'>{escape(cycle['scanned_at'])}</span>"
                f"<span class='meta-pill'>Execution: {escape(execution_mode)}</span>"
                f"<span class='meta-pill'>Pending Entries: {pending_entries_count}</span>"
                "</div>"
            ),
            unsafe_allow_html=True,
        )

        if execution_mode == "LIVE":
            if guard.get("allowed"):
                lane = "testnet" if guard.get("is_testnet") else "mainnet"
                st.success(f"Live safety guard unlocked ({lane}).")
            else:
                st.error("Live safety guard is locking execution.")
                for issue in guard.get("issues", []):
                    st.caption(f"- {issue}")

        if auto_added_symbols:
            st.info(f"Auto-added from Bybit spot this cycle: {', '.join(auto_added_symbols)}")

        tab_decision, tab_market, tab_performance, tab_ops = st.tabs(
            ["Decision", "Market", "Performance", "Ops"]
        )

        with tab_decision:
            if results:
                sorted_results = sorted(results, key=lambda x: x["score"], reverse=True)
                best = sorted_results[0]
                buy_candidates = [row for row in sorted_results if row.get("action") == "BUY_LIMIT"]
                wait_candidates = [row for row in sorted_results if row.get("wait_price")]

                if buy_candidates:
                    pick = buy_candidates[0]
                    pick_context = "Top actionable BUY setup"
                elif wait_candidates:
                    pick = wait_candidates[0]
                    pick_context = "Best pending setup"
                else:
                    pick = best
                    pick_context = "Momentum leader only (no actionable entry)"

                pick_action = str(pick.get("action", "WAIT"))
                tone = action_tone(pick_action)
                exec_status = summarize_focus_execution_status(
                    symbol=str(pick.get("symbol", "")),
                    pick_action=pick_action,
                    cycle=cycle,
                )

                st.markdown(
                    action_badge(pick_action),
                    unsafe_allow_html=True,
                )
                st.markdown(
                    f"<span class='action-badge action-{exec_status['tone']}'>{escape(exec_status['label'])}</span>",
                    unsafe_allow_html=True,
                )
                st.caption(
                    f"{pick_context}. Best momentum this cycle: {best['symbol']} (score {float(best['score']):.2f})."
                )
                st.caption(f"Execution status: {exec_status['message']}")

                dcol1, dcol2, dcol3, dcol4 = st.columns(4)
                render_metric_card(dcol1, "Focus Symbol", pick.get("symbol", "-"), tone=tone)
                render_metric_card(dcol2, "Action", pick_action, tone=tone)
                render_metric_card(dcol3, "Entry", format_price_or_dash(pick.get("wait_price")), tone=tone)
                render_metric_card(
                    dcol4,
                    "Limit Sell",
                    format_price_or_dash(pick.get("tp_price")),
                    tone=tone,
                )
                st.caption(
                    f"Suggested stop-loss for {pick.get('symbol', '-')}: {format_price_or_dash(pick.get('sl_price'))}"
                )
                if pick.get("order_plan"):
                    with st.expander("Bybit Order Payload (Focused Setup)"):
                        st.json(pick["order_plan"])
            else:
                st.warning("No market results returned in this cycle.")

            if risk_state:
                paused = bool(risk_state.get("paused"))
                cooldown_until_ts = float(risk_state.get("cooldown_until_ts", 0.0) or 0.0)
                cooldown_left_s = max(0, int(cooldown_until_ts - time.time()))
                risk_col1, risk_col2, risk_col3, risk_col4 = st.columns(4)
                risk_col1.metric(
                    "Daily PnL (USDT)",
                    f"{float(risk_state.get('daily_realized_pnl_usdt', 0.0)):.2f}",
                )
                risk_col2.metric("Consecutive Losses", int(risk_state.get("consecutive_losses", 0)))
                risk_col3.metric("Trading Paused", "YES" if paused else "NO")
                risk_col4.metric("Cooldown (sec)", cooldown_left_s)
                if paused and risk_state.get("pause_reason"):
                    st.warning(f"Circuit breaker active: {risk_state.get('pause_reason')}")
                if cooldown_left_s > 0:
                    st.warning(
                        f"Loss cooldown active ({cooldown_left_s}s): "
                        f"{risk_state.get('cooldown_reason', 'post-loss cooldown')}"
                    )

        with tab_market:
            st.caption(
                f"Showing {len(table_rows)} of {len(results)} rows "
                f"(min score {min_score:.1f}, actionable_only={actionable_only})"
            )
            if table_rows:
                st.dataframe(table_rows, width="stretch", hide_index=True)
            else:
                st.info("No rows match the current filters.")

        with tab_performance:
            perf_col1, perf_col2 = st.columns(2)
            perf_col1.markdown("**Overall**")
            perf_col1.metric("Trades", int(performance_overall.get("total_trades", 0)))
            perf_col1.metric("Win Rate", f"{float(performance_overall.get('win_rate_pct', 0.0)):.1f}%")
            perf_col1.metric("Net PnL (USDT)", f"{float(performance_overall.get('net_pnl_usdt', 0.0)):.2f}")
            perf_col1.metric("Profit Factor", format_profit_factor(performance_overall.get("profit_factor", 0.0)))
            perf_col1.metric("Expectancy (USDT)", f"{float(performance_overall.get('expectancy_usdt', 0.0)):.2f}")
            perf_col1.metric(
                "Max Drawdown (USDT)",
                f"{float(performance_overall.get('max_drawdown_usdt', 0.0)):.2f}",
            )

            perf_col2.markdown("**Last 7 Days**")
            perf_col2.metric("Trades", int(performance_7d.get("total_trades", 0)))
            perf_col2.metric("Win Rate", f"{float(performance_7d.get('win_rate_pct', 0.0)):.1f}%")
            perf_col2.metric("Net PnL (USDT)", f"{float(performance_7d.get('net_pnl_usdt', 0.0)):.2f}")
            perf_col2.metric("Profit Factor", format_profit_factor(performance_7d.get("profit_factor", 0.0)))
            perf_col2.metric("Expectancy (USDT)", f"{float(performance_7d.get('expectancy_usdt', 0.0)):.2f}")
            perf_col2.metric(
                "Max Drawdown (USDT)",
                f"{float(performance_7d.get('max_drawdown_usdt', 0.0)):.2f}",
            )

            st.markdown("**Recent Closed Trades**")
            if recent_closed_trades:
                journal_rows: List[Dict[str, Any]] = []
                for row in recent_closed_trades:
                    journal_rows.append(
                        {
                            "Closed At": row.get("closed_at", "-"),
                            "Symbol": row.get("symbol", "-"),
                            "Signal": row.get("exit_signal", "-"),
                            "Entry": format_price(to_float(row.get("entry_price"), 0.0)),
                            "Exit": format_price(to_float(row.get("exit_price"), 0.0)),
                            "Qty": round(to_float(row.get("qty"), 0.0), 6),
                            "Net Return %": round(to_float(row.get("net_return_pct"), 0.0), 2),
                            "PnL (USDT)": round(to_float(row.get("pnl_usdt"), 0.0), 2),
                            "Source": row.get("source", "-"),
                        }
                    )
                st.dataframe(journal_rows, width="stretch", hide_index=True)
            else:
                st.info("No closed trades yet. Metrics will appear after exits.")

        with tab_ops:
            st.markdown("**Execution Events**")
            if execution_events:
                exec_rows: List[Dict[str, Any]] = []
                for event in execution_events:
                    result = event.get("result", {})
                    if bool(result.get("success")):
                        status = "ORDER_SUBMITTED" if bool(result.get("submitted")) else "NOT_SUBMITTED"
                    else:
                        status = "FAILED"
                    exec_rows.append(
                        {
                            "Time": event.get("time", "-"),
                            "Symbol": event.get("symbol", "-"),
                            "Status": status,
                            "Submitted": "YES" if bool(result.get("submitted")) else "NO",
                            "Message": result.get("message", "-"),
                        }
                    )
                st.dataframe(exec_rows, width="stretch", hide_index=True)
            else:
                st.info("No execution events in this cycle.")

            st.markdown("**Alerts**")
            if alerts:
                for idx, alert in enumerate(alerts, start=1):
                    st.success(f"{idx}. {alert}")
            else:
                st.info("No new trade alerts in this cycle.")

            st.markdown("**Errors**")
            if errors:
                for err in errors:
                    st.error(err)
            else:
                st.info("No errors in this cycle.")

    if auto_refresh:
        st.caption(f"Auto refresh enabled: rerunning every {refresh_seconds} seconds.")
        time.sleep(int(refresh_seconds))
        st.rerun()


if __name__ == "__main__":
    main()
