#!/usr/bin/env python3
"""
Free crypto alert bot:
- Pulls public candle data from Binance/Bybit (no API key required)
- Generates LIMIT BUY / SELL / TAKE PROFIT alerts
- Keeps lightweight local paper-position state for TP/SL logic
- Can notify Telegram (also free)
"""

from __future__ import annotations

import argparse
import copy
import hashlib
import json
import os
import time
import urllib.error
import urllib.parse
import urllib.request
from decimal import Decimal, ROUND_DOWN
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from .bybit_client import (
    build_json_compact as client_build_json_compact,
    bybit_signed_get as client_bybit_signed_get,
    bybit_signed_get_with_fallback as client_bybit_signed_get_with_fallback,
    bybit_signed_post as client_bybit_signed_post,
    bybit_signed_post_with_fallback as client_bybit_signed_post_with_fallback,
    cancel_bybit_order as client_cancel_bybit_order,
    extract_bybit_order_id as client_extract_bybit_order_id,
    fetch_bybit_instrument_constraints as client_fetch_bybit_instrument_constraints,
    fetch_bybit_live_position_for_symbol as client_fetch_bybit_live_position_for_symbol,
    fetch_bybit_open_orders_for_symbol as client_fetch_bybit_open_orders_for_symbol,
    fetch_bybit_tickers as client_fetch_bybit_tickers,
    get_bybit_base_urls as client_get_bybit_base_urls,
    is_active_open_order_status as client_is_active_open_order_status,
    is_bybit_duplicate_order_response as client_is_bybit_duplicate_order_response,
)


DEFAULT_STATE_FILE = "state/bot_state.json"
DEFAULT_VERCEL_STATE_FILE = "/tmp/trading_bot_state.json"
DEFAULT_JOURNAL_LIMIT = 5000
SUPPORTED_EXCHANGES = {"binance", "bybit"}
DEFAULT_EXCLUDED_STABLE_SYMBOLS = {
    "USDCUSDT",
    "USDEUSDT",
    "FDUSDUSDT",
    "DAIUSDT",
    "TUSDUSDT",
    "PYUSDUSDT",
    "USDTUSDT",
}

BYBIT_INTERVAL_MAP: Dict[str, str] = {
    "1m": "1",
    "3m": "3",
    "5m": "5",
    "15m": "15",
    "30m": "30",
    "1h": "60",
    "2h": "120",
    "4h": "240",
    "6h": "360",
    "12h": "720",
    "1d": "D",
    "1w": "W",
    "1M": "M",
}

BINANCE_INTERVAL_MAP: Dict[str, str] = {
    "1": "1m",
    "3": "3m",
    "5": "5m",
    "15": "15m",
    "30": "30m",
    "60": "1h",
    "120": "2h",
    "240": "4h",
    "360": "6h",
    "720": "12h",
    "D": "1d",
    "W": "1w",
    "M": "1M",
}

TRUTHY_ENV_VALUES = {"1", "true", "yes", "on"}
LIVE_ACK_DEFAULT = "I_UNDERSTAND_LIVE_TRADING_RISK"
_ENV_LOADED = False


def build_default_state() -> Dict[str, Any]:
    return {
        "positions": {},
        "last_alerts": {},
        "live_open_positions": {},
        "live_pending_entries": {},
    }


def parse_env_bool(value: Any, default: bool = False) -> bool:
    if value is None:
        return default
    return str(value).strip().lower() in TRUTHY_ENV_VALUES


def load_env_file(path: str = ".env", override: bool = False) -> Dict[str, str]:
    loaded: Dict[str, str] = {}
    try:
        with open(path, "r", encoding="utf-8") as f:
            for raw_line in f:
                line = raw_line.strip()
                if not line or line.startswith("#"):
                    continue
                if line.startswith("export "):
                    line = line[7:].strip()
                if "=" not in line:
                    continue
                key, raw_value = line.split("=", 1)
                key = key.strip()
                value = raw_value.strip()
                if not key:
                    continue

                if value.startswith(("'", '"')) and value.endswith(("'", '"')) and len(value) >= 2:
                    value = value[1:-1]
                else:
                    comment_index = value.find(" #")
                    if comment_index >= 0:
                        value = value[:comment_index].strip()
                if override or key not in os.environ:
                    os.environ[key] = value
                    loaded[key] = value
    except FileNotFoundError:
        return {}
    return loaded


def ensure_runtime_env(path: str = ".env") -> None:
    global _ENV_LOADED
    if _ENV_LOADED:
        return
    load_env_file(path=path, override=False)
    _ENV_LOADED = True


def is_running_on_vercel() -> bool:
    if parse_env_bool(os.getenv("VERCEL"), False):
        return True
    return bool(str(os.getenv("VERCEL_ENV", "")).strip())


def apply_runtime_safety_overrides(config: Dict[str, Any]) -> List[str]:
    """
    Apply deployment-time safety overrides.

    On Vercel, default behavior is:
    - Force paper mode unless TRADING_BOT_ALLOW_LIVE_ON_VERCEL=true
    - Use /tmp state file unless TRADING_BOT_FORCE_CONFIG_STATE_FILE=true
    """
    ensure_runtime_env()
    notes: List[str] = []
    if not is_running_on_vercel():
        return notes

    exec_cfg = config.setdefault("execution", {})
    mode = str(exec_cfg.get("mode", "paper")).lower()
    allow_live_on_vercel = parse_env_bool(os.getenv("TRADING_BOT_ALLOW_LIVE_ON_VERCEL"), False)
    if mode == "live" and not allow_live_on_vercel:
        exec_cfg["mode"] = "paper"
        notes.append(
            "Vercel safety override: execution.mode forced to 'paper'. "
            "Set TRADING_BOT_ALLOW_LIVE_ON_VERCEL=true to opt in."
        )

    keep_config_state_file = parse_env_bool(os.getenv("TRADING_BOT_FORCE_CONFIG_STATE_FILE"), False)
    if not keep_config_state_file:
        config["state_file"] = os.getenv("TRADING_BOT_STATE_FILE", DEFAULT_VERCEL_STATE_FILE)
        notes.append(f"Vercel runtime state file: {config['state_file']}")

    return notes


def load_json_file(path: str, fallback: Any) -> Any:
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except FileNotFoundError:
        return fallback


def save_json_file(path: str, payload: Any) -> None:
    parent = os.path.dirname(path)
    if parent:
        os.makedirs(parent, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2)


def prepare_config_for_runtime(config: Dict[str, Any]) -> Dict[str, Any]:
    """
    Return a config copy with runtime/deployment safety overrides applied.
    """
    cfg = copy.deepcopy(config)
    notes = apply_runtime_safety_overrides(cfg)
    if notes:
        cfg["_runtime_notes"] = notes
    return cfg


def now_utc_str() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")


def now_utc_ts() -> float:
    return time.time()


def utc_day_str() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")


def format_price(price: float) -> str:
    if price >= 1000:
        return f"{price:.2f}"
    if price >= 1:
        return f"{price:.4f}"
    return f"{price:.6f}"


def format_turnover(value: float) -> str:
    if value >= 1_000_000_000:
        return f"{value / 1_000_000_000:.2f}B"
    if value >= 1_000_000:
        return f"{value / 1_000_000:.2f}M"
    if value >= 1_000:
        return f"{value / 1_000:.2f}K"
    return f"{value:.2f}"


def build_exit_prices(entry_price: float, tp_pct: float, sl_pct: float) -> Dict[str, float]:
    return {
        "tp_price": entry_price * (1 + tp_pct),
        "sl_price": entry_price * (1 - sl_pct),
    }


def to_float(value: Any, fallback: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return fallback


def get_execution_costs_config(config: Dict[str, Any]) -> Dict[str, float]:
    costs_cfg = config.get("execution_costs", {})
    return {
        "entry_fee_pct": max(0.0, to_float(costs_cfg.get("entry_fee_pct"), 0.0)),
        "exit_fee_pct": max(0.0, to_float(costs_cfg.get("exit_fee_pct"), 0.0)),
        "entry_slippage_pct": max(0.0, to_float(costs_cfg.get("entry_slippage_pct"), 0.0)),
        "exit_slippage_pct": max(0.0, to_float(costs_cfg.get("exit_slippage_pct"), 0.0)),
    }


def get_liquidity_filter_config(config: Dict[str, Any]) -> Dict[str, Any]:
    liq_cfg = config.get("liquidity_filter", {})
    return {
        "enabled": bool(liq_cfg.get("enabled", False)),
        "max_spread_pct": max(0.0, to_float(liq_cfg.get("max_spread_pct"), 0.0)),
        "min_turnover_24h_usdt": max(0.0, to_float(liq_cfg.get("min_turnover_24h_usdt"), 0.0)),
        "block_when_ticker_missing": bool(liq_cfg.get("block_when_ticker_missing", True)),
    }


def get_price_filter_config(config: Dict[str, Any]) -> Dict[str, Any]:
    # Backward-compatible fallback: allow spot_discovery.max_price_usdt if top-level price_filter is absent.
    pf_cfg = config.get("price_filter", {})
    discovery_cfg = config.get("spot_discovery", {})
    max_price = to_float(
        pf_cfg.get("max_price_usdt"),
        to_float(discovery_cfg.get("max_price_usdt"), 0.0),
    )
    return {
        "enabled": bool(pf_cfg.get("enabled", max_price > 0)),
        "max_price_usdt": max(0.0, max_price),
        "apply_to_watchlist": bool(pf_cfg.get("apply_to_watchlist", True)),
        "apply_to_spot_discovery": bool(pf_cfg.get("apply_to_spot_discovery", True)),
    }


def compute_effective_trade_prices(
    entry_price: float,
    exit_price: float,
    costs_cfg: Dict[str, float],
) -> Dict[str, float]:
    if entry_price <= 0 or exit_price <= 0:
        return {"effective_entry": 0.0, "effective_exit": 0.0}
    entry_slip = max(0.0, to_float(costs_cfg.get("entry_slippage_pct"), 0.0)) / 100.0
    exit_slip = max(0.0, to_float(costs_cfg.get("exit_slippage_pct"), 0.0)) / 100.0
    entry_fee = max(0.0, to_float(costs_cfg.get("entry_fee_pct"), 0.0)) / 100.0
    exit_fee = max(0.0, to_float(costs_cfg.get("exit_fee_pct"), 0.0)) / 100.0
    effective_entry = entry_price * (1 + entry_slip) * (1 + entry_fee)
    effective_exit = exit_price * (1 - exit_slip) * (1 - exit_fee)
    return {"effective_entry": effective_entry, "effective_exit": effective_exit}


def compute_net_return_pct(
    entry_price: float,
    exit_price: float,
    costs_cfg: Dict[str, float],
) -> float:
    effective = compute_effective_trade_prices(entry_price=entry_price, exit_price=exit_price, costs_cfg=costs_cfg)
    effective_entry = effective["effective_entry"]
    effective_exit = effective["effective_exit"]
    if effective_entry <= 0 or effective_exit <= 0:
        return 0.0
    return ((effective_exit / effective_entry) - 1.0) * 100.0


def compute_trade_pnl_usdt(
    entry_price: float,
    exit_price: float,
    qty: float,
    costs_cfg: Dict[str, float],
) -> float:
    if qty <= 0:
        return 0.0
    effective = compute_effective_trade_prices(entry_price=entry_price, exit_price=exit_price, costs_cfg=costs_cfg)
    return (effective["effective_exit"] - effective["effective_entry"]) * qty


def get_journal_config(config: Dict[str, Any]) -> Dict[str, Any]:
    journal_cfg = config.get("journal", {})
    max_closed = int(journal_cfg.get("max_closed_trades", DEFAULT_JOURNAL_LIMIT))
    return {
        "enabled": bool(journal_cfg.get("enabled", True)),
        "max_closed_trades": max(100, max_closed),
    }


def ensure_trade_history_state(state: Dict[str, Any]) -> List[Dict[str, Any]]:
    trade_history = state.setdefault("trade_history", [])
    if not isinstance(trade_history, list):
        trade_history = []
        state["trade_history"] = trade_history
    return trade_history


def add_closed_trade_to_history(
    config: Dict[str, Any],
    state: Dict[str, Any],
    trade: Dict[str, Any],
) -> None:
    journal_cfg = get_journal_config(config)
    if not journal_cfg.get("enabled"):
        return
    trade_history = ensure_trade_history_state(state)
    trade_history.append(trade)
    max_closed = int(journal_cfg.get("max_closed_trades", DEFAULT_JOURNAL_LIMIT))
    overflow = len(trade_history) - max_closed
    if overflow > 0:
        del trade_history[:overflow]


def compute_trade_metrics(
    trades: List[Dict[str, Any]],
    lookback_days: Optional[int] = None,
    now_ts: Optional[float] = None,
) -> Dict[str, Any]:
    current_ts = now_utc_ts() if now_ts is None else now_ts
    lookback_seconds = (lookback_days * 24 * 60 * 60) if lookback_days else None
    filtered: List[Dict[str, Any]] = []

    for trade in trades:
        closed_at_ts = to_float(trade.get("closed_at_ts"), 0.0)
        if closed_at_ts <= 0:
            continue
        if lookback_seconds is not None and closed_at_ts < (current_ts - lookback_seconds):
            continue
        filtered.append(trade)

    filtered.sort(key=lambda x: to_float(x.get("closed_at_ts"), 0.0))
    total = len(filtered)
    if total == 0:
        return {
            "total_trades": 0,
            "wins": 0,
            "losses": 0,
            "win_rate_pct": 0.0,
            "gross_profit_usdt": 0.0,
            "gross_loss_usdt": 0.0,
            "net_pnl_usdt": 0.0,
            "avg_win_usdt": 0.0,
            "avg_loss_usdt": 0.0,
            "profit_factor": 0.0,
            "expectancy_usdt": 0.0,
            "max_drawdown_usdt": 0.0,
            "best_trade_usdt": 0.0,
            "worst_trade_usdt": 0.0,
        }

    pnl_values = [to_float(trade.get("pnl_usdt"), 0.0) for trade in filtered]
    wins = [pnl for pnl in pnl_values if pnl > 0]
    losses = [pnl for pnl in pnl_values if pnl < 0]
    gross_profit = sum(wins)
    gross_loss = abs(sum(losses))
    net_pnl = sum(pnl_values)
    win_rate = (len(wins) / total) * 100.0
    avg_win = (gross_profit / len(wins)) if wins else 0.0
    avg_loss = (gross_loss / len(losses)) if losses else 0.0
    expectancy = net_pnl / total

    if gross_loss > 0:
        profit_factor = gross_profit / gross_loss
    elif gross_profit > 0:
        profit_factor = float("inf")
    else:
        profit_factor = 0.0

    equity = 0.0
    peak = 0.0
    max_drawdown = 0.0
    for pnl in pnl_values:
        equity += pnl
        if equity > peak:
            peak = equity
        drawdown = peak - equity
        if drawdown > max_drawdown:
            max_drawdown = drawdown

    return {
        "total_trades": total,
        "wins": len(wins),
        "losses": len(losses),
        "win_rate_pct": win_rate,
        "gross_profit_usdt": gross_profit,
        "gross_loss_usdt": gross_loss,
        "net_pnl_usdt": net_pnl,
        "avg_win_usdt": avg_win,
        "avg_loss_usdt": avg_loss,
        "profit_factor": profit_factor,
        "expectancy_usdt": expectancy,
        "max_drawdown_usdt": max_drawdown,
        "best_trade_usdt": max(pnl_values),
        "worst_trade_usdt": min(pnl_values),
    }


def build_recent_closed_trades(
    trades: List[Dict[str, Any]],
    limit: int = 25,
) -> List[Dict[str, Any]]:
    rows = sorted(
        trades,
        key=lambda x: to_float(x.get("closed_at_ts"), 0.0),
        reverse=True,
    )
    return rows[: max(1, limit)]


def dedupe_urls(urls: List[str]) -> List[str]:
    seen = set()
    result: List[str] = []
    for raw in urls:
        url = str(raw).strip().rstrip("/")
        if not url or url in seen:
            continue
        seen.add(url)
        result.append(url)
    return result


def floor_to_step(value: float, step: float) -> float:
    if value <= 0:
        return 0.0
    if step <= 0:
        return value
    d_value = Decimal(str(value))
    d_step = Decimal(str(step))
    units = (d_value / d_step).to_integral_value(rounding=ROUND_DOWN)
    floored = units * d_step
    return float(floored)


def round_price_to_tick(price: float, tick_size: float) -> float:
    if price <= 0:
        return 0.0
    if tick_size <= 0:
        return price
    return floor_to_step(price, tick_size)


def build_order_group_id(symbol: str, close_time: Any) -> str:
    raw = f"{str(symbol).upper()}:{str(close_time)}"
    digest = hashlib.sha1(raw.encode("utf-8")).hexdigest()[:12]
    return f"cab{digest}"


def build_order_link_id(order_group_id: str, leg: str) -> str:
    clean_group = "".join(ch for ch in str(order_group_id) if ch.isalnum()).lower()
    clean_leg = "".join(ch for ch in str(leg) if ch.isalnum()).lower()
    if not clean_group:
        clean_group = "cabfallback"
    if not clean_leg:
        clean_leg = "x"
    # Bybit orderLinkId max length is 36 chars.
    return f"{clean_group[:32]}{clean_leg[:4]}"


def get_bybit_base_urls(exchange_cfg: Dict[str, Any]) -> List[str]:
    return client_get_bybit_base_urls(exchange_cfg)


def fetch_bybit_instrument_constraints(
    base_urls: List[str],
    category: str,
    symbol: str,
) -> Dict[str, float]:
    return client_fetch_bybit_instrument_constraints(base_urls=base_urls, category=category, symbol=symbol)


def calculate_position_size(entry_price: float, stop_price: float, risk_usdt: float) -> float:
    distance = abs(entry_price - stop_price)
    if distance <= 0 or risk_usdt <= 0:
        return 0.0
    return risk_usdt / distance


def get_state_total_realized_pnl_usdt(state: Optional[Dict[str, Any]]) -> float:
    if not isinstance(state, dict):
        return 0.0
    trade_history = state.get("trade_history", [])
    if not isinstance(trade_history, list):
        return 0.0
    total = 0.0
    for row in trade_history:
        if not isinstance(row, dict):
            continue
        total += to_float(row.get("pnl_usdt"), 0.0)
    return total


def get_compounding_config(config: Dict[str, Any]) -> Dict[str, float]:
    risk_cfg = config.get("risk", {})
    comp_cfg = risk_cfg.get("compounding", {})
    return {
        "enabled": bool(comp_cfg.get("enabled", False)),
        "position_notional_pct_of_equity": max(
            0.0, to_float(comp_cfg.get("position_notional_pct_of_equity"), 0.0)
        ),
        "min_position_notional_usdt": max(
            0.0, to_float(comp_cfg.get("min_position_notional_usdt"), 0.0)
        ),
        "max_position_notional_usdt": max(
            0.0,
            to_float(
                comp_cfg.get("max_position_notional_usdt"),
                to_float(risk_cfg.get("max_position_notional_usdt"), 0.0),
            ),
        ),
    }


def get_risk_limits(
    config: Dict[str, Any],
    state: Optional[Dict[str, Any]] = None,
    live_equity_override_usdt: Optional[float] = None,
) -> Dict[str, float]:
    risk_cfg = config.get("risk", {})
    base_equity = to_float(risk_cfg.get("account_equity_usdt"), 0.0)
    realized_pnl = get_state_total_realized_pnl_usdt(state)
    effective_equity = max(0.0, base_equity + realized_pnl)
    live_equity = to_float(live_equity_override_usdt, 0.0)
    compounding_equity = live_equity if live_equity > 0 else effective_equity
    risk_per_trade_pct = to_float(risk_cfg.get("risk_per_trade_pct"), 0.0)
    max_daily_loss_pct = to_float(risk_cfg.get("max_daily_loss_pct"), 0.0)
    max_position_notional = to_float(risk_cfg.get("max_position_notional_usdt"), 0.0)

    comp_cfg = get_compounding_config(config)
    if (
        bool(comp_cfg.get("enabled"))
        and to_float(comp_cfg.get("position_notional_pct_of_equity"), 0.0) > 0
        and compounding_equity > 0
    ):
        dynamic_notional = compounding_equity * (
            to_float(comp_cfg.get("position_notional_pct_of_equity"), 0.0) / 100.0
        )
        min_notional = to_float(comp_cfg.get("min_position_notional_usdt"), 0.0)
        max_notional = to_float(comp_cfg.get("max_position_notional_usdt"), 0.0)
        if min_notional > 0:
            dynamic_notional = max(dynamic_notional, min_notional)
        if max_notional > 0:
            dynamic_notional = min(dynamic_notional, max_notional)
        max_position_notional = dynamic_notional

    return {
        "equity": base_equity,
        "effective_equity_usdt": effective_equity,
        "compounding_equity_usdt": compounding_equity,
        "risk_per_trade_usdt": compounding_equity * (risk_per_trade_pct / 100.0),
        # Keep daily circuit-breaker anchored to configured base equity.
        "daily_loss_limit_usdt": base_equity * (max_daily_loss_pct / 100.0),
        "max_position_notional_usdt": max_position_notional,
    }


def is_cooldown_active(risk_state: Dict[str, Any]) -> bool:
    return to_float(risk_state.get("cooldown_until_ts"), 0.0) > time.time()


def cooldown_seconds_remaining(risk_state: Dict[str, Any]) -> int:
    cooldown_until = to_float(risk_state.get("cooldown_until_ts"), 0.0)
    return max(0, int(cooldown_until - time.time()))


def ensure_risk_state(state: Dict[str, Any]) -> Dict[str, Any]:
    risk_state = state.setdefault(
        "risk_state",
        {
            "day": utc_day_str(),
            "daily_realized_pnl_usdt": 0.0,
            "consecutive_losses": 0,
            "paused": False,
            "pause_reason": "",
            "paused_at": "",
            "cooldown_until_ts": 0.0,
            "cooldown_reason": "",
        },
    )
    if risk_state.get("day") != utc_day_str():
        risk_state["day"] = utc_day_str()
        risk_state["daily_realized_pnl_usdt"] = 0.0
        risk_state["consecutive_losses"] = 0
        risk_state["paused"] = False
        risk_state["pause_reason"] = ""
        risk_state["paused_at"] = ""
        risk_state["cooldown_until_ts"] = 0.0
        risk_state["cooldown_reason"] = ""
    return risk_state


def update_circuit_breaker_status(
    config: Dict[str, Any],
    state: Dict[str, Any],
    live_equity_override_usdt: Optional[float] = None,
) -> None:
    risk_cfg = config.get("risk", {})
    risk_state = ensure_risk_state(state)
    pause_on_limit = bool(risk_cfg.get("pause_on_limit", True))
    max_consecutive_losses = int(risk_cfg.get("max_consecutive_losses", 0))
    limits = get_risk_limits(
        config,
        state=state,
        live_equity_override_usdt=live_equity_override_usdt,
    )
    daily_limit = limits["daily_loss_limit_usdt"]

    trigger_reason = ""
    if daily_limit > 0 and risk_state["daily_realized_pnl_usdt"] <= -daily_limit:
        trigger_reason = (
            f"Daily loss limit hit ({risk_state['daily_realized_pnl_usdt']:.2f} "
            f"<= -{daily_limit:.2f})"
        )
    elif max_consecutive_losses > 0 and risk_state["consecutive_losses"] >= max_consecutive_losses:
        trigger_reason = (
            f"Max consecutive losses hit ({risk_state['consecutive_losses']}/"
            f"{max_consecutive_losses})"
        )

    if pause_on_limit and trigger_reason:
        risk_state["paused"] = True
        risk_state["pause_reason"] = trigger_reason
        if not risk_state.get("paused_at"):
            risk_state["paused_at"] = now_utc_str()


def build_bybit_order_plan(
    symbol: str,
    category: str,
    entry_price: float,
    qty: float,
    tp_price: float,
    sl_price: float,
    order_group_id: str = "",
) -> Optional[Dict[str, Any]]:
    if category not in {"linear", "inverse"}:
        return None
    qty_text = f"{qty:.6f}"
    entry_order: Dict[str, Any] = {
        "category": category,
        "symbol": symbol,
        "side": "Buy",
        "orderType": "Limit",
        "price": f"{entry_price:.10f}",
        "qty": qty_text,
        "timeInForce": "GTC",
    }
    take_profit_order: Dict[str, Any] = {
        "category": category,
        "symbol": symbol,
        "side": "Sell",
        "orderType": "Limit",
        "price": f"{tp_price:.10f}",
        "qty": qty_text,
        "timeInForce": "GTC",
        "reduceOnly": True,
    }
    stop_loss_order: Dict[str, Any] = {
        "category": category,
        "symbol": symbol,
        "side": "Sell",
        "orderType": "Market",
        "triggerPrice": f"{sl_price:.10f}",
        "qty": qty_text,
        "reduceOnly": True,
        "closeOnTrigger": True,
        "triggerDirection": 2,
    }
    if order_group_id:
        entry_order["orderLinkId"] = build_order_link_id(order_group_id, "en")
        take_profit_order["orderLinkId"] = build_order_link_id(order_group_id, "tp")
        stop_loss_order["orderLinkId"] = build_order_link_id(order_group_id, "sl")
    return {
        "order_group_id": order_group_id or "",
        "entry_order": {
            **entry_order,
        },
        "take_profit_order": {
            **take_profit_order,
        },
        "stop_loss_order": {
            **stop_loss_order,
        },
    }


def build_json_compact(payload: Dict[str, Any]) -> str:
    return client_build_json_compact(payload)


def bybit_signed_post(
    base_url: str,
    path: str,
    payload: Dict[str, Any],
    api_key: str,
    api_secret: str,
    recv_window: int = 5000,
) -> Dict[str, Any]:
    return client_bybit_signed_post(
        base_url=base_url,
        path=path,
        payload=payload,
        api_key=api_key,
        api_secret=api_secret,
        recv_window=recv_window,
    )


def bybit_signed_post_with_fallback(
    base_urls: List[str],
    path: str,
    payload: Dict[str, Any],
    api_key: str,
    api_secret: str,
    recv_window: int = 5000,
) -> Dict[str, Any]:
    return client_bybit_signed_post_with_fallback(
        base_urls=base_urls,
        path=path,
        payload=payload,
        api_key=api_key,
        api_secret=api_secret,
        recv_window=recv_window,
    )


def bybit_signed_get(
    base_url: str,
    path: str,
    params: Dict[str, Any],
    api_key: str,
    api_secret: str,
    recv_window: int = 5000,
) -> Dict[str, Any]:
    return client_bybit_signed_get(
        base_url=base_url,
        path=path,
        params=params,
        api_key=api_key,
        api_secret=api_secret,
        recv_window=recv_window,
    )


def bybit_signed_get_with_fallback(
    base_urls: List[str],
    path: str,
    params: Dict[str, Any],
    api_key: str,
    api_secret: str,
    recv_window: int = 5000,
) -> Dict[str, Any]:
    return client_bybit_signed_get_with_fallback(
        base_urls=base_urls,
        path=path,
        params=params,
        api_key=api_key,
        api_secret=api_secret,
        recv_window=recv_window,
    )


def is_bybit_duplicate_order_response(response: Dict[str, Any]) -> bool:
    return client_is_bybit_duplicate_order_response(response)


def extract_bybit_order_id(response: Dict[str, Any]) -> str:
    return client_extract_bybit_order_id(response)


def cancel_bybit_order(
    base_urls: List[str],
    api_key: str,
    api_secret: str,
    recv_window: int,
    category: str,
    symbol: str,
    order_id: str = "",
    order_link_id: str = "",
) -> Dict[str, Any]:
    return client_cancel_bybit_order(
        base_urls=base_urls,
        api_key=api_key,
        api_secret=api_secret,
        recv_window=recv_window,
        category=category,
        symbol=symbol,
        order_id=order_id,
        order_link_id=order_link_id,
    )


def is_active_open_order_status(status: str) -> bool:
    return client_is_active_open_order_status(status)


def fetch_bybit_open_orders_for_symbol(
    base_urls: List[str],
    api_key: str,
    api_secret: str,
    recv_window: int,
    category: str,
    symbol: str,
) -> List[Dict[str, Any]]:
    return client_fetch_bybit_open_orders_for_symbol(
        base_urls=base_urls,
        api_key=api_key,
        api_secret=api_secret,
        recv_window=recv_window,
        category=category,
        symbol=symbol,
    )


def fetch_bybit_live_position_for_symbol(
    base_urls: List[str],
    api_key: str,
    api_secret: str,
    recv_window: int,
    category: str,
    symbol: str,
) -> Optional[Dict[str, Any]]:
    return client_fetch_bybit_live_position_for_symbol(
        base_urls=base_urls,
        api_key=api_key,
        api_secret=api_secret,
        recv_window=recv_window,
        category=category,
        symbol=symbol,
    )


def fetch_bybit_live_equity_usdt(
    base_urls: List[str],
    api_key: str,
    api_secret: str,
    recv_window: int,
) -> float:
    """
    Best-effort fetch for live account equity in USDT terms.
    Tries common Bybit account types and multiple fields for compatibility.
    """
    account_types = ["UNIFIED", "CONTRACT", "SPOT"]
    errors: List[str] = []
    for account_type in account_types:
        try:
            response = bybit_signed_get_with_fallback(
                base_urls=base_urls,
                path="/v5/account/wallet-balance",
                params={"accountType": account_type},
                api_key=api_key,
                api_secret=api_secret,
                recv_window=recv_window,
            )
            if response.get("retCode") != 0:
                errors.append(
                    f"{account_type}: retCode={response.get('retCode')} "
                    f"retMsg={response.get('retMsg')}"
                )
                continue

            rows = response.get("result", {}).get("list", [])
            if not rows:
                errors.append(f"{account_type}: empty account rows")
                continue
            row = rows[0]

            # Preferred aggregate fields for UTA/derivatives.
            for key in ("totalEquity", "totalWalletBalance", "totalAvailableBalance"):
                value = to_float(row.get(key), 0.0)
                if value > 0:
                    return value

            # Fallback: coin-level balances.
            coins = row.get("coin", [])
            if isinstance(coins, list):
                for coin in coins:
                    symbol = str(coin.get("coin", "")).upper()
                    if symbol not in {"USDT", "USDC"}:
                        continue
                    for key in ("equity", "walletBalance", "availableToWithdraw"):
                        value = to_float(coin.get(key), 0.0)
                        if value > 0:
                            return value
            errors.append(f"{account_type}: no positive equity field found")
        except Exception as e:
            errors.append(f"{account_type}: {e}")

    raise RuntimeError("Unable to fetch live equity from Bybit: " + " | ".join(errors))


def is_testnet_url(base_url: str) -> bool:
    return "testnet" in str(base_url).lower()


def get_execution_config(config: Dict[str, Any]) -> Dict[str, Any]:
    ensure_runtime_env()
    exec_cfg = config.get("execution", {})
    bybit_cfg = exec_cfg.get("bybit", {})
    live_safety_cfg = exec_cfg.get("live_safety", {})
    env_mode = os.getenv("TRADING_BOT_EXECUTION_MODE", "")
    mode = str(env_mode or exec_cfg.get("mode", "paper")).lower()
    return {
        "mode": mode,
        "assume_filled_on_submit": bool(exec_cfg.get("assume_filled_on_submit", False)),
        "recv_window": int(exec_cfg.get("recv_window_ms", 5000)),
        "api_key": str(bybit_cfg.get("api_key", "") or os.getenv("BYBIT_API_KEY", "")),
        "api_secret": str(bybit_cfg.get("api_secret", "") or os.getenv("BYBIT_API_SECRET", "")),
        "live_safety": {
            "require_manual_unlock": bool(live_safety_cfg.get("require_manual_unlock", True)),
            "required_ack_phrase": str(
                live_safety_cfg.get("required_ack_phrase", LIVE_ACK_DEFAULT)
            ).strip()
            or LIVE_ACK_DEFAULT,
            "require_mainnet_flag": bool(live_safety_cfg.get("require_mainnet_flag", True)),
            "allow_live_trading": parse_env_bool(os.getenv("TRADING_BOT_ALLOW_LIVE"), False),
            "allow_mainnet": parse_env_bool(os.getenv("TRADING_BOT_ALLOW_MAINNET"), False),
            "ack_phrase": str(os.getenv("TRADING_BOT_LIVE_ACK", "")).strip(),
        },
    }


def evaluate_live_execution_guard(
    config: Dict[str, Any],
    exchange_cfg: Dict[str, Any],
) -> Dict[str, Any]:
    exec_ctx = get_execution_config(config)
    mode = exec_ctx["mode"]
    base_url = str(exchange_cfg.get("base_url", ""))
    safety = exec_ctx.get("live_safety", {})
    issues: List[str] = []

    if mode != "live":
        return {
            "mode": mode,
            "allowed": True,
            "issues": [],
            "is_testnet": is_testnet_url(base_url),
        }

    if safety.get("require_manual_unlock") and not safety.get("allow_live_trading"):
        issues.append("Set TRADING_BOT_ALLOW_LIVE=true to unlock live order submission.")
    required_ack = str(safety.get("required_ack_phrase", LIVE_ACK_DEFAULT))
    if required_ack and str(safety.get("ack_phrase", "")) != required_ack:
        issues.append(f"Set TRADING_BOT_LIVE_ACK exactly to '{required_ack}'.")
    if safety.get("require_mainnet_flag") and not is_testnet_url(base_url):
        if not safety.get("allow_mainnet"):
            issues.append(
                "Mainnet URL detected. Set TRADING_BOT_ALLOW_MAINNET=true after paper/testing."
            )

    return {
        "mode": mode,
        "allowed": len(issues) == 0,
        "issues": issues,
        "is_testnet": is_testnet_url(base_url),
    }


def execute_bybit_order_plan(
    config: Dict[str, Any],
    exchange_cfg: Dict[str, Any],
    order_plan: Dict[str, Any],
) -> Dict[str, Any]:
    exec_ctx = get_execution_config(config)
    mode = exec_ctx["mode"]
    if mode != "live":
        return {
            "mode": mode,
            "submitted": False,
            "success": True,
            "message": "Paper mode: order not submitted.",
            "responses": {},
        }

    guard = evaluate_live_execution_guard(config=config, exchange_cfg=exchange_cfg)
    if not guard.get("allowed"):
        return {
            "mode": mode,
            "submitted": False,
            "success": False,
            "message": "Live safety lock active: " + " ".join(guard.get("issues", [])),
            "responses": {},
        }

    api_key = exec_ctx["api_key"]
    api_secret = exec_ctx["api_secret"]
    if not api_key or not api_secret:
        return {
            "mode": mode,
            "submitted": False,
            "success": False,
            "message": "Missing Bybit API credentials (execution.bybit or env vars).",
            "responses": {},
        }

    recv_window = int(exec_ctx["recv_window"])
    base_urls = get_bybit_base_urls(exchange_cfg)
    responses: Dict[str, Any] = {}
    submitted_refs: List[Dict[str, str]] = []
    duplicate_ok_count = 0

    def _rollback_submitted_orders() -> Dict[str, Any]:
        rollback_results: List[str] = []
        for ref in reversed(submitted_refs):
            order_id = str(ref.get("order_id", "")).strip()
            order_link_id = str(ref.get("order_link_id", "")).strip()
            try:
                cancel_res = cancel_bybit_order(
                    base_urls=base_urls,
                    api_key=api_key,
                    api_secret=api_secret,
                    recv_window=recv_window,
                    category=ref["category"],
                    symbol=ref["symbol"],
                    order_id=order_id,
                    order_link_id=order_link_id,
                )
                cancel_code = int(to_float(cancel_res.get("retCode"), 1))
                if cancel_code == 0:
                    rollback_results.append(f"{ref['order_key']}:cancelled")
                else:
                    rollback_results.append(
                        f"{ref['order_key']}:cancel_failed({cancel_res.get('retCode')} {cancel_res.get('retMsg')})"
                    )
            except Exception as e:
                rollback_results.append(f"{ref['order_key']}:cancel_error({e})")
        return {"results": rollback_results}

    submit_order = ["entry_order", "take_profit_order", "stop_loss_order"]
    for order_key in submit_order:
        payload = order_plan.get(order_key)
        if not payload:
            continue
        res = bybit_signed_post_with_fallback(
            base_urls=base_urls,
            path="/v5/order/create",
            payload=payload,
            api_key=api_key,
            api_secret=api_secret,
            recv_window=recv_window,
        )
        responses[order_key] = res
        ret_code = int(to_float(res.get("retCode"), 1))
        if ret_code == 0:
            submitted_refs.append(
                {
                    "order_key": order_key,
                    "category": str(payload.get("category", "")),
                    "symbol": str(payload.get("symbol", "")),
                    "order_id": extract_bybit_order_id(res),
                    "order_link_id": str(payload.get("orderLinkId", "")),
                }
            )
            continue
        if is_bybit_duplicate_order_response(res):
            duplicate_ok_count += 1
            responses[f"{order_key}_dedupe"] = {
                "retCode": ret_code,
                "retMsg": str(res.get("retMsg", "")),
                "note": "Duplicate orderLinkId treated as already-submitted.",
            }
            continue
        rollback_info = _rollback_submitted_orders()
        responses["rollback"] = rollback_info
        rollback_text = ", ".join(rollback_info.get("results", [])) or "none"
        submitted_any = bool(submitted_refs)
        return {
            "mode": mode,
            "submitted": submitted_any,
            "success": False,
            "message": (
                f"{order_key} failed: retCode={res.get('retCode')} retMsg={res.get('retMsg')} "
                f"| rollback={rollback_text}"
            ),
            "responses": responses,
        }

    if not submitted_refs and duplicate_ok_count == 0 and mode == "live":
        return {
            "mode": mode,
            "submitted": False,
            "success": False,
            "message": "No orders accepted by exchange.",
            "responses": responses,
        }

    return {
        "mode": mode,
        "submitted": bool(submitted_refs) or duplicate_ok_count > 0,
        "success": True,
        "message": "Bybit order plan submitted.",
        "responses": responses,
    }


def ema(values: List[float], period: int) -> List[float]:
    if period <= 0:
        raise ValueError("EMA period must be > 0")
    if len(values) < period:
        return []
    k = 2 / (period + 1)
    seed = sum(values[:period]) / period
    output = [seed]
    for value in values[period:]:
        output.append((value - output[-1]) * k + output[-1])
    return output


def rsi(values: List[float], period: int = 14) -> List[float]:
    if period <= 0:
        raise ValueError("RSI period must be > 0")
    if len(values) <= period:
        return []

    gains: List[float] = []
    losses: List[float] = []
    for i in range(1, len(values)):
        delta = values[i] - values[i - 1]
        gains.append(max(delta, 0.0))
        losses.append(max(-delta, 0.0))

    avg_gain = sum(gains[:period]) / period
    avg_loss = sum(losses[:period]) / period
    rsis: List[float] = []

    def _calc(avg_g: float, avg_l: float) -> float:
        if avg_l == 0:
            return 100.0
        rs = avg_g / avg_l
        return 100 - (100 / (1 + rs))

    rsis.append(_calc(avg_gain, avg_loss))
    for i in range(period, len(gains)):
        avg_gain = ((avg_gain * (period - 1)) + gains[i]) / period
        avg_loss = ((avg_loss * (period - 1)) + losses[i]) / period
        rsis.append(_calc(avg_gain, avg_loss))
    return rsis


def normalize_interval_for_bybit(interval: str) -> str:
    raw = str(interval).strip()
    if raw in {"1", "3", "5", "15", "30", "60", "120", "240", "360", "720", "D", "W", "M"}:
        return raw
    return BYBIT_INTERVAL_MAP.get(raw, raw)


def normalize_interval_for_binance(interval: str) -> str:
    raw = str(interval).strip()
    return BINANCE_INTERVAL_MAP.get(raw, raw)


def interval_to_milliseconds(interval: str) -> int:
    bybit_style = normalize_interval_for_bybit(interval)
    if bybit_style in {"1", "3", "5", "15", "30", "60", "120", "240", "360", "720"}:
        return int(bybit_style) * 60_000
    if bybit_style == "D":
        return 24 * 60 * 60 * 1000
    if bybit_style == "W":
        return 7 * 24 * 60 * 60 * 1000
    if bybit_style == "M":
        return 30 * 24 * 60 * 60 * 1000
    return 0


def fetch_klines_binance(
    symbol: str,
    interval: str,
    limit: int,
    base_url: str,
) -> List[Dict[str, Any]]:
    normalized_interval = normalize_interval_for_binance(interval)
    params = urllib.parse.urlencode(
        {
            "symbol": symbol,
            "interval": normalized_interval,
            "limit": limit,
        }
    )
    url = f"{base_url.rstrip('/')}/api/v3/klines?{params}"
    req = urllib.request.Request(url, headers={"User-Agent": "crypto-alert-bot/1.0"})
    with urllib.request.urlopen(req, timeout=20) as res:
        payload = json.loads(res.read().decode("utf-8"))

    candles: List[Dict[str, Any]] = []
    for row in payload:
        candles.append(
            {
                "open_time": int(row[0]),
                "open": float(row[1]),
                "high": float(row[2]),
                "low": float(row[3]),
                "close": float(row[4]),
                "volume": float(row[5]),
                "close_time": int(row[6]),
            }
        )
    return candles


def fetch_klines_bybit(
    symbol: str,
    interval: str,
    limit: int,
    base_urls: List[str],
    category: str = "linear",
) -> List[Dict[str, Any]]:
    normalized_interval = normalize_interval_for_bybit(interval)
    params = urllib.parse.urlencode(
        {
            "category": category,
            "symbol": symbol,
            "interval": normalized_interval,
            "limit": limit,
        }
    )
    payload: Optional[Dict[str, Any]] = None
    errors: List[str] = []
    for base_url in dedupe_urls(base_urls):
        url = f"{base_url.rstrip('/')}/v5/market/kline?{params}"
        req = urllib.request.Request(url, headers={"User-Agent": "crypto-alert-bot/1.0"})
        try:
            with urllib.request.urlopen(req, timeout=20) as res:
                payload = json.loads(res.read().decode("utf-8"))
            break
        except Exception as e:
            errors.append(f"{base_url}: {e}")
    if payload is None:
        raise RuntimeError(
            f"All Bybit endpoints failed for kline ({symbol}): " + " | ".join(errors)
        )

    if payload.get("retCode") != 0:
        raise ValueError(
            f"Bybit API error for {symbol}: retCode={payload.get('retCode')} "
            f"retMsg={payload.get('retMsg')}"
        )

    rows = payload.get("result", {}).get("list", [])
    interval_ms = interval_to_milliseconds(interval)
    candles: List[Dict[str, Any]] = []
    for row in rows:
        open_time = int(row[0])
        close_time = open_time + interval_ms - 1 if interval_ms > 0 else open_time
        candles.append(
            {
                "open_time": open_time,
                "open": float(row[1]),
                "high": float(row[2]),
                "low": float(row[3]),
                "close": float(row[4]),
                "volume": float(row[5]),
                "close_time": close_time,
            }
        )

    # Bybit returns newest-first; indicators expect oldest-first.
    candles.sort(key=lambda x: x["open_time"])
    return candles


def fetch_klines(
    exchange_cfg: Dict[str, Any],
    symbol: str,
    interval: str,
    limit: int,
    category_override: Optional[str] = None,
) -> List[Dict[str, Any]]:
    exchange_name = str(exchange_cfg.get("name", "binance")).lower()
    base_url = exchange_cfg.get("base_url", "")
    if exchange_name == "binance":
        return fetch_klines_binance(
            symbol=symbol,
            interval=interval,
            limit=limit,
            base_url=base_url,
        )
    if exchange_name == "bybit":
        category = category_override or str(exchange_cfg.get("category", "linear"))
        return fetch_klines_bybit(
            symbol=symbol,
            interval=interval,
            limit=limit,
            base_urls=get_bybit_base_urls(exchange_cfg),
            category=category,
        )
    raise ValueError(
        f"Unsupported exchange '{exchange_name}'. "
        f"Supported: {', '.join(sorted(SUPPORTED_EXCHANGES))}"
    )


def fetch_bybit_tickers(base_urls: List[str], category: str = "spot") -> List[Dict[str, Any]]:
    return client_fetch_bybit_tickers(base_urls=base_urls, category=category)


def build_symbol_ticker_map(rows: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    symbol_map: Dict[str, Dict[str, Any]] = {}
    for row in rows:
        symbol = str(row.get("symbol", "")).upper()
        if symbol:
            symbol_map[symbol] = row
    return symbol_map


def evaluate_entry_liquidity(
    symbol: str,
    ticker_row: Optional[Dict[str, Any]],
    liq_cfg: Dict[str, Any],
) -> Dict[str, Any]:
    if not liq_cfg.get("enabled", False):
        return {"allowed": True, "reason": "", "spread_pct": None, "turnover_24h_usdt": None}

    if not ticker_row:
        if liq_cfg.get("block_when_ticker_missing", True):
            return {
                "allowed": False,
                "reason": f"Liquidity check blocked {symbol}: missing ticker data",
                "spread_pct": None,
                "turnover_24h_usdt": None,
            }
        return {"allowed": True, "reason": "", "spread_pct": None, "turnover_24h_usdt": None}

    bid = to_float(ticker_row.get("bid1Price"), 0.0)
    ask = to_float(ticker_row.get("ask1Price"), 0.0)
    turnover = to_float(ticker_row.get("turnover24h"), 0.0)
    spread_pct: Optional[float] = None
    if bid > 0 and ask > 0 and ask >= bid:
        mid = (bid + ask) / 2.0
        if mid > 0:
            spread_pct = ((ask - bid) / mid) * 100.0

    reasons: List[str] = []
    min_turnover = to_float(liq_cfg.get("min_turnover_24h_usdt"), 0.0)
    max_spread = to_float(liq_cfg.get("max_spread_pct"), 0.0)
    if min_turnover > 0 and turnover < min_turnover:
        reasons.append(
            f"turnover {format_turnover(turnover)} < {format_turnover(min_turnover)}"
        )
    if max_spread > 0:
        if spread_pct is None:
            reasons.append("spread unavailable")
        elif spread_pct > max_spread:
            reasons.append(f"spread {spread_pct:.3f}% > {max_spread:.3f}%")

    return {
        "allowed": len(reasons) == 0,
        "reason": "; ".join(reasons),
        "spread_pct": spread_pct,
        "turnover_24h_usdt": turnover if turnover > 0 else None,
    }


def pick_best_bybit_spot_symbols(
    config: Dict[str, Any],
    existing_symbols: List[str],
) -> List[str]:
    exchange_cfg = config["exchange"]
    exchange_name = str(exchange_cfg.get("name", "binance")).lower()
    if exchange_name != "bybit":
        return []

    discovery_cfg = config.get("spot_discovery", {})
    if not discovery_cfg.get("enabled", False):
        return []

    add_count = int(discovery_cfg.get("add_count", 1))
    if add_count <= 0:
        return []

    min_turnover = float(discovery_cfg.get("min_turnover_usdt", 20_000_000))
    min_price_change = float(discovery_cfg.get("min_price_change_pct", 0.05))
    fallback_to_best = bool(discovery_cfg.get("fallback_to_best", True))
    price_filter_cfg = get_price_filter_config(config)
    max_price_usdt = to_float(price_filter_cfg.get("max_price_usdt"), 0.0)
    apply_price_filter_to_spot = bool(price_filter_cfg.get("apply_to_spot_discovery", True))
    excluded_symbols = {
        str(sym).upper()
        for sym in discovery_cfg.get("exclude_symbols", [])
    }
    excluded_symbols.update(DEFAULT_EXCLUDED_STABLE_SYMBOLS)
    existing_set = {str(sym).upper() for sym in existing_symbols}

    tickers = fetch_bybit_tickers(
        base_urls=get_bybit_base_urls(exchange_cfg),
        category="spot",
    )
    candidates: List[Dict[str, Any]] = []
    relaxed_candidates: List[Dict[str, Any]] = []
    for row in tickers:
        symbol = str(row.get("symbol", "")).upper()
        if not symbol.endswith("USDT"):
            continue
        if symbol in existing_set or symbol in excluded_symbols:
            continue

        pct = to_float(row.get("price24hPcnt"), 0.0)
        turnover = to_float(row.get("turnover24h"), 0.0)
        last_price = to_float(row.get("lastPrice"), 0.0)
        if (
            bool(price_filter_cfg.get("enabled"))
            and apply_price_filter_to_spot
            and max_price_usdt > 0
            and last_price > max_price_usdt
        ):
            continue
        if turnover < min_turnover:
            continue

        candidate = {
            "symbol": symbol,
            "pct": pct,
            "turnover": turnover,
        }
        if pct >= min_price_change:
            candidates.append(candidate)
        relaxed_candidates.append(candidate)

    candidates.sort(key=lambda x: (x["pct"], x["turnover"]), reverse=True)
    if candidates:
        return [row["symbol"] for row in candidates[:add_count]]

    if fallback_to_best:
        relaxed_candidates.sort(key=lambda x: (x["pct"], x["turnover"]), reverse=True)
        return [row["symbol"] for row in relaxed_candidates[:add_count]]

    return []


def send_telegram_message(bot_token: str, chat_id: str, text: str) -> None:
    url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
    data = urllib.parse.urlencode({"chat_id": chat_id, "text": text}).encode("utf-8")
    req = urllib.request.Request(url, data=data, method="POST")
    with urllib.request.urlopen(req, timeout=20):
        return


def get_notifications_config(config: Dict[str, Any]) -> Dict[str, Any]:
    ensure_runtime_env()
    notify_cfg = dict(config.get("notifications", {}))
    notify_cfg["bot_token"] = str(
        notify_cfg.get("bot_token", "") or os.getenv("TELEGRAM_BOT_TOKEN", "")
    )
    notify_cfg["chat_id"] = str(
        notify_cfg.get("chat_id", "") or os.getenv("TELEGRAM_CHAT_ID", "")
    )
    return notify_cfg


def analyze_symbol(
    symbol: str,
    candles: List[Dict[str, Any]],
    strategy_cfg: Dict[str, Any],
    position: Optional[Dict[str, Any]],
    costs_cfg: Dict[str, float],
) -> Dict[str, Any]:
    closes = [c["close"] for c in candles]
    if len(closes) < max(strategy_cfg["ema_slow"] + 2, strategy_cfg["rsi_period"] + 2):
        raise ValueError(f"Not enough candle data for {symbol}")

    ema_fast_series = ema(closes, strategy_cfg["ema_fast"])
    ema_slow_series = ema(closes, strategy_cfg["ema_slow"])
    rsi_series = rsi(closes, strategy_cfg["rsi_period"])

    if len(ema_fast_series) < 2 or len(ema_slow_series) < 2 or len(rsi_series) < 2:
        raise ValueError(f"Indicator data too short for {symbol}")

    # Align end-of-series values (latest candle).
    ema_fast_now = ema_fast_series[-1]
    ema_fast_prev = ema_fast_series[-2]
    ema_slow_now = ema_slow_series[-1]
    ema_slow_prev = ema_slow_series[-2]
    rsi_now = rsi_series[-1]
    price_now = closes[-1]
    close_time = candles[-1]["close_time"]

    cross_up = ema_fast_prev <= ema_slow_prev and ema_fast_now > ema_slow_now
    cross_down = ema_fast_prev >= ema_slow_prev and ema_fast_now < ema_slow_now

    trend_pct = ((ema_fast_now - ema_slow_now) / price_now) * 100 if price_now else 0.0
    score = (trend_pct * 2.0) + (1.0 - min(abs(rsi_now - 60.0), 40.0) / 40.0) * 2.0

    result: Dict[str, Any] = {
        "symbol": symbol,
        "source": "WATCHLIST",
        "price": price_now,
        "ema_fast": ema_fast_now,
        "ema_slow": ema_slow_now,
        "rsi": rsi_now,
        "score": score,
        "close_time": close_time,
        "signal": None,
        "action": "WAIT",
        "wait_price": None,
        "tp_price": None,
        "sl_price": None,
        "qty": None,
        "risk_budget_usdt": None,
        "risk_usdt": None,
        "order_plan": None,
        "execution": None,
        "spread_pct": None,
        "turnover_24h_usdt": None,
        "net_return_pct": None,
        "note": "",
        "message": "",
    }

    tp_pct = strategy_cfg["take_profit_pct"] / 100.0
    sl_pct = strategy_cfg["stop_loss_pct"] / 100.0

    if position:
        entry = float(position["entry"])
        exits = build_exit_prices(entry, tp_pct, sl_pct)
        tp_price = exits["tp_price"]
        sl_price = exits["sl_price"]
        net_return_pct = compute_net_return_pct(
            entry_price=entry,
            exit_price=price_now,
            costs_cfg=costs_cfg,
        )
        result["tp_price"] = tp_price
        result["sl_price"] = sl_price
        result["action"] = "HOLD"
        result["net_return_pct"] = net_return_pct
        result["note"] = (
            f"In position (entry {format_price(entry)}), "
            f"TP {format_price(tp_price)}, SL {format_price(sl_price)}"
        )

        if net_return_pct >= strategy_cfg["take_profit_pct"]:
            result["signal"] = "TAKE_PROFIT"
            result["action"] = "SELL"
            result["message"] = (
                f"TAKE PROFIT NOW | {symbol} at {format_price(price_now)} "
                f"(net {net_return_pct:.2f}%)"
            )
        elif net_return_pct <= -strategy_cfg["stop_loss_pct"]:
            result["signal"] = "STOP_LOSS"
            result["action"] = "SELL"
            result["message"] = (
                f"STOP LOSS NOW | {symbol} at {format_price(price_now)} "
                f"(net {net_return_pct:.2f}%)"
            )
        elif cross_down or rsi_now >= strategy_cfg["rsi_sell"]:
            result["signal"] = "SELL_NOW"
            result["action"] = "SELL"
            result["message"] = (
                f"SELL NOW | {symbol} at {format_price(price_now)} "
                f"(RSI {rsi_now:.1f}, net {net_return_pct:.2f}%)"
            )
        return result

    buy_min = strategy_cfg["rsi_buy_min"]
    buy_max = strategy_cfg["rsi_buy_max"]
    pullback = strategy_cfg["limit_pullback_pct"] / 100.0
    limit_price = min(price_now, ema_fast_now) * (1 - pullback)
    uptrend = ema_fast_now > ema_slow_now

    if cross_up and buy_min <= rsi_now <= buy_max and price_now >= ema_fast_now:
        exits = build_exit_prices(limit_price, tp_pct, sl_pct)
        result["signal"] = "LIMIT_BUY"
        result["action"] = "BUY_LIMIT"
        result["wait_price"] = limit_price
        result["tp_price"] = exits["tp_price"]
        result["sl_price"] = exits["sl_price"]
        result["note"] = "Trend cross confirmed"
        result["message"] = (
            f"LIMIT BUY NOW | {symbol} market {format_price(price_now)} "
            f"| suggested limit {format_price(limit_price)} "
            f"| LIMIT SELL {format_price(exits['tp_price'])} "
            f"| STOP {format_price(exits['sl_price'])}"
        )
        return result

    if uptrend and rsi_now > buy_max:
        result["action"] = "WAIT_PULLBACK"
        result["wait_price"] = limit_price
        exits = build_exit_prices(limit_price, tp_pct, sl_pct)
        result["tp_price"] = exits["tp_price"]
        result["sl_price"] = exits["sl_price"]
        result["note"] = f"RSI high ({rsi_now:.1f})"
    elif uptrend and rsi_now < buy_min:
        result["action"] = "WAIT_CONFIRM"
        result["wait_price"] = ema_fast_now
        exits = build_exit_prices(ema_fast_now, tp_pct, sl_pct)
        result["tp_price"] = exits["tp_price"]
        result["sl_price"] = exits["sl_price"]
        result["note"] = f"Need stronger momentum (RSI {rsi_now:.1f})"
    elif uptrend:
        result["action"] = "WAIT_LIMIT"
        result["wait_price"] = limit_price
        exits = build_exit_prices(limit_price, tp_pct, sl_pct)
        result["tp_price"] = exits["tp_price"]
        result["sl_price"] = exits["sl_price"]
        result["note"] = "Uptrend but no fresh cross yet"
    else:
        result["action"] = "WAIT"
        result["wait_price"] = limit_price
        exits = build_exit_prices(limit_price, tp_pct, sl_pct)
        result["tp_price"] = exits["tp_price"]
        result["sl_price"] = exits["sl_price"]
        result["note"] = "Trend still down"
    return result


def enrich_result_with_risk_and_orders(
    config: Dict[str, Any],
    exchange_cfg: Dict[str, Any],
    result: Dict[str, Any],
    qty_constraints: Optional[Dict[str, float]] = None,
    state: Optional[Dict[str, Any]] = None,
    live_equity_override_usdt: Optional[float] = None,
) -> None:
    entry = to_float(result.get("wait_price"), 0.0)
    sl = to_float(result.get("sl_price"), 0.0)
    tp = to_float(result.get("tp_price"), 0.0)
    if entry <= 0 or sl <= 0 or tp <= 0:
        return

    tick_size = to_float((qty_constraints or {}).get("tick_size"), 0.0)
    if tick_size > 0:
        entry = round_price_to_tick(entry, tick_size)
        sl = round_price_to_tick(sl, tick_size)
        tp = round_price_to_tick(tp, tick_size)
        result["wait_price"] = entry if entry > 0 else None
        result["sl_price"] = sl if sl > 0 else None
        result["tp_price"] = tp if tp > 0 else None
        if entry <= 0 or sl <= 0 or tp <= 0:
            result["note"] = f"{result.get('note', '')} | Invalid rounded price (tick size).".strip(" |")
            result["qty"] = None
            result["order_plan"] = None
            return
        if sl >= entry:
            result["note"] = f"{result.get('note', '')} | Stop-loss rounded too close to entry.".strip(" |")
            result["qty"] = None
            result["order_plan"] = None
            return
        if tp <= entry:
            result["note"] = f"{result.get('note', '')} | Take-profit rounded too close to entry.".strip(" |")
            result["qty"] = None
            result["order_plan"] = None
            return

    limits = get_risk_limits(
        config,
        state=state,
        live_equity_override_usdt=live_equity_override_usdt,
    )
    risk_budget = limits["risk_per_trade_usdt"]
    raw_qty = calculate_position_size(entry, sl, risk_budget)
    max_notional = limits.get("max_position_notional_usdt", 0.0)
    capped_qty = raw_qty
    if max_notional > 0 and entry > 0:
        max_qty_by_notional = max_notional / entry
        capped_qty = min(raw_qty, max_qty_by_notional)

    adjusted_qty = capped_qty
    if qty_constraints:
        qty_step = to_float(qty_constraints.get("qty_step"), 0.0)
        min_qty = to_float(qty_constraints.get("min_qty"), 0.0)
        max_qty = to_float(qty_constraints.get("max_qty"), 0.0)
        adjusted_qty = floor_to_step(adjusted_qty, qty_step)
        if max_qty > 0:
            adjusted_qty = min(adjusted_qty, max_qty)
        if min_qty > 0 and adjusted_qty < min_qty:
            result["risk_budget_usdt"] = risk_budget if risk_budget > 0 else None
            result["risk_usdt"] = None
            result["qty"] = None
            min_notional = (min_qty * entry) if entry > 0 else 0.0
            result["note"] = (
                f"{result.get('note', '')} | Qty below exchange minimum "
                f"({adjusted_qty:.6f} < {min_qty:.6f}). "
                f"Increase max_position_notional_usdt to at least {min_notional:.2f}."
            ).strip(" |")
            result["order_plan"] = None
            return

    realized_risk_usdt = abs(entry - sl) * adjusted_qty if adjusted_qty > 0 else 0.0

    result["risk_budget_usdt"] = risk_budget if risk_budget > 0 else None
    result["risk_usdt"] = realized_risk_usdt if realized_risk_usdt > 0 else None
    result["qty"] = adjusted_qty if adjusted_qty > 0 else None
    if raw_qty > 0 and adjusted_qty < raw_qty:
        result["note"] = f"{result.get('note', '')} | Qty capped by max notional".strip(" |")

    exchange_name = str(exchange_cfg.get("name", "bybit")).lower()
    if exchange_name != "bybit":
        # Order-plan execution is currently implemented only for Bybit.
        result["order_plan"] = None
        return

    if adjusted_qty > 0:
        category = "spot" if result.get("source") == "SPOT_BEST" else str(
            exchange_cfg.get("category", "linear")
        )
        order_group_id = build_order_group_id(
            symbol=str(result["symbol"]),
            close_time=result.get("close_time"),
        )
        result["order_plan"] = build_bybit_order_plan(
            symbol=str(result["symbol"]),
            category=category,
            entry_price=entry,
            qty=adjusted_qty,
            tp_price=tp,
            sl_price=sl,
            order_group_id=order_group_id,
        )


def print_market_snapshot(results: List[Dict[str, Any]]) -> None:
    sorted_rows = sorted(results, key=lambda x: x["score"], reverse=True)
    print(f"\n[{now_utc_str()}] Market scan")
    print(
        f"{'RK':>2} {'SYMBOL':<10} {'PRICE':>12} {'RSI':>6} "
        f"{'TREND':<6} {'SCORE':>7} {'SRC':<10} {'ACTION':<13} {'ENTRY':>12} {'TP':>12} {'SL':>12} {'QTY':>12}"
    )
    print("-" * 140)
    for idx, row in enumerate(sorted_rows, start=1):
        trend_delta = row["ema_fast"] - row["ema_slow"]
        trend_tag = "UP" if trend_delta > 0 else "DOWN"
        source = row.get("source", "WATCHLIST")
        wait_at = format_price(row["wait_price"]) if row.get("wait_price") else "-"
        tp_at = format_price(row["tp_price"]) if row.get("tp_price") else "-"
        sl_at = format_price(row["sl_price"]) if row.get("sl_price") else "-"
        qty_text = f"{to_float(row.get('qty'), 0.0):.4f}" if row.get("qty") else "-"
        print(
            f"{idx:>2} {row['symbol']:<10} {format_price(row['price']):>12} "
            f"{row['rsi']:>6.1f} {trend_tag:<6} {row['score']:>7.2f} "
            f"{source:<10} {row['action']:<13} {wait_at:>12} {tp_at:>12} {sl_at:>12} {qty_text:>12}"
        )

    top_three = ", ".join([row["symbol"] for row in sorted_rows[:3]])
    best = sorted_rows[0]
    print("-" * 140)
    print(f"Top momentum: {top_three}")
    print(
        f"Best now: {best['symbol']} at {format_price(best['price'])} "
        f"(RSI {best['rsi']:.1f}, score {best['score']:.2f})"
    )
    buy_candidates = [row for row in sorted_rows if row["action"] == "BUY_LIMIT"]
    if buy_candidates:
        pick = buy_candidates[0]
        print(
            f"BUY candidate now: {pick['symbol']} | "
            f"entry {format_price(pick['wait_price'])} | "
            f"limit sell {format_price(pick['tp_price'])} | "
            f"stop {format_price(pick['sl_price'])}"
        )
    else:
        wait_candidates = [row for row in sorted_rows if row.get("wait_price")]
        if wait_candidates:
            pick = wait_candidates[0]
            print(
                f"No safe buy now. Wait for {pick['symbol']} near "
                f"{format_price(pick['wait_price'])} ({pick['action']}) | "
                f"if bought, limit sell {format_price(pick['tp_price'])}, "
                f"stop {format_price(pick['sl_price'])}."
            )


def scan_once(config: Dict[str, Any], state: Dict[str, Any]) -> Dict[str, Any]:
    positions: Dict[str, Dict[str, Any]] = state.setdefault("positions", {})
    last_alerts: Dict[str, str] = state.setdefault("last_alerts", {})
    live_open_positions_state: Dict[str, Dict[str, Any]] = state.setdefault("live_open_positions", {})
    live_pending_entries_state: Dict[str, Dict[str, Any]] = state.setdefault("live_pending_entries", {})
    if not isinstance(live_open_positions_state, dict):
        live_open_positions_state = {}
        state["live_open_positions"] = live_open_positions_state
    if not isinstance(live_pending_entries_state, dict):
        live_pending_entries_state = {}
        state["live_pending_entries"] = live_pending_entries_state
    ensure_trade_history_state(state)
    risk_state = ensure_risk_state(state)

    exchange_cfg = config["exchange"]
    exchange_name = str(exchange_cfg.get("name", "binance")).lower()
    interval = config["interval"]
    lookback = int(config["lookback"])
    symbols = [str(s).upper() for s in config["symbols"]]
    strategy_cfg = config["strategy"]
    risk_cfg = config["risk"]
    exec_ctx = get_execution_config(config)
    costs_cfg = get_execution_costs_config(config)
    liq_cfg = get_liquidity_filter_config(config)
    price_filter_cfg = get_price_filter_config(config)
    live_sync_required = (
        exchange_name == "bybit"
        and str(exec_ctx.get("mode", "paper")).lower() == "live"
        and not bool(exec_ctx.get("assume_filled_on_submit"))
    )

    cycle_results: List[Dict[str, Any]] = []
    cycle_alerts: List[str] = []
    cycle_errors: List[str] = []
    auto_added_symbols: List[str] = []
    execution_events: List[Dict[str, Any]] = []
    performance: Dict[str, Any] = {}
    recent_closed_trades: List[Dict[str, Any]] = []
    live_synced_positions: Dict[str, Dict[str, Any]] = {}
    live_synced_pending_entries: Dict[str, Dict[str, Any]] = {}
    live_sync_errors: Dict[str, str] = {}
    live_equity_override_usdt: Optional[float] = None

    symbol_jobs: List[Dict[str, str]] = []
    seen_symbols = set()
    for symbol in symbols:
        if symbol in seen_symbols:
            continue
        seen_symbols.add(symbol)
        symbol_jobs.append({"symbol": symbol, "category": "", "source": "WATCHLIST"})

    try:
        auto_added_symbols = pick_best_bybit_spot_symbols(config=config, existing_symbols=symbols)
        for symbol in auto_added_symbols:
            if symbol in seen_symbols:
                continue
            seen_symbols.add(symbol)
            symbol_jobs.append({"symbol": symbol, "category": "spot", "source": "SPOT_BEST"})
    except Exception as e:
        cycle_errors.append(f"[SPOT_DISCOVERY] Error: {e}")

    ticker_maps: Dict[str, Dict[str, Dict[str, Any]]] = {}
    qty_constraints_map: Dict[str, Dict[str, float]] = {}
    if exchange_name == "bybit" and liq_cfg.get("enabled"):
        categories = {
            (job["category"] or str(exchange_cfg.get("category", "linear")))
            for job in symbol_jobs
        }
        for category in categories:
            try:
                rows = fetch_bybit_tickers(
                    base_urls=get_bybit_base_urls(exchange_cfg),
                    category=category,
                )
                ticker_maps[category] = build_symbol_ticker_map(rows)
            except Exception as e:
                cycle_errors.append(f"[LIQUIDITY:{category}] Error: {e}")

    if exchange_name == "bybit":
        base_urls = get_bybit_base_urls(exchange_cfg)
        for job in symbol_jobs:
            symbol = job["symbol"]
            category = job["category"] or str(exchange_cfg.get("category", "linear"))
            if category not in {"linear", "inverse"}:
                continue
            key = f"{category}:{symbol}"
            if key in qty_constraints_map:
                continue
            try:
                qty_constraints_map[key] = fetch_bybit_instrument_constraints(
                    base_urls=base_urls,
                    category=category,
                    symbol=symbol,
                )
            except Exception as e:
                cycle_errors.append(f"[INSTRUMENT:{symbol}] Error: {e}")

        # Live compounding prefers real wallet equity to reduce drift from ephemeral state.
        if str(exec_ctx.get("mode", "paper")).lower() == "live":
            api_key = str(exec_ctx.get("api_key", ""))
            api_secret = str(exec_ctx.get("api_secret", ""))
            recv_window = int(exec_ctx.get("recv_window", 5000))
            if api_key and api_secret:
                try:
                    live_equity_override_usdt = fetch_bybit_live_equity_usdt(
                        base_urls=base_urls,
                        api_key=api_key,
                        api_secret=api_secret,
                        recv_window=recv_window,
                    )
                except Exception as e:
                    cycle_errors.append(f"[LIVE_EQUITY] Warning: {e}")

    update_circuit_breaker_status(
        config=config,
        state=state,
        live_equity_override_usdt=live_equity_override_usdt,
    )

    if live_sync_required:
        api_key = str(exec_ctx.get("api_key", ""))
        api_secret = str(exec_ctx.get("api_secret", ""))
        recv_window = int(exec_ctx.get("recv_window", 5000))
        base_urls = get_bybit_base_urls(exchange_cfg)
        if not api_key or not api_secret:
            issue = "Missing API credentials for live sync in non-assumed mode."
            cycle_errors.append(f"[LIVE_SYNC] {issue}")
            live_sync_errors["*"] = issue
        else:
            synced_pairs: set[str] = set()
            for job in symbol_jobs:
                symbol = str(job["symbol"]).upper()
                category = job["category"] or str(exchange_cfg.get("category", "linear"))
                if category not in {"linear", "inverse"}:
                    continue
                pair_key = f"{category}:{symbol}"
                if pair_key in synced_pairs:
                    continue
                synced_pairs.add(pair_key)
                try:
                    pos = fetch_bybit_live_position_for_symbol(
                        base_urls=base_urls,
                        api_key=api_key,
                        api_secret=api_secret,
                        recv_window=recv_window,
                        category=category,
                        symbol=symbol,
                    )
                    if pos:
                        live_synced_positions[symbol] = pos
                except Exception as e:
                    live_sync_errors[symbol] = f"position sync failed: {e}"
                    cycle_errors.append(f"[LIVE_SYNC:{symbol}] Position error: {e}")

                try:
                    open_orders = fetch_bybit_open_orders_for_symbol(
                        base_urls=base_urls,
                        api_key=api_key,
                        api_secret=api_secret,
                        recv_window=recv_window,
                        category=category,
                        symbol=symbol,
                    )
                    entry_orders = [
                        row
                        for row in open_orders
                        if str(row.get("side", "")).upper() == "BUY"
                        and not parse_env_bool(row.get("reduceOnly"), False)
                    ]
                    if entry_orders:
                        row = entry_orders[0]
                        live_synced_pending_entries[symbol] = {
                            "order_id": str(row.get("orderId", "")),
                            "order_link_id": str(row.get("orderLinkId", "")),
                            "price": to_float(row.get("price"), 0.0),
                            "qty": abs(to_float(row.get("qty"), 0.0)),
                            "status": str(row.get("orderStatus", "")),
                            "updated_at": now_utc_str(),
                        }
                except Exception as e:
                    if symbol not in live_sync_errors:
                        live_sync_errors[symbol] = f"order sync failed: {e}"
                    cycle_errors.append(f"[LIVE_SYNC:{symbol}] Open-order error: {e}")

        state["live_open_positions"] = live_synced_positions
        state["live_pending_entries"] = live_synced_pending_entries
    else:
        state["live_open_positions"] = {}
        state["live_pending_entries"] = {}

    for job in symbol_jobs:
        symbol = job["symbol"]
        category_override = job["category"] or None
        source = job["source"]
        category_for_symbol = category_override or str(exchange_cfg.get("category", "linear"))
        try:
            candles = fetch_klines(
                exchange_cfg=exchange_cfg,
                symbol=symbol,
                interval=interval,
                limit=lookback,
                category_override=category_override,
            )
            position = live_synced_positions.get(symbol) if live_sync_required else positions.get(symbol)
            result = analyze_symbol(
                symbol=symbol,
                candles=candles,
                strategy_cfg=strategy_cfg,
                position=position,
                costs_cfg=costs_cfg,
            )
            result["source"] = source
            ticker_row = ticker_maps.get(category_for_symbol, {}).get(symbol)
            liq_eval = evaluate_entry_liquidity(
                symbol=symbol,
                ticker_row=ticker_row,
                liq_cfg=liq_cfg if exchange_name == "bybit" else {"enabled": False},
            )
            result["spread_pct"] = liq_eval.get("spread_pct")
            result["turnover_24h_usdt"] = liq_eval.get("turnover_24h_usdt")
            enrich_result_with_risk_and_orders(
                config=config,
                exchange_cfg=exchange_cfg,
                result=result,
                qty_constraints=qty_constraints_map.get(f"{category_for_symbol}:{symbol}"),
                state=state,
                live_equity_override_usdt=live_equity_override_usdt,
            )
            max_price_usdt = to_float(price_filter_cfg.get("max_price_usdt"), 0.0)
            apply_by_source = (
                bool(price_filter_cfg.get("apply_to_spot_discovery", True))
                if source == "SPOT_BEST"
                else bool(price_filter_cfg.get("apply_to_watchlist", True))
            )
            if (
                bool(price_filter_cfg.get("enabled"))
                and apply_by_source
                and max_price_usdt > 0
                and to_float(result.get("price"), 0.0) > max_price_usdt
            ):
                current_price = to_float(result.get("price"), 0.0)
                result["signal"] = None
                result["action"] = "WAIT_PRICE"
                result["message"] = ""
                result["qty"] = None
                result["risk_budget_usdt"] = None
                result["risk_usdt"] = None
                result["order_plan"] = None
                result["execution"] = None
                result["note"] = (
                    f"{result.get('note', '')} | "
                    f"Price filter: {format_price(current_price)} > max {format_price(max_price_usdt)}"
                ).strip(" |")
            if result.get("signal") == "LIMIT_BUY" and not liq_eval.get("allowed", True):
                result["signal"] = None
                result["action"] = "WAIT_LIQUIDITY"
                result["message"] = ""
                result["qty"] = None
                result["risk_usdt"] = None
                result["order_plan"] = None
                reason = liq_eval.get("reason", "Liquidity filter blocked entry")
                result["note"] = f"{result.get('note', '')} | {reason}".strip(" |")
            cycle_results.append(result)

            signal = result["signal"]
            if signal:
                alert_key = f"{signal}:{result['close_time']}"
                if last_alerts.get(symbol) != alert_key:
                    last_alerts[symbol] = alert_key
                    cycle_alerts.append(result["message"])

                    if signal == "LIMIT_BUY" and risk_cfg.get("open_on_buy_signal", True):
                        if risk_state.get("paused"):
                            cycle_alerts.append(
                                f"ENTRY BLOCKED ({symbol}) | Trading paused by circuit breaker: "
                                f"{risk_state.get('pause_reason', 'risk limit')}"
                            )
                        elif is_cooldown_active(risk_state):
                            remaining = cooldown_seconds_remaining(risk_state)
                            cycle_alerts.append(
                                f"ENTRY BLOCKED ({symbol}) | Cooldown active for {remaining}s: "
                                f"{risk_state.get('cooldown_reason', 'post-loss cooldown')}"
                            )
                        elif live_sync_required and "*" in live_sync_errors:
                            cycle_alerts.append(
                                f"ENTRY BLOCKED ({symbol}) | Live sync unavailable: {live_sync_errors['*']}"
                            )
                        elif live_sync_required and symbol in live_sync_errors:
                            cycle_alerts.append(
                                f"ENTRY BLOCKED ({symbol}) | Live sync error: {live_sync_errors[symbol]}"
                            )
                        elif live_sync_required and symbol in live_synced_pending_entries:
                            pending = live_synced_pending_entries[symbol]
                            cycle_alerts.append(
                                f"ENTRY BLOCKED ({symbol}) | Existing open entry order "
                                f"({pending.get('status', 'NEW')}) on exchange."
                            )
                        elif int(risk_cfg.get("max_open_positions", 1)) > 0 and (
                            (
                                len(live_synced_positions) + len(live_synced_pending_entries)
                                if live_sync_required
                                else len(positions)
                            )
                            >= int(risk_cfg.get("max_open_positions", 1))
                        ):
                            open_count = (
                                len(live_synced_positions) + len(live_synced_pending_entries)
                                if live_sync_required
                                else len(positions)
                            )
                            cycle_alerts.append(
                                f"ENTRY BLOCKED ({symbol}) | Max open positions reached "
                                f"({open_count}/{int(risk_cfg.get('max_open_positions', 1))})"
                            )
                        else:
                            qty = to_float(result.get("qty"), 0.0)
                            if qty <= 0:
                                cycle_alerts.append(
                                    f"ENTRY BLOCKED ({symbol}) | Invalid qty from risk model."
                                )
                            else:
                                entry_submission_ok = False
                                order_plan = result.get("order_plan")
                                if order_plan:
                                    exec_result = execute_bybit_order_plan(
                                        config=config,
                                        exchange_cfg=exchange_cfg,
                                        order_plan=order_plan,
                                    )
                                    result["execution"] = exec_result
                                    execution_events.append(
                                        {
                                            "symbol": symbol,
                                            "result": exec_result,
                                            "time": now_utc_str(),
                                        }
                                        )
                                    if exec_result.get("success"):
                                        cycle_alerts.append(
                                            f"EXECUTION {symbol} | {exec_result.get('message')}"
                                        )
                                        entry_submission_ok = True
                                    else:
                                        cycle_alerts.append(
                                            f"EXECUTION FAILED {symbol} | {exec_result.get('message')}"
                                        )
                                else:
                                    cycle_alerts.append(
                                        f"NO ORDER PLAN ({symbol}) | Spot/non-derivative signal, execution skipped."
                                    )

                                if entry_submission_ok and (
                                    exec_ctx.get("mode") != "live" or exec_ctx.get("assume_filled_on_submit")
                                ):
                                    simulated_entry = to_float(
                                        result.get("wait_price"),
                                        to_float(result.get("price"), 0.0),
                                    )
                                    positions[symbol] = {
                                        "entry": simulated_entry,
                                        "opened_at": now_utc_str(),
                                        "qty": qty,
                                        "tp_price": to_float(result.get("tp_price"), 0.0),
                                        "sl_price": to_float(result.get("sl_price"), 0.0),
                                        "source": source,
                                    }
                    elif signal in {"TAKE_PROFIT", "SELL_NOW", "STOP_LOSS"}:
                        pos = positions.pop(symbol, None)
                        if pos:
                            entry = to_float(pos.get("entry"), 0.0)
                            qty = to_float(pos.get("qty"), 0.0)
                            if entry > 0 and qty > 0:
                                exit_price = to_float(result.get("price"), entry)
                                pnl = compute_trade_pnl_usdt(
                                    entry_price=entry,
                                    exit_price=exit_price,
                                    qty=qty,
                                    costs_cfg=costs_cfg,
                                )
                                risk_state["daily_realized_pnl_usdt"] = to_float(
                                    risk_state.get("daily_realized_pnl_usdt"), 0.0
                                ) + pnl
                                if pnl < 0:
                                    risk_state["consecutive_losses"] = int(
                                        risk_state.get("consecutive_losses", 0)
                                    ) + 1
                                    cooldown_minutes = int(risk_cfg.get("cooldown_minutes_after_loss", 0))
                                    if cooldown_minutes > 0:
                                        risk_state["cooldown_until_ts"] = time.time() + (cooldown_minutes * 60)
                                        risk_state["cooldown_reason"] = (
                                            f"Loss cooldown ({cooldown_minutes}m) after {symbol}"
                                        )
                                elif pnl > 0:
                                    risk_state["consecutive_losses"] = 0
                                cycle_alerts.append(
                                    f"CLOSED {symbol} | PnL {pnl:.2f} USDT | "
                                    f"Daily PnL {risk_state['daily_realized_pnl_usdt']:.2f} USDT"
                                )
                                closed_at = now_utc_str()
                                closed_at_ts = now_utc_ts()
                                add_closed_trade_to_history(
                                    config=config,
                                    state=state,
                                    trade={
                                        "symbol": symbol,
                                        "source": str(pos.get("source", source)),
                                        "opened_at": str(pos.get("opened_at", "")),
                                        "closed_at": closed_at,
                                        "closed_at_ts": closed_at_ts,
                                        "entry_price": entry,
                                        "exit_price": exit_price,
                                        "qty": qty,
                                        "pnl_usdt": pnl,
                                        "net_return_pct": compute_net_return_pct(
                                            entry_price=entry,
                                            exit_price=exit_price,
                                            costs_cfg=costs_cfg,
                                        ),
                                        "exit_signal": signal,
                                        "costs": {
                                            "entry_fee_pct": to_float(costs_cfg.get("entry_fee_pct"), 0.0),
                                            "exit_fee_pct": to_float(costs_cfg.get("exit_fee_pct"), 0.0),
                                            "entry_slippage_pct": to_float(costs_cfg.get("entry_slippage_pct"), 0.0),
                                            "exit_slippage_pct": to_float(costs_cfg.get("exit_slippage_pct"), 0.0),
                                        },
                                    },
                                )
                                update_circuit_breaker_status(
                                    config=config,
                                    state=state,
                                    live_equity_override_usdt=live_equity_override_usdt,
                                )

        except urllib.error.URLError as e:
            cycle_errors.append(f"[{symbol}] Network/API error: {e}")
        except Exception as e:
            cycle_errors.append(f"[{symbol}] Error: {e}")

    trade_history = ensure_trade_history_state(state)
    limits_snapshot = get_risk_limits(
        config,
        state=state,
        live_equity_override_usdt=live_equity_override_usdt,
    )
    performance = {
        "overall": compute_trade_metrics(trade_history),
        "last_7d": compute_trade_metrics(trade_history, lookback_days=7),
    }
    recent_closed_trades = build_recent_closed_trades(trade_history, limit=25)

    return {
        "results": cycle_results,
        "alerts": cycle_alerts,
        "errors": cycle_errors,
        "state": state,
        "live_open_positions": live_synced_positions if live_sync_required else {},
        "live_pending_entries": live_synced_pending_entries if live_sync_required else {},
        "live_sync_errors": live_sync_errors if live_sync_required else {},
        "auto_added_symbols": auto_added_symbols,
        "scanned_symbols": [job["symbol"] for job in symbol_jobs],
        "risk_state": risk_state,
        "risk_limits": limits_snapshot,
        "execution_events": execution_events,
        "performance": performance,
        "recent_closed_trades": recent_closed_trades,
    }


def run_bot(config: Dict[str, Any], run_once: bool = False) -> None:
    ensure_runtime_env()
    state_file = config.get("state_file", DEFAULT_STATE_FILE)
    state = load_json_file(state_file, build_default_state())

    exchange_cfg = config["exchange"]
    exchange_name = str(exchange_cfg.get("name", "binance")).lower()
    symbols = config["symbols"]
    notify_cfg = get_notifications_config(config)
    scan_every_seconds = int(config["scan_every_seconds"])
    interval = config["interval"]
    exec_mode = get_execution_config(config).get("mode", "paper")
    guard = evaluate_live_execution_guard(config=config, exchange_cfg=exchange_cfg)

    print("Starting free crypto alert bot...")
    print(
        f"Exchange={exchange_name} | Symbols: {', '.join(symbols)} "
        f"| interval={interval} | scan={scan_every_seconds}s | execution={exec_mode}"
    )
    for note in config.get("_runtime_notes", []):
        print(f"Runtime note: {note}")
    if exec_mode == "live":
        if guard.get("allowed"):
            lane = "testnet" if guard.get("is_testnet") else "mainnet"
            print(f"Live safety guard: UNLOCKED ({lane})")
        else:
            print("Live safety guard: LOCKED")
            for issue in guard.get("issues", []):
                print(f"  - {issue}")
    if notify_cfg.get("telegram_enabled") and (
        not notify_cfg.get("bot_token") or not notify_cfg.get("chat_id")
    ):
        print(
            "Telegram is enabled but credentials are missing. "
            "Use notifications fields or TELEGRAM_BOT_TOKEN / TELEGRAM_CHAT_ID."
        )
    print("Press Ctrl+C to stop.\n")

    while True:
        cycle = scan_once(config=config, state=state)
        cycle_results = cycle["results"]
        cycle_alerts = cycle["alerts"]
        cycle_errors = cycle["errors"]
        auto_added_symbols = cycle.get("auto_added_symbols", [])
        risk_state = cycle.get("risk_state", {})
        execution_events = cycle.get("execution_events", [])
        perf = cycle.get("performance", {})
        perf_overall = perf.get("overall", {})
        perf_7d = perf.get("last_7d", {})

        for err in cycle_errors:
            print(err)
        if auto_added_symbols:
            print(f"Auto-added from Bybit spot: {', '.join(auto_added_symbols)}")
        if risk_state:
            paused_text = "YES" if risk_state.get("paused") else "NO"
            cooldown_active = is_cooldown_active(risk_state)
            cooldown_text = str(cooldown_seconds_remaining(risk_state)) if cooldown_active else "0"
            print(
                "Risk status | "
                f"day={risk_state.get('day')} "
                f"daily_pnl={to_float(risk_state.get('daily_realized_pnl_usdt')):.2f} USDT "
                f"consec_losses={int(risk_state.get('consecutive_losses', 0))} "
                f"paused={paused_text} "
                f"cooldown_s={cooldown_text}"
            )
            if risk_state.get("paused") and risk_state.get("pause_reason"):
                print(f"Pause reason: {risk_state.get('pause_reason')}")
            if cooldown_active and risk_state.get("cooldown_reason"):
                print(f"Cooldown reason: {risk_state.get('cooldown_reason')}")
        if to_float(perf_overall.get("total_trades"), 0.0) > 0:
            pf_value = perf_overall.get("profit_factor", 0.0)
            pf_text = "INF" if pf_value == float("inf") else f"{to_float(pf_value):.2f}"
            print(
                "Performance | "
                f"trades={int(perf_overall.get('total_trades', 0))} "
                f"win_rate={to_float(perf_overall.get('win_rate_pct')):.1f}% "
                f"net={to_float(perf_overall.get('net_pnl_usdt')):.2f} "
                f"pf={pf_text} "
                f"max_dd={to_float(perf_overall.get('max_drawdown_usdt')):.2f}"
            )
            if to_float(perf_7d.get("total_trades"), 0.0) > 0:
                print(
                    "Performance 7d | "
                    f"trades={int(perf_7d.get('total_trades', 0))} "
                    f"win_rate={to_float(perf_7d.get('win_rate_pct')):.1f}% "
                    f"net={to_float(perf_7d.get('net_pnl_usdt')):.2f}"
                )
        if execution_events:
            for event in execution_events:
                status = "OK" if event["result"].get("success") else "FAILED"
                print(
                    f"Execution event | {event['symbol']} | {status} | "
                    f"{event['result'].get('message')}"
                )

        if cycle_results:
            print_market_snapshot(cycle_results)

        if cycle_alerts:
            print("\nTRADE ALERTS")
            print("-" * 62)
            for idx, alert in enumerate(cycle_alerts, start=1):
                print(f"{idx:>2}. {alert}")

                if notify_cfg.get("telegram_enabled"):
                    try:
                        send_telegram_message(
                            bot_token=notify_cfg["bot_token"],
                            chat_id=notify_cfg["chat_id"],
                            text=alert,
                        )
                    except Exception as e:
                        print(f"    Telegram send failed: {e}")
        else:
            print(f"\n[{now_utc_str()}] No new trade alerts this cycle.")

        save_json_file(state_file, state)
        if run_once:
            break
        time.sleep(scan_every_seconds)


def run_single_scan_with_state(config: Dict[str, Any], persist_state: bool = True) -> Dict[str, Any]:
    state_file = str(config.get("state_file", DEFAULT_STATE_FILE))
    state = load_json_file(state_file, build_default_state())
    cycle = scan_once(config=config, state=state)
    if persist_state:
        save_json_file(state_file, state)
    cycle["state_file"] = state_file
    cycle["state_persisted"] = bool(persist_state)
    if config.get("_runtime_notes"):
        cycle["runtime_notes"] = list(config.get("_runtime_notes", []))
    return cycle


def validate_config(config: Dict[str, Any]) -> None:
    ensure_runtime_env()
    required = [
        "exchange",
        "interval",
        "lookback",
        "scan_every_seconds",
        "symbols",
        "strategy",
        "risk",
        "notifications",
    ]
    missing = [k for k in required if k not in config]
    if missing:
        raise ValueError(f"Missing config keys: {', '.join(missing)}")
    if not config["symbols"]:
        raise ValueError("symbols list cannot be empty")
    exchange_name = str(config["exchange"].get("name", "binance")).lower()
    if exchange_name not in SUPPORTED_EXCHANGES:
        raise ValueError(
            f"Unsupported exchange '{exchange_name}'. "
            f"Supported: {', '.join(sorted(SUPPORTED_EXCHANGES))}"
        )
    backup_urls = config["exchange"].get("backup_base_urls", [])
    if backup_urls and not isinstance(backup_urls, list):
        raise ValueError("exchange.backup_base_urls must be a list of URLs")
    if isinstance(backup_urls, list):
        for idx, url in enumerate(backup_urls, start=1):
            raw = str(url).strip()
            if not raw.startswith("https://"):
                raise ValueError(
                    f"exchange.backup_base_urls[{idx}] must start with https://"
                )
    discovery_cfg = config.get("spot_discovery", {})
    if discovery_cfg:
        add_count = int(discovery_cfg.get("add_count", 1))
        if add_count < 0:
            raise ValueError("spot_discovery.add_count must be >= 0")
        min_turnover = float(discovery_cfg.get("min_turnover_usdt", 0))
        if min_turnover < 0:
            raise ValueError("spot_discovery.min_turnover_usdt must be >= 0")
    risk_cfg = config.get("risk", {})
    for key in ("account_equity_usdt", "risk_per_trade_pct", "max_daily_loss_pct", "max_position_notional_usdt"):
        if key in risk_cfg and to_float(risk_cfg.get(key), 0) < 0:
            raise ValueError(f"risk.{key} must be >= 0")
    if "max_consecutive_losses" in risk_cfg and int(risk_cfg.get("max_consecutive_losses", 0)) < 0:
        raise ValueError("risk.max_consecutive_losses must be >= 0")
    if "max_open_positions" in risk_cfg and int(risk_cfg.get("max_open_positions", 1)) < 0:
        raise ValueError("risk.max_open_positions must be >= 0")
    if "cooldown_minutes_after_loss" in risk_cfg and int(risk_cfg.get("cooldown_minutes_after_loss", 0)) < 0:
        raise ValueError("risk.cooldown_minutes_after_loss must be >= 0")
    comp_cfg = risk_cfg.get("compounding", {})
    if comp_cfg:
        if "enabled" in comp_cfg and not isinstance(comp_cfg.get("enabled"), bool):
            raise ValueError("risk.compounding.enabled must be boolean")
        for key in (
            "position_notional_pct_of_equity",
            "min_position_notional_usdt",
            "max_position_notional_usdt",
        ):
            if key in comp_cfg and to_float(comp_cfg.get(key), 0) < 0:
                raise ValueError(f"risk.compounding.{key} must be >= 0")
    costs_cfg = config.get("execution_costs", {})
    for key in ("entry_fee_pct", "exit_fee_pct", "entry_slippage_pct", "exit_slippage_pct"):
        if key in costs_cfg and to_float(costs_cfg.get(key), 0) < 0:
            raise ValueError(f"execution_costs.{key} must be >= 0")
    liq_cfg = config.get("liquidity_filter", {})
    for key in ("max_spread_pct", "min_turnover_24h_usdt"):
        if key in liq_cfg and to_float(liq_cfg.get(key), 0) < 0:
            raise ValueError(f"liquidity_filter.{key} must be >= 0")
    price_filter_cfg = config.get("price_filter", {})
    if "enabled" in price_filter_cfg and not isinstance(price_filter_cfg.get("enabled"), bool):
        raise ValueError("price_filter.enabled must be boolean")
    if "max_price_usdt" in price_filter_cfg and to_float(price_filter_cfg.get("max_price_usdt"), 0) < 0:
        raise ValueError("price_filter.max_price_usdt must be >= 0")
    if "apply_to_watchlist" in price_filter_cfg and not isinstance(
        price_filter_cfg.get("apply_to_watchlist"), bool
    ):
        raise ValueError("price_filter.apply_to_watchlist must be boolean")
    if "apply_to_spot_discovery" in price_filter_cfg and not isinstance(
        price_filter_cfg.get("apply_to_spot_discovery"), bool
    ):
        raise ValueError("price_filter.apply_to_spot_discovery must be boolean")
    journal_cfg = config.get("journal", {})
    if "enabled" in journal_cfg and not isinstance(journal_cfg.get("enabled"), bool):
        raise ValueError("journal.enabled must be boolean")
    if "max_closed_trades" in journal_cfg and int(journal_cfg.get("max_closed_trades", 0)) <= 0:
        raise ValueError("journal.max_closed_trades must be > 0")
    exec_cfg = config.get("execution", {})
    env_mode = os.getenv("TRADING_BOT_EXECUTION_MODE", "")
    mode = str(env_mode or exec_cfg.get("mode", "paper")).lower()
    if mode not in {"paper", "live"}:
        raise ValueError("execution.mode must be 'paper' or 'live'")
    if mode == "live" and exchange_name != "bybit":
        raise ValueError("execution.mode='live' currently supports only exchange.name='bybit'")
    if "recv_window_ms" in exec_cfg and int(exec_cfg.get("recv_window_ms", 0)) <= 0:
        raise ValueError("execution.recv_window_ms must be > 0")
    live_safety_cfg = exec_cfg.get("live_safety", {})
    if "require_manual_unlock" in live_safety_cfg and not isinstance(
        live_safety_cfg.get("require_manual_unlock"), bool
    ):
        raise ValueError("execution.live_safety.require_manual_unlock must be boolean")
    if "require_mainnet_flag" in live_safety_cfg and not isinstance(
        live_safety_cfg.get("require_mainnet_flag"), bool
    ):
        raise ValueError("execution.live_safety.require_mainnet_flag must be boolean")
    required_phrase = str(live_safety_cfg.get("required_ack_phrase", LIVE_ACK_DEFAULT)).strip()
    if not required_phrase:
        raise ValueError("execution.live_safety.required_ack_phrase cannot be empty")


def main() -> None:
    parser = argparse.ArgumentParser(description="Free crypto alert bot")
    parser.add_argument(
        "--config",
        default="configs/config.json",
        help="Path to config JSON (default: configs/config.json)",
    )
    parser.add_argument(
        "--once",
        action="store_true",
        help="Run one scan cycle then exit",
    )
    args = parser.parse_args()

    config = load_json_file(args.config, None)
    if config is None:
        raise FileNotFoundError(
            f"Config file not found: {args.config}\n"
            "Copy configs/config.example.json to configs/config.json and edit it first."
        )

    runtime_config = prepare_config_for_runtime(config)
    validate_config(runtime_config)
    run_bot(runtime_config, run_once=args.once)


if __name__ == "__main__":
    main()
