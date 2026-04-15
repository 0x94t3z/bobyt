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
    fetch_bybit_execution_history_for_symbol as client_fetch_bybit_execution_history_for_symbol,
    fetch_bybit_instrument_constraints as client_fetch_bybit_instrument_constraints,
    fetch_bybit_live_position_for_symbol as client_fetch_bybit_live_position_for_symbol,
    fetch_bybit_order_history_for_symbol as client_fetch_bybit_order_history_for_symbol,
    fetch_bybit_open_orders_for_symbol as client_fetch_bybit_open_orders_for_symbol,
    fetch_bybit_tickers as client_fetch_bybit_tickers,
    get_bybit_base_urls as client_get_bybit_base_urls,
    is_active_open_order_status as client_is_active_open_order_status,
    is_bybit_duplicate_order_response as client_is_bybit_duplicate_order_response,
)
from .state_store import (
    describe_json_storage_backend,
    load_persisted_json,
    save_closed_trade_record,
    save_persisted_json,
)


DEFAULT_STATE_FILE = "state/bot_state.json"
DEFAULT_VERCEL_STATE_FILE = "/tmp/trading_bot_state.json"
DEFAULT_JOURNAL_LIMIT = 5000
DEFAULT_EXECUTION_EVENTS_LIMIT = 1000
SUPPORTED_EXCHANGES = {"binance", "bybit"}
BYBIT_SUPPORTED_CATEGORIES = {"linear", "inverse", "spot"}
BYBIT_DERIVATIVE_CATEGORIES = {"linear", "inverse"}
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
        "spot_entry_hints": {},
        "spot_close_candidates": {},
        "execution_events_history": [],
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
    state_file = str(cfg.get("state_file", DEFAULT_STATE_FILE))
    storage_info = describe_json_storage_backend(path=state_file, purpose="state")
    backend = storage_info.get("backend", "file")
    if backend == "postgres":
        notes.append(
            "State backend: postgres "
            f"(table={storage_info.get('table')}, key={storage_info.get('storage_key')})"
        )
    else:
        notes.append(f"State backend: file ({state_file})")
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


def normalize_bybit_category(raw: Any, fallback: str = "linear") -> str:
    candidate = str(raw or "").strip().lower()
    if candidate in BYBIT_SUPPORTED_CATEGORIES:
        return candidate
    return fallback


def get_bybit_default_category(exchange_cfg: Dict[str, Any]) -> str:
    return normalize_bybit_category(exchange_cfg.get("category", "linear"), fallback="linear")


def split_symbol_base_quote(symbol: str) -> Dict[str, str]:
    symbol_up = str(symbol).upper().strip()
    for quote in ("USDT", "USDC", "BTC", "ETH"):
        if symbol_up.endswith(quote) and len(symbol_up) > len(quote):
            return {"base": symbol_up[: -len(quote)], "quote": quote}
    return {"base": symbol_up, "quote": ""}


def is_bot_owned_marker(payload: Any) -> bool:
    if not isinstance(payload, dict):
        return False
    if parse_env_bool(payload.get("managed_by_bot"), False):
        return True
    order_link_id = str(
        payload.get("order_link_id")
        or payload.get("orderLinkId")
        or ""
    ).strip().lower()
    return order_link_id.startswith("cab")


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


def get_regime_filter_config(strategy_cfg: Dict[str, Any]) -> Dict[str, Any]:
    regime_cfg = strategy_cfg.get("regime_filter", {})
    return {
        "enabled": bool(regime_cfg.get("enabled", False)),
        "require_uptrend": bool(regime_cfg.get("require_uptrend", True)),
        "min_trend_pct": max(0.0, to_float(regime_cfg.get("min_trend_pct"), 0.0)),
        "require_slow_ema_rising": bool(regime_cfg.get("require_slow_ema_rising", True)),
        "min_ema_slope_pct": max(0.0, to_float(regime_cfg.get("min_ema_slope_pct"), 0.0)),
    }


def evaluate_regime_filter(
    strategy_cfg: Dict[str, Any],
    ema_fast_now: float,
    ema_slow_now: float,
    ema_slow_prev: float,
    price_now: float,
) -> Dict[str, Any]:
    trend_pct = ((ema_fast_now - ema_slow_now) / price_now) * 100 if price_now else 0.0
    slow_ema_slope_pct = ((ema_slow_now - ema_slow_prev) / price_now) * 100 if price_now else 0.0

    regime_cfg = get_regime_filter_config(strategy_cfg)
    if not regime_cfg.get("enabled", False):
        return {
            "enabled": False,
            "ok": True,
            "trend_pct": trend_pct,
            "slow_ema_slope_pct": slow_ema_slope_pct,
            "reason": "",
        }

    blockers: List[str] = []
    if regime_cfg.get("require_uptrend", True) and ema_fast_now <= ema_slow_now:
        blockers.append("ema_fast<=ema_slow")
    min_trend_pct = to_float(regime_cfg.get("min_trend_pct"), 0.0)
    if min_trend_pct > 0 and trend_pct < min_trend_pct:
        blockers.append(f"trend_pct {trend_pct:.3f}% < {min_trend_pct:.3f}%")
    if regime_cfg.get("require_slow_ema_rising", True) and slow_ema_slope_pct <= 0:
        blockers.append("slow_ema_slope<=0")
    min_slope_pct = to_float(regime_cfg.get("min_ema_slope_pct"), 0.0)
    if min_slope_pct > 0 and slow_ema_slope_pct < min_slope_pct:
        blockers.append(f"slow_ema_slope {slow_ema_slope_pct:.3f}% < {min_slope_pct:.3f}%")

    return {
        "enabled": True,
        "ok": len(blockers) == 0,
        "trend_pct": trend_pct,
        "slow_ema_slope_pct": slow_ema_slope_pct,
        "reason": "; ".join(blockers),
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
    max_events = int(journal_cfg.get("max_execution_events", DEFAULT_EXECUTION_EVENTS_LIMIT))
    return {
        "enabled": bool(journal_cfg.get("enabled", True)),
        "max_closed_trades": max(100, max_closed),
        "max_execution_events": max(100, max_events),
    }


def ensure_trade_history_state(state: Dict[str, Any]) -> List[Dict[str, Any]]:
    trade_history = state.setdefault("trade_history", [])
    if not isinstance(trade_history, list):
        trade_history = []
        state["trade_history"] = trade_history
    return trade_history


def ensure_execution_events_history_state(state: Dict[str, Any]) -> List[Dict[str, Any]]:
    history = state.setdefault("execution_events_history", [])
    if not isinstance(history, list):
        history = []
        state["execution_events_history"] = history
    return history


def add_execution_events_to_history(
    config: Dict[str, Any],
    state: Dict[str, Any],
    execution_events: List[Dict[str, Any]],
) -> None:
    if not isinstance(execution_events, list) or not execution_events:
        return
    journal_cfg = get_journal_config(config)
    history = ensure_execution_events_history_state(state)
    for event in execution_events:
        if not isinstance(event, dict):
            continue
        history.append(
            {
                "time": str(event.get("time", now_utc_str())),
                "symbol": str(event.get("symbol", "")),
                "result": event.get("result", {}),
            }
        )
    max_events = int(journal_cfg.get("max_execution_events", DEFAULT_EXECUTION_EVENTS_LIMIT))
    overflow = len(history) - max_events
    if overflow > 0:
        del history[:overflow]


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

    state_file = str(config.get("state_file", DEFAULT_STATE_FILE))
    journal_error = save_closed_trade_record(path=state_file, trade=trade, purpose="state")
    if journal_error:
        trade.setdefault("journal_warning", f"closed_trade_record_not_saved: {journal_error}")


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


def _qty_decimals_from_step(step: float) -> int:
    if step <= 0:
        return 0
    text = format(Decimal(str(step)).normalize(), "f")
    if "." not in text:
        return 0
    return len(text.split(".", 1)[1].rstrip("0"))


def format_order_qty(qty: float, qty_step: float = 0.0) -> str:
    if qty <= 0:
        return "0"
    d_qty = Decimal(str(qty))
    if qty_step > 0:
        d_step = Decimal(str(qty_step))
        units = (d_qty / d_step).to_integral_value(rounding=ROUND_DOWN)
        d_qty = units * d_step
        step_decimals = _qty_decimals_from_step(qty_step)
        if step_decimals > 0:
            quantum = Decimal(1).scaleb(-step_decimals)
            d_qty = d_qty.quantize(quantum, rounding=ROUND_DOWN)
    d_qty = d_qty.normalize()
    text = format(d_qty, "f")
    if "." in text:
        text = text.rstrip("0").rstrip(".")
    return text or "0"


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


def get_autoscale_config(config: Dict[str, Any]) -> Dict[str, Any]:
    risk_cfg = config.get("risk", {})
    comp_cfg = risk_cfg.get("compounding", {})
    autoscale_cfg = comp_cfg.get("autoscale", {})
    return {
        "enabled": bool(autoscale_cfg.get("enabled", False)),
        "lookback_days": max(0, int(to_float(autoscale_cfg.get("lookback_days"), 3))),
        "min_trades": max(0, int(to_float(autoscale_cfg.get("min_trades"), 30))),
        "min_win_rate_pct": max(0.0, to_float(autoscale_cfg.get("min_win_rate_pct"), 52.0)),
        "min_profit_factor": max(0.0, to_float(autoscale_cfg.get("min_profit_factor"), 1.2)),
        "min_net_pnl_usdt": to_float(autoscale_cfg.get("min_net_pnl_usdt"), 0.0),
        "max_drawdown_limit_usdt": max(0.0, to_float(autoscale_cfg.get("max_drawdown_limit_usdt"), 0.0)),
    }


def evaluate_autoscale_eligibility(
    config: Dict[str, Any],
    state: Optional[Dict[str, Any]],
) -> Dict[str, Any]:
    autoscale_cfg = get_autoscale_config(config)
    if not autoscale_cfg.get("enabled", False):
        return {
            "enabled": False,
            "eligible": True,
            "reason": "Autoscale disabled",
            "metrics": {},
        }

    trade_history = []
    if isinstance(state, dict):
        raw_history = state.get("trade_history", [])
        if isinstance(raw_history, list):
            trade_history = raw_history
    lookback_days = int(autoscale_cfg.get("lookback_days", 0))
    metrics = compute_trade_metrics(
        trade_history,
        lookback_days=lookback_days if lookback_days > 0 else None,
    )

    total_trades = int(metrics.get("total_trades", 0))
    win_rate = to_float(metrics.get("win_rate_pct"), 0.0)
    profit_factor = metrics.get("profit_factor", 0.0)
    net_pnl = to_float(metrics.get("net_pnl_usdt"), 0.0)
    max_drawdown = to_float(metrics.get("max_drawdown_usdt"), 0.0)

    checks: List[str] = []
    if total_trades < int(autoscale_cfg.get("min_trades", 0)):
        checks.append(
            f"need trades>={int(autoscale_cfg.get('min_trades', 0))} (got {total_trades})"
        )
    if win_rate < to_float(autoscale_cfg.get("min_win_rate_pct"), 0.0):
        checks.append(
            f"need win_rate>={to_float(autoscale_cfg.get('min_win_rate_pct'), 0.0):.2f}% "
            f"(got {win_rate:.2f}%)"
        )
    min_pf = to_float(autoscale_cfg.get("min_profit_factor"), 0.0)
    if min_pf > 0:
        pf_value = float("inf") if profit_factor == float("inf") else to_float(profit_factor, 0.0)
        if pf_value < min_pf:
            checks.append(f"need profit_factor>={min_pf:.2f} (got {pf_value:.2f})")
    min_net_pnl = to_float(autoscale_cfg.get("min_net_pnl_usdt"), 0.0)
    if net_pnl < min_net_pnl:
        checks.append(f"need net_pnl>={min_net_pnl:.2f} (got {net_pnl:.2f})")
    max_dd_limit = to_float(autoscale_cfg.get("max_drawdown_limit_usdt"), 0.0)
    if max_dd_limit > 0 and max_drawdown > max_dd_limit:
        checks.append(f"need max_drawdown<={max_dd_limit:.2f} (got {max_drawdown:.2f})")

    eligible = len(checks) == 0
    return {
        "enabled": True,
        "eligible": eligible,
        "reason": "OK" if eligible else "; ".join(checks),
        "metrics": metrics,
    }


def get_risk_limits(
    config: Dict[str, Any],
    state: Optional[Dict[str, Any]] = None,
    live_equity_override_usdt: Optional[float] = None,
) -> Dict[str, Any]:
    risk_cfg = config.get("risk", {})
    base_equity = to_float(risk_cfg.get("account_equity_usdt"), 0.0)
    realized_pnl = get_state_total_realized_pnl_usdt(state)
    effective_equity = max(0.0, base_equity + realized_pnl)
    live_equity = to_float(live_equity_override_usdt, 0.0)
    compounding_equity = live_equity if live_equity > 0 else effective_equity
    risk_per_trade_pct = to_float(risk_cfg.get("risk_per_trade_pct"), 0.0)
    max_daily_loss_pct = to_float(risk_cfg.get("max_daily_loss_pct"), 0.0)
    base_max_position_notional = to_float(risk_cfg.get("max_position_notional_usdt"), 0.0)
    max_position_notional = base_max_position_notional

    comp_cfg = get_compounding_config(config)
    autoscale_eval = evaluate_autoscale_eligibility(config=config, state=state)
    autoscale_enabled = bool(autoscale_eval.get("enabled", False))
    autoscale_allowed = bool(autoscale_eval.get("eligible", True))
    # When autoscale is enabled but not yet eligible, keep risk sizing anchored to base equity.
    sizing_equity = compounding_equity
    if autoscale_enabled and not autoscale_allowed:
        sizing_equity = base_equity
    if (
        bool(comp_cfg.get("enabled"))
        and autoscale_allowed
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
        # Autoscale mode should only increase size above base cap, never reduce it.
        if autoscale_enabled:
            dynamic_notional = max(dynamic_notional, base_max_position_notional)
        max_position_notional = dynamic_notional

    risk_limits = {
        "equity": base_equity,
        "effective_equity_usdt": effective_equity,
        "compounding_equity_usdt": compounding_equity,
        "risk_per_trade_usdt": sizing_equity * (risk_per_trade_pct / 100.0),
        # Keep daily circuit-breaker anchored to configured base equity.
        "daily_loss_limit_usdt": base_equity * (max_daily_loss_pct / 100.0),
        "max_position_notional_usdt": max_position_notional,
    }
    if autoscale_enabled:
        risk_limits["autoscale_enabled"] = True
        risk_limits["autoscale_allowed"] = autoscale_allowed
        risk_limits["autoscale_reason"] = str(autoscale_eval.get("reason", ""))
    return risk_limits


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
    spot_native_tpsl_on_entry: bool = True,
    qty_step: float = 0.0,
) -> Optional[Dict[str, Any]]:
    category = normalize_bybit_category(category, fallback="")
    if category not in BYBIT_SUPPORTED_CATEGORIES:
        return None
    qty_text = format_order_qty(qty, qty_step=qty_step)
    entry_order: Dict[str, Any] = {
        "category": category,
        "symbol": symbol,
        "side": "Buy",
        "orderType": "Limit",
        "price": f"{entry_price:.10f}",
        "qty": qty_text,
        "timeInForce": "GTC",
    }
    if category == "spot":
        plan_mode = "spot_entry_only"
        if spot_native_tpsl_on_entry and tp_price > 0 and sl_price > 0:
            entry_order["takeProfit"] = f"{tp_price:.10f}"
            entry_order["stopLoss"] = f"{sl_price:.10f}"
            entry_order["tpOrderType"] = "Market"
            entry_order["slOrderType"] = "Market"
            plan_mode = "spot_entry_with_tpsl"
        if order_group_id:
            entry_order["orderLinkId"] = build_order_link_id(order_group_id, "en")
        return {
            "plan_mode": plan_mode,
            "order_group_id": order_group_id or "",
            "entry_order": {
                **entry_order,
            },
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
        "plan_mode": "derivatives_bracket",
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


def build_bybit_spot_exit_order_plan(
    symbol: str,
    qty: float,
    order_group_id: str = "",
    qty_constraints: Optional[Dict[str, float]] = None,
    reference_price: float = 0.0,
) -> Optional[Dict[str, Any]]:
    if qty <= 0:
        return None
    qty_step = to_float((qty_constraints or {}).get("qty_step"), 0.0)
    min_qty = to_float((qty_constraints or {}).get("min_qty"), 0.0)
    max_qty = to_float((qty_constraints or {}).get("max_qty"), 0.0)
    min_notional = to_float((qty_constraints or {}).get("min_notional"), 0.0)
    adjusted_qty = floor_to_step(qty, qty_step) if qty_step > 0 else qty
    if max_qty > 0:
        adjusted_qty = min(adjusted_qty, max_qty)
    if adjusted_qty <= 0:
        return None
    if min_qty > 0 and adjusted_qty < min_qty:
        return None
    if min_notional > 0 and reference_price > 0:
        if (adjusted_qty * reference_price) < min_notional:
            return None
    exit_order: Dict[str, Any] = {
        "category": "spot",
        "symbol": symbol,
        "side": "Sell",
        "orderType": "Market",
        "qty": format_order_qty(adjusted_qty, qty_step=qty_step),
    }
    if order_group_id:
        exit_order["orderLinkId"] = build_order_link_id(order_group_id, "sx")
    return {
        "plan_mode": "spot_exit_market",
        "order_group_id": order_group_id or "",
        "exit_order": {
            **exit_order,
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


def fetch_bybit_order_history_for_symbol(
    base_urls: List[str],
    api_key: str,
    api_secret: str,
    recv_window: int,
    category: str,
    symbol: str,
    limit: int = 50,
    order_id: str = "",
    order_link_id: str = "",
) -> List[Dict[str, Any]]:
    return client_fetch_bybit_order_history_for_symbol(
        base_urls=base_urls,
        api_key=api_key,
        api_secret=api_secret,
        recv_window=recv_window,
        category=category,
        symbol=symbol,
        limit=limit,
        order_id=order_id,
        order_link_id=order_link_id,
    )


def fetch_bybit_execution_history_for_symbol(
    base_urls: List[str],
    api_key: str,
    api_secret: str,
    recv_window: int,
    category: str,
    symbol: str,
    limit: int = 50,
    start_time_ms: int = 0,
    end_time_ms: int = 0,
) -> List[Dict[str, Any]]:
    return client_fetch_bybit_execution_history_for_symbol(
        base_urls=base_urls,
        api_key=api_key,
        api_secret=api_secret,
        recv_window=recv_window,
        category=category,
        symbol=symbol,
        limit=limit,
        start_time_ms=start_time_ms,
        end_time_ms=end_time_ms,
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


def find_matching_order_history_row(
    rows: List[Dict[str, Any]],
    order_id: str = "",
    order_link_id: str = "",
) -> Dict[str, Any]:
    order_id = str(order_id or "").strip()
    order_link_id = str(order_link_id or "").strip()
    if not rows:
        return {}
    if order_id:
        for row in rows:
            if str(row.get("orderId", "")).strip() == order_id:
                return row
    if order_link_id:
        for row in rows:
            if str(row.get("orderLinkId", "")).strip() == order_link_id:
                return row
    for row in rows:
        if str(row.get("side", "")).upper() == "BUY" and is_bot_owned_marker(row):
            return row
    return {}


def get_bybit_order_filled_qty(order_row: Dict[str, Any]) -> float:
    filled = abs(to_float(order_row.get("cumExecQty"), 0.0))
    if filled > 0:
        return filled
    return abs(to_float(order_row.get("cumExecQuantity"), 0.0))


def get_bybit_order_fill_price(order_row: Dict[str, Any], fallback_price: float = 0.0) -> float:
    avg_price = to_float(order_row.get("avgPrice"), 0.0)
    if avg_price > 0:
        return avg_price
    filled_qty = get_bybit_order_filled_qty(order_row)
    if filled_qty > 0:
        cum_exec_value = to_float(order_row.get("cumExecValue"), 0.0)
        if cum_exec_value > 0:
            return cum_exec_value / filled_qty
    candidate = to_float(order_row.get("price"), 0.0)
    if candidate > 0:
        return candidate
    return fallback_price


def parse_bot_utc_to_ts(value: Any) -> float:
    text = str(value or "").strip()
    if not text:
        return 0.0
    try:
        dt = datetime.strptime(text, "%Y-%m-%d %H:%M:%S UTC")
        return dt.replace(tzinfo=timezone.utc).timestamp()
    except Exception:
        return 0.0


def extract_timestamp_ms(row: Dict[str, Any], keys: List[str]) -> int:
    for key in keys:
        raw = row.get(key)
        ts_ms = int(to_float(raw, 0.0))
        if ts_ms > 0:
            return ts_ms
    return 0


def find_spot_exit_fill_summary(
    execution_rows: List[Dict[str, Any]],
    opened_at_ts: float = 0.0,
) -> Dict[str, Any]:
    if not execution_rows:
        return {}
    candidates: List[Dict[str, Any]] = []
    for row in execution_rows:
        side = str(row.get("side", "")).upper()
        if side != "SELL":
            continue
        exec_qty = abs(to_float(row.get("execQty"), 0.0))
        exec_price = to_float(row.get("execPrice"), 0.0)
        if exec_qty <= 0 or exec_price <= 0:
            continue
        ts_ms = extract_timestamp_ms(row, ["execTime", "createdTime", "updatedTime"])
        if opened_at_ts > 0 and ts_ms > 0:
            # small grace window for clock skew across systems
            if ts_ms < int((opened_at_ts - 120.0) * 1000):
                continue
        enriched = dict(row)
        enriched["_exec_ts_ms"] = ts_ms
        candidates.append(enriched)
    if not candidates:
        return {}

    candidates.sort(
        key=lambda x: (int(to_float(x.get("_exec_ts_ms"), 0.0)), int(is_bot_owned_marker(x))),
        reverse=True,
    )
    latest = candidates[0]
    order_id = str(latest.get("orderId", "")).strip()
    grouped = [latest]
    if order_id:
        grouped = [row for row in candidates if str(row.get("orderId", "")).strip() == order_id]

    total_qty = sum(abs(to_float(row.get("execQty"), 0.0)) for row in grouped)
    if total_qty <= 0:
        return {}
    weighted_exit = sum(
        abs(to_float(row.get("execQty"), 0.0)) * to_float(row.get("execPrice"), 0.0)
        for row in grouped
    ) / total_qty
    ts_ms = max(int(to_float(row.get("_exec_ts_ms"), 0.0)) for row in grouped)
    return {
        "exit_price": weighted_exit,
        "exit_qty": total_qty,
        "exec_time_ms": ts_ms,
        "order_id": order_id,
        "order_link_id": str(latest.get("orderLinkId", "")).strip(),
        "source": "bot_link" if is_bot_owned_marker(latest) else "recent_sell_execution",
    }


def find_execution_fill_for_order(
    execution_rows: List[Dict[str, Any]],
    order_id: str = "",
    order_link_id: str = "",
) -> Dict[str, Any]:
    order_id = str(order_id or "").strip()
    order_link_id = str(order_link_id or "").strip()
    if not execution_rows:
        return {}
    selected: List[Dict[str, Any]] = []
    if order_id:
        selected = [row for row in execution_rows if str(row.get("orderId", "")).strip() == order_id]
    if not selected and order_link_id:
        selected = [
            row for row in execution_rows if str(row.get("orderLinkId", "")).strip() == order_link_id
        ]
    if not selected:
        return {}
    filled_rows = []
    for row in selected:
        qty = abs(to_float(row.get("execQty"), 0.0))
        price = to_float(row.get("execPrice"), 0.0)
        if qty <= 0 or price <= 0:
            continue
        filled_rows.append(row)
    if not filled_rows:
        return {}
    total_qty = sum(abs(to_float(row.get("execQty"), 0.0)) for row in filled_rows)
    if total_qty <= 0:
        return {}
    weighted_price = sum(
        abs(to_float(row.get("execQty"), 0.0)) * to_float(row.get("execPrice"), 0.0)
        for row in filled_rows
    ) / total_qty
    ts_ms = max(extract_timestamp_ms(row, ["execTime", "createdTime", "updatedTime"]) for row in filled_rows)
    return {
        "exit_price": weighted_price,
        "exit_qty": total_qty,
        "exec_time_ms": ts_ms,
    }


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


def fetch_bybit_wallet_coin_balances(
    base_urls: List[str],
    api_key: str,
    api_secret: str,
    recv_window: int,
) -> Dict[str, Dict[str, float]]:
    account_types = ["UNIFIED", "SPOT", "CONTRACT"]
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
            if not isinstance(rows, list) or not rows:
                errors.append(f"{account_type}: empty account rows")
                continue

            coin_balances: Dict[str, Dict[str, float]] = {}
            for row in rows:
                coins = row.get("coin", [])
                if not isinstance(coins, list):
                    continue
                for coin in coins:
                    coin_name = str(coin.get("coin", "")).upper().strip()
                    if not coin_name:
                        continue
                    wallet_qty = to_float(coin.get("walletBalance"), 0.0)
                    available_qty = to_float(coin.get("availableToWithdraw"), wallet_qty)
                    free_qty = to_float(coin.get("free"), available_qty)
                    if wallet_qty <= 0 and available_qty <= 0 and free_qty <= 0:
                        continue
                    bucket = coin_balances.setdefault(
                        coin_name,
                        {"wallet": 0.0, "available": 0.0, "free": 0.0},
                    )
                    bucket["wallet"] += max(0.0, wallet_qty)
                    bucket["available"] += max(0.0, available_qty)
                    bucket["free"] += max(0.0, free_qty)

            if coin_balances:
                return coin_balances
            errors.append(f"{account_type}: no positive coin balances")
        except Exception as e:
            errors.append(f"{account_type}: {e}")

    raise RuntimeError("Unable to fetch wallet coin balances from Bybit: " + " | ".join(errors))


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
        "bybit": {
            "spot_native_tpsl_on_entry": bool(bybit_cfg.get("spot_native_tpsl_on_entry", True)),
        },
        "live_safety": {
            "require_manual_unlock": bool(live_safety_cfg.get("require_manual_unlock", True)),
            "required_ack_phrase": str(
                live_safety_cfg.get("required_ack_phrase", LIVE_ACK_DEFAULT)
            ).strip()
            or LIVE_ACK_DEFAULT,
            "require_mainnet_flag": bool(live_safety_cfg.get("require_mainnet_flag", True)),
            "allow_unprotected_spot_entry": bool(
                live_safety_cfg.get("allow_unprotected_spot_entry", False)
            ),
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
    exchange_name = str(exchange_cfg.get("name", "")).lower()
    if exchange_name == "bybit":
        bybit_category = get_bybit_default_category(exchange_cfg)
        bybit_exec_cfg = exec_ctx.get("bybit", {})
        spot_native_tpsl_on_entry = bool(bybit_exec_cfg.get("spot_native_tpsl_on_entry", True))
        if bybit_category == "spot" and bool(exec_ctx.get("assume_filled_on_submit")):
            issues.append(
                "Bybit spot live requires execution.assume_filled_on_submit=false."
            )
        if (
            bybit_category == "spot"
            and not spot_native_tpsl_on_entry
            and not bool(safety.get("allow_unprotected_spot_entry", False))
        ):
            issues.append(
                "Bybit spot live native TP/SL on entry is disabled. "
                "Either set execution.bybit.spot_native_tpsl_on_entry=true (recommended) "
                "or set execution.live_safety.allow_unprotected_spot_entry=true to acknowledge this risk."
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

    submit_order = ["entry_order", "take_profit_order", "stop_loss_order", "exit_order"]
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

    plan_mode = str(order_plan.get("plan_mode", "")).lower()
    success_message = "Bybit order plan submitted."
    if plan_mode == "spot_entry_only":
        success_message = "Bybit spot entry order submitted."
    elif plan_mode == "spot_entry_with_tpsl":
        success_message = "Bybit spot entry order submitted with attached TP/SL."
    elif plan_mode == "spot_exit_market":
        success_message = "Bybit spot exit sell submitted."

    return {
        "mode": mode,
        "submitted": bool(submitted_refs) or duplicate_ok_count > 0,
        "success": True,
        "message": success_message,
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

    regime_eval = evaluate_regime_filter(
        strategy_cfg=strategy_cfg,
        ema_fast_now=ema_fast_now,
        ema_slow_now=ema_slow_now,
        ema_slow_prev=ema_slow_prev,
        price_now=price_now,
    )
    trend_pct = to_float(regime_eval.get("trend_pct"), 0.0)
    score = (trend_pct * 2.0) + (1.0 - min(abs(rsi_now - 60.0), 40.0) / 40.0) * 2.0

    result: Dict[str, Any] = {
        "symbol": symbol,
        "source": "WATCHLIST",
        "price": price_now,
        "ema_fast": ema_fast_now,
        "ema_slow": ema_slow_now,
        "rsi": rsi_now,
        "score": score,
        "trend_pct": trend_pct,
        "slow_ema_slope_pct": to_float(regime_eval.get("slow_ema_slope_pct"), 0.0),
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
        if not regime_eval.get("ok", True):
            result["action"] = "WAIT_REGIME"
            result["wait_price"] = limit_price
            exits = build_exit_prices(limit_price, tp_pct, sl_pct)
            result["tp_price"] = exits["tp_price"]
            result["sl_price"] = exits["sl_price"]
            result["note"] = (
                f"Regime filter blocked entry ({regime_eval.get('reason', 'weak trend')})"
            )
            return result
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
        category = normalize_bybit_category(result.get("market_category"), fallback="")
        if not category:
            category = (
                "spot"
                if result.get("source") == "SPOT_BEST"
                else get_bybit_default_category(exchange_cfg)
            )
        if category == "spot":
            tradable = True
            if isinstance(qty_constraints, dict):
                tradable = bool(qty_constraints.get("tradable", True))
            if not tradable:
                status = str((qty_constraints or {}).get("status", "UNKNOWN"))
                result["signal"] = None
                result["action"] = "WAIT_LISTING"
                result["message"] = ""
                result["qty"] = None
                result["risk_usdt"] = None
                result["order_plan"] = None
                result["note"] = (
                    f"{result.get('note', '')} | Instrument status {status}; not tradable yet."
                ).strip(" |")
                return
        exec_cfg = config.get("execution", {})
        bybit_exec_cfg = exec_cfg.get("bybit", {})
        spot_native_tpsl_on_entry = bool(bybit_exec_cfg.get("spot_native_tpsl_on_entry", True))
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
            spot_native_tpsl_on_entry=spot_native_tpsl_on_entry,
            qty_step=to_float((qty_constraints or {}).get("qty_step"), 0.0),
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
    spot_entry_hints: Dict[str, Dict[str, Any]] = state.setdefault("spot_entry_hints", {})
    spot_close_candidates: Dict[str, Dict[str, Any]] = state.setdefault("spot_close_candidates", {})
    if not isinstance(live_open_positions_state, dict):
        live_open_positions_state = {}
        state["live_open_positions"] = live_open_positions_state
    if not isinstance(live_pending_entries_state, dict):
        live_pending_entries_state = {}
        state["live_pending_entries"] = live_pending_entries_state
    if not isinstance(spot_entry_hints, dict):
        spot_entry_hints = {}
        state["spot_entry_hints"] = spot_entry_hints
    if not isinstance(spot_close_candidates, dict):
        spot_close_candidates = {}
        state["spot_close_candidates"] = spot_close_candidates
    ensure_trade_history_state(state)
    ensure_execution_events_history_state(state)
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
    default_bybit_category = (
        get_bybit_default_category(exchange_cfg) if exchange_name == "bybit" else ""
    )
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
        symbol_jobs.append(
            {
                "symbol": symbol,
                "category": default_bybit_category if exchange_name == "bybit" else "",
                "source": "WATCHLIST",
            }
        )

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
            (job["category"] or default_bybit_category)
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
            category = normalize_bybit_category(job["category"] or default_bybit_category, fallback="")
            if category not in BYBIT_SUPPORTED_CATEGORIES:
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
            spot_coin_balances: Dict[str, Dict[str, float]] = {}
            spot_wallet_sync_ok = True
            spot_wallet_sync_error = ""
            if any(
                normalize_bybit_category(job.get("category", ""), fallback=default_bybit_category) == "spot"
                for job in symbol_jobs
            ):
                try:
                    spot_coin_balances = fetch_bybit_wallet_coin_balances(
                        base_urls=base_urls,
                        api_key=api_key,
                        api_secret=api_secret,
                        recv_window=recv_window,
                    )
                except Exception as e:
                    spot_wallet_sync_ok = False
                    spot_wallet_sync_error = str(e)
                    cycle_errors.append(f"[LIVE_SYNC:SPOT] Wallet error: {e}")

            synced_pairs: set[str] = set()
            for job in symbol_jobs:
                symbol = str(job["symbol"]).upper()
                category = normalize_bybit_category(job["category"] or default_bybit_category, fallback="")
                pair_key = f"{category}:{symbol}"
                if pair_key in synced_pairs:
                    continue
                synced_pairs.add(pair_key)
                if category in BYBIT_DERIVATIVE_CATEGORIES:
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
                    continue

                if category != "spot":
                    continue

                try:
                    open_orders = fetch_bybit_open_orders_for_symbol(
                        base_urls=base_urls,
                        api_key=api_key,
                        api_secret=api_secret,
                        recv_window=recv_window,
                        category="spot",
                        symbol=symbol,
                    )
                except Exception as e:
                    live_sync_errors[symbol] = f"spot open-order sync failed: {e}"
                    cycle_errors.append(f"[LIVE_SYNC:{symbol}] Spot open-order error: {e}")
                    open_orders = []

                entry_orders = [
                    row
                    for row in open_orders
                    if str(row.get("side", "")).upper() == "BUY"
                    and is_bot_owned_marker(row)
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
                    spot_close_candidates.pop(symbol, None)

                if not spot_wallet_sync_ok:
                    live_sync_errors[symbol] = f"spot wallet sync unavailable: {spot_wallet_sync_error}"
                    # Without wallet balances we cannot infer held qty safely.
                    # Keep prior state untouched and retry on next cycle.
                    continue

                base_asset = split_symbol_base_quote(symbol).get("base", "")
                coin_row = spot_coin_balances.get(base_asset, {})
                wallet_qty = to_float(coin_row.get("wallet"), 0.0)
                available_qty = to_float(coin_row.get("available"), wallet_qty)
                free_qty = to_float(coin_row.get("free"), available_qty)
                held_qty = max(wallet_qty, available_qty, free_qty)
                sellable_candidates = [q for q in (free_qty, available_qty, wallet_qty) if q > 0]
                # Use conservative sellable quantity for exit submissions to reduce insufficient-balance rejects.
                sellable_qty = min(sellable_candidates) if sellable_candidates else 0.0
                qty_constraints = qty_constraints_map.get(f"spot:{symbol}", {})
                min_qty = to_float(qty_constraints.get("min_qty"), 0.0)
                min_notional = to_float(qty_constraints.get("min_notional"), 0.0)
                ticker_row = ticker_maps.get("spot", {}).get(symbol, {})
                ref_price = to_float(ticker_row.get("lastPrice"), 0.0)
                qty_threshold = min_qty if min_qty > 0 else 0.0
                if min_notional > 0 and ref_price > 0:
                    qty_threshold = max(qty_threshold, (min_notional / ref_price))
                if held_qty > qty_threshold:
                    spot_close_candidates.pop(symbol, None)
                    hint_candidates = [
                        spot_entry_hints.get(symbol),
                        positions.get(symbol),
                        live_open_positions_state.get(symbol),
                    ]
                    prior_hint = next(
                        (
                            row
                            for row in hint_candidates
                            if isinstance(row, dict) and is_bot_owned_marker(row)
                        ),
                        {},
                    )
                    if not prior_hint:
                        continue
                    entry_hint = to_float(prior_hint.get("entry"), 0.0)
                    if entry_hint <= 0:
                        entry_hint = to_float(prior_hint.get("price"), 0.0)
                    if entry_hint <= 0:
                        continue
                    live_synced_positions[symbol] = {
                        "entry": entry_hint,
                        "opened_at": str(prior_hint.get("opened_at", now_utc_str())),
                        "qty": held_qty,
                        "sellable_qty": sellable_qty,
                        "source": "LIVE_SYNC_SPOT",
                        "managed_by_bot": True,
                        "updated_at": now_utc_str(),
                    }
                    if is_bot_owned_marker(spot_entry_hints.get(symbol)):
                        spot_entry_hints[symbol]["status"] = "FILLED"
                        spot_entry_hints[symbol]["filled_at"] = now_utc_str()
                elif symbol not in live_synced_pending_entries:
                    # Detect exchange-side close (e.g., native TP/SL filled) and persist journal/performance.
                    prior_live_pos = live_open_positions_state.get(symbol)
                    prior_hint = spot_entry_hints.get(symbol)
                    if isinstance(prior_hint, dict) and is_bot_owned_marker(prior_hint):
                        hint_status = str(prior_hint.get("status", "")).upper()
                        if hint_status != "FILLED":
                            order_id = str(prior_hint.get("order_id", "")).strip()
                            order_link_id = str(prior_hint.get("order_link_id", "")).strip()
                            history_rows: List[Dict[str, Any]] = []
                            try:
                                history_rows = fetch_bybit_order_history_for_symbol(
                                    base_urls=base_urls,
                                    api_key=api_key,
                                    api_secret=api_secret,
                                    recv_window=recv_window,
                                    category="spot",
                                    symbol=symbol,
                                    limit=20,
                                    order_id=order_id,
                                    order_link_id=order_link_id,
                                )
                            except Exception as e:
                                live_sync_errors[symbol] = f"spot history sync failed: {e}"
                                cycle_errors.append(f"[LIVE_SYNC:{symbol}] Spot history error: {e}")
                            history_row = find_matching_order_history_row(
                                rows=history_rows,
                                order_id=order_id,
                                order_link_id=order_link_id,
                            )
                            if history_row:
                                history_status = str(history_row.get("orderStatus", "")).upper()
                                filled_qty = get_bybit_order_filled_qty(history_row)
                                if filled_qty > 0:
                                    prior_hint["status"] = "FILLED"
                                    prior_hint["filled_at"] = now_utc_str()
                                    prior_hint["qty"] = filled_qty
                                    prior_hint["history_status"] = history_status
                                    fill_price = get_bybit_order_fill_price(
                                        history_row,
                                        fallback_price=to_float(prior_hint.get("entry"), 0.0),
                                    )
                                    if fill_price <= 0:
                                        fill_price = to_float(prior_hint.get("price"), 0.0)
                                    if fill_price > 0:
                                        prior_hint["entry"] = fill_price
                                        prior_hint["price"] = fill_price
                                elif history_status in {"CANCELLED", "CANCELED", "REJECTED", "DEACTIVATED"}:
                                    prior_hint["status"] = history_status
                                    prior_hint["history_status"] = history_status

                    close_marker: Dict[str, Any] = {}
                    if isinstance(prior_live_pos, dict) and is_bot_owned_marker(prior_live_pos):
                        close_marker = prior_live_pos
                    elif isinstance(prior_hint, dict) and is_bot_owned_marker(prior_hint):
                        if str(prior_hint.get("status", "")).upper() == "FILLED":
                            close_marker = prior_hint

                    if close_marker:
                        candidate = spot_close_candidates.get(symbol)
                        if not isinstance(candidate, dict):
                            candidate = {
                                "first_seen_at": now_utc_str(),
                                "first_seen_ts": now_utc_ts(),
                                "attempts": 0,
                            }
                            spot_close_candidates[symbol] = candidate
                        candidate["attempts"] = int(to_float(candidate.get("attempts"), 0.0)) + 1

                        entry_price = to_float(close_marker.get("entry"), 0.0)
                        if entry_price <= 0:
                            entry_price = to_float(close_marker.get("price"), 0.0)
                        closed_qty = to_float(close_marker.get("qty"), 0.0)
                        if entry_price > 0 and closed_qty > 0:
                            opened_at_ts = parse_bot_utc_to_ts(close_marker.get("opened_at"))
                            now_ts = now_utc_ts()
                            start_time_ms = 0
                            if opened_at_ts > 0:
                                # Query a bounded window around the tracked position lifecycle.
                                start_time_ms = int(max(0.0, (opened_at_ts - (12 * 60 * 60)) * 1000))
                            end_time_ms = int((now_ts + 120.0) * 1000)

                            execution_rows: List[Dict[str, Any]] = []
                            try:
                                execution_rows = fetch_bybit_execution_history_for_symbol(
                                    base_urls=base_urls,
                                    api_key=api_key,
                                    api_secret=api_secret,
                                    recv_window=recv_window,
                                    category="spot",
                                    symbol=symbol,
                                    limit=100,
                                    start_time_ms=start_time_ms,
                                    end_time_ms=end_time_ms,
                                )
                            except Exception as e:
                                live_sync_errors[symbol] = f"spot execution sync failed: {e}"
                                cycle_errors.append(f"[LIVE_SYNC:{symbol}] Spot execution error: {e}")

                            exit_fill = find_spot_exit_fill_summary(
                                execution_rows=execution_rows,
                                opened_at_ts=opened_at_ts,
                            )
                            close_confirmed = bool(exit_fill)
                            if not close_confirmed:
                                close_confirmed = int(to_float(candidate.get("attempts"), 0.0)) >= 2
                            if not close_confirmed:
                                # Wait for at least one more cycle when close has no explicit fill evidence.
                                continue

                            exit_price = to_float(exit_fill.get("exit_price"), 0.0)
                            pnl_estimated = False
                            exit_signal = "LIVE_SYNC_EXIT_FILL"
                            if exit_price <= 0:
                                ticker_row = ticker_maps.get("spot", {}).get(symbol, {})
                                exit_price = to_float(ticker_row.get("lastPrice"), 0.0)
                                if exit_price <= 0:
                                    exit_price = entry_price
                                pnl_estimated = True
                                exit_signal = "LIVE_SYNC_EXIT_ESTIMATED"

                            pnl = compute_trade_pnl_usdt(
                                entry_price=entry_price,
                                exit_price=exit_price,
                                qty=closed_qty,
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

                            source_note = (
                                str(exit_fill.get("source", "recent_sell_execution"))
                                if exit_signal == "LIVE_SYNC_EXIT_FILL"
                                else "ticker_estimate"
                            )
                            cycle_alerts.append(
                                f"CLOSED {symbol} (LIVE_SYNC) | PnL {pnl:.2f} USDT | "
                                f"Daily PnL {risk_state['daily_realized_pnl_usdt']:.2f} USDT | "
                                f"price_source={source_note}"
                            )
                            closed_at = now_utc_str()
                            closed_at_ts = now_utc_ts()
                            add_closed_trade_to_history(
                                config=config,
                                state=state,
                                trade={
                                    "symbol": symbol,
                                    "source": str(close_marker.get("source", "LIVE_SYNC_SPOT")),
                                    "opened_at": str(close_marker.get("opened_at", "")),
                                    "closed_at": closed_at,
                                    "closed_at_ts": closed_at_ts,
                                    "entry_price": entry_price,
                                    "exit_price": exit_price,
                                    "qty": closed_qty,
                                    "pnl_usdt": pnl,
                                    "net_return_pct": compute_net_return_pct(
                                        entry_price=entry_price,
                                        exit_price=exit_price,
                                        costs_cfg=costs_cfg,
                                    ),
                                    "exit_signal": exit_signal,
                                    "pnl_estimated": pnl_estimated,
                                    "close_confirmed_by_fill": bool(exit_fill),
                                    "close_confirmation_attempts": int(to_float(candidate.get("attempts"), 0.0)),
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
                        spot_close_candidates.pop(symbol, None)
                        positions.pop(symbol, None)
                        spot_entry_hints.pop(symbol, None)
                    elif is_bot_owned_marker(spot_entry_hints.get(symbol)):
                        spot_close_candidates.pop(symbol, None)
                        spot_entry_hints.pop(symbol, None)

        state["live_open_positions"] = live_synced_positions
        state["live_pending_entries"] = live_synced_pending_entries
        state["spot_entry_hints"] = spot_entry_hints
        state["spot_close_candidates"] = spot_close_candidates
    else:
        state["live_open_positions"] = {}
        state["live_pending_entries"] = {}
        state["spot_entry_hints"] = spot_entry_hints
        state["spot_close_candidates"] = spot_close_candidates

    for job in symbol_jobs:
        symbol = job["symbol"]
        category_override = job["category"] or None
        source = job["source"]
        category_for_symbol = normalize_bybit_category(
            category_override or default_bybit_category,
            fallback="linear",
        )
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
            result["market_category"] = category_for_symbol
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
                                        if (
                                            live_sync_required
                                            and normalize_bybit_category(
                                                result.get("market_category"), fallback=""
                                            )
                                            == "spot"
                                        ):
                                            entry_res = exec_result.get("responses", {}).get("entry_order", {})
                                            live_synced_pending_entries[symbol] = {
                                                "order_id": extract_bybit_order_id(entry_res),
                                                "order_link_id": str(
                                                    order_plan.get("entry_order", {}).get("orderLinkId", "")
                                                ),
                                                "price": to_float(result.get("wait_price"), 0.0),
                                                "qty": qty,
                                                "status": "Submitted",
                                                "updated_at": now_utc_str(),
                                            }
                                            spot_entry_hints[symbol] = {
                                                "entry": to_float(result.get("wait_price"), 0.0),
                                                "price": to_float(result.get("wait_price"), 0.0),
                                                "qty": qty,
                                                "opened_at": now_utc_str(),
                                                "status": "PENDING",
                                                "managed_by_bot": True,
                                                "order_id": str(
                                                    live_synced_pending_entries[symbol].get("order_id", "")
                                                ),
                                                "order_link_id": str(
                                                    live_synced_pending_entries[symbol].get("order_link_id", "")
                                                ),
                                            }
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
                                        "managed_by_bot": True,
                                    }
                    elif signal in {"TAKE_PROFIT", "SELL_NOW", "STOP_LOSS"}:
                        pos = (
                            live_synced_positions.get(symbol)
                            if live_sync_required
                            else positions.get(symbol)
                        )
                        if not pos and not live_sync_required:
                            pos = positions.get(symbol)
                        if pos:
                            entry = to_float(pos.get("entry"), 0.0)
                            qty = to_float(pos.get("qty"), 0.0)
                            close_qty = to_float(pos.get("sellable_qty"), qty)
                            if entry > 0 and qty > 0:
                                exit_allowed = True
                                exit_fill: Dict[str, Any] = {}
                                exec_mode = str(exec_ctx.get("mode", "paper")).lower()
                                market_category = normalize_bybit_category(
                                    result.get("market_category"),
                                    fallback=default_bybit_category,
                                )
                                if (
                                    live_sync_required
                                    and exec_mode == "live"
                                    and exchange_name == "bybit"
                                    and market_category == "spot"
                                ):
                                    if symbol in live_sync_errors:
                                        cycle_alerts.append(
                                            f"EXIT BLOCKED {symbol} | Live sync error: {live_sync_errors[symbol]}"
                                        )
                                        continue
                                    if symbol not in live_synced_positions:
                                        cycle_alerts.append(
                                            f"EXIT SKIPPED {symbol} | No synced live spot position "
                                            f"(likely already closed by TP/SL or dust below min order value)."
                                        )
                                        positions.pop(symbol, None)
                                        spot_entry_hints.pop(symbol, None)
                                        spot_close_candidates.pop(symbol, None)
                                        continue
                                if (
                                    exec_mode == "live"
                                    and exchange_name == "bybit"
                                    and market_category == "spot"
                                    and close_qty <= 0
                                ):
                                    cycle_alerts.append(
                                        f"EXIT SKIPPED {symbol} | Sellable balance is zero (waiting for next sync)."
                                    )
                                    continue
                                if (
                                    exec_mode == "live"
                                    and exchange_name == "bybit"
                                    and market_category == "spot"
                                    and not is_bot_owned_marker(pos)
                                ):
                                    cycle_alerts.append(
                                        f"EXIT BLOCKED {symbol} | Spot position not marked as bot-owned."
                                    )
                                    continue
                                if exec_mode == "live" and exchange_name == "bybit" and market_category == "spot":
                                    qty_constraints = qty_constraints_map.get(f"spot:{symbol}", {})
                                    min_notional = to_float((qty_constraints or {}).get("min_notional"), 0.0)
                                    ref_price = to_float(result.get("price"), entry)
                                    exit_plan = build_bybit_spot_exit_order_plan(
                                        symbol=symbol,
                                        qty=close_qty,
                                        order_group_id=build_order_group_id(
                                            symbol=symbol,
                                            close_time=result.get("close_time"),
                                        ),
                                        qty_constraints=qty_constraints,
                                        reference_price=ref_price,
                                    )
                                    if not exit_plan:
                                        qty_step = to_float((qty_constraints or {}).get("qty_step"), 0.0)
                                        min_qty = to_float((qty_constraints or {}).get("min_qty"), 0.0)
                                        max_qty = to_float((qty_constraints or {}).get("max_qty"), 0.0)
                                        adjusted_qty = floor_to_step(close_qty, qty_step) if qty_step > 0 else close_qty
                                        if max_qty > 0:
                                            adjusted_qty = min(adjusted_qty, max_qty)
                                        exit_allowed = False
                                        if min_qty > 0 and adjusted_qty < min_qty:
                                            cycle_alerts.append(
                                                f"EXIT BLOCKED {symbol} | Qty below min order after rounding "
                                                f"({adjusted_qty:.8f} < {min_qty:.8f})."
                                            )
                                        elif min_notional > 0 and ref_price > 0 and (adjusted_qty * ref_price) < min_notional:
                                            cycle_alerts.append(
                                                f"EXIT SKIPPED {symbol} | Position value below min order value "
                                                f"({adjusted_qty * ref_price:.6f} < {min_notional:.6f})."
                                            )
                                            positions.pop(symbol, None)
                                            spot_entry_hints.pop(symbol, None)
                                            spot_close_candidates.pop(symbol, None)
                                        else:
                                            cycle_alerts.append(
                                                f"EXIT BLOCKED {symbol} | Unable to build spot exit plan."
                                            )
                                    else:
                                        planned_exit_qty = to_float(
                                            exit_plan.get("exit_order", {}).get("qty"),
                                            close_qty,
                                        )
                                        if planned_exit_qty > 0:
                                            close_qty = planned_exit_qty
                                        exec_result = execute_bybit_order_plan(
                                            config=config,
                                            exchange_cfg=exchange_cfg,
                                            order_plan=exit_plan,
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
                                                f"EXIT EXECUTION {symbol} | {exec_result.get('message')}"
                                            )
                                            exit_res = exec_result.get("responses", {}).get("exit_order", {})
                                            exit_order_id = extract_bybit_order_id(exit_res)
                                            exit_order_link_id = str(
                                                exit_plan.get("exit_order", {}).get("orderLinkId", "")
                                            ).strip()
                                            opened_at_ts = parse_bot_utc_to_ts(pos.get("opened_at"))
                                            start_time_ms = (
                                                int(max(0.0, (opened_at_ts - (12 * 60 * 60)) * 1000))
                                                if opened_at_ts > 0
                                                else 0
                                            )
                                            end_time_ms = int((now_utc_ts() + 120.0) * 1000)
                                            execution_rows: List[Dict[str, Any]] = []
                                            try:
                                                execution_rows = fetch_bybit_execution_history_for_symbol(
                                                    base_urls=get_bybit_base_urls(exchange_cfg),
                                                    api_key=str(exec_ctx.get("api_key", "")),
                                                    api_secret=str(exec_ctx.get("api_secret", "")),
                                                    recv_window=int(exec_ctx.get("recv_window", 5000)),
                                                    category="spot",
                                                    symbol=symbol,
                                                    limit=100,
                                                    start_time_ms=start_time_ms,
                                                    end_time_ms=end_time_ms,
                                                )
                                            except Exception as e:
                                                cycle_errors.append(
                                                    f"[LIVE_SYNC:{symbol}] Spot execution error (exit): {e}"
                                                )
                                            exit_fill = find_execution_fill_for_order(
                                                execution_rows=execution_rows,
                                                order_id=exit_order_id,
                                                order_link_id=exit_order_link_id,
                                            )
                                            if not exit_fill:
                                                exit_fill = find_spot_exit_fill_summary(
                                                    execution_rows=execution_rows,
                                                    opened_at_ts=opened_at_ts,
                                                )
                                        else:
                                            exit_allowed = False
                                            cycle_alerts.append(
                                                f"EXIT EXECUTION FAILED {symbol} | {exec_result.get('message')}"
                                            )

                                if not exit_allowed:
                                    continue

                                positions.pop(symbol, None)
                                live_synced_positions.pop(symbol, None)
                                live_synced_pending_entries.pop(symbol, None)
                                spot_entry_hints.pop(symbol, None)
                                spot_close_candidates.pop(symbol, None)

                                exit_price = to_float(exit_fill.get("exit_price"), 0.0)
                                pnl_estimated = False
                                if exit_price <= 0:
                                    exit_price = to_float(result.get("price"), entry)
                                    pnl_estimated = True
                                pnl = compute_trade_pnl_usdt(
                                    entry_price=entry,
                                    exit_price=exit_price,
                                    qty=close_qty,
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
                                price_source = (
                                    str(exit_fill.get("source", "exchange_execution"))
                                    if not pnl_estimated
                                    else "strategy_price_estimate"
                                )
                                cycle_alerts.append(
                                    f"CLOSED {symbol} | PnL {pnl:.2f} USDT | "
                                    f"Daily PnL {risk_state['daily_realized_pnl_usdt']:.2f} USDT | "
                                    f"price_source={price_source}"
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
                                        "qty": close_qty,
                                        "pnl_usdt": pnl,
                                        "net_return_pct": compute_net_return_pct(
                                            entry_price=entry,
                                            exit_price=exit_price,
                                            costs_cfg=costs_cfg,
                                        ),
                                        "exit_signal": signal,
                                        "pnl_estimated": pnl_estimated,
                                        "close_confirmed_by_fill": bool(exit_fill),
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

    add_execution_events_to_history(config=config, state=state, execution_events=execution_events)
    execution_events_history = ensure_execution_events_history_state(state)
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
        "execution_events_history": execution_events_history[-50:],
        "performance": performance,
        "recent_closed_trades": recent_closed_trades,
    }


def run_bot(config: Dict[str, Any], run_once: bool = False) -> None:
    ensure_runtime_env()
    state_file = config.get("state_file", DEFAULT_STATE_FILE)
    state = load_persisted_json(state_file, build_default_state(), purpose="state")

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

        save_persisted_json(state_file, state, purpose="state")
        if run_once:
            break
        time.sleep(scan_every_seconds)


def run_single_scan_with_state(config: Dict[str, Any], persist_state: bool = True) -> Dict[str, Any]:
    state_file = str(config.get("state_file", DEFAULT_STATE_FILE))
    state = load_persisted_json(state_file, build_default_state(), purpose="state")
    cycle = scan_once(config=config, state=state)
    state_storage = describe_json_storage_backend(path=state_file, purpose="state")
    if persist_state:
        save_persisted_json(state_file, state, purpose="state")
    cycle["state_file"] = state_file
    cycle["state_persisted"] = bool(persist_state)
    cycle["state_backend"] = state_storage.get("backend", "file")
    if state_storage.get("storage_key"):
        cycle["state_storage_key"] = state_storage.get("storage_key")
    if state_storage.get("table"):
        cycle["state_storage_table"] = state_storage.get("table")
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
    if exchange_name == "bybit":
        raw_category = str(config["exchange"].get("category", "linear")).lower()
        if raw_category not in BYBIT_SUPPORTED_CATEGORIES:
            raise ValueError(
                "exchange.category for Bybit must be one of: "
                + ", ".join(sorted(BYBIT_SUPPORTED_CATEGORIES))
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
    strategy_cfg = config.get("strategy", {})
    regime_cfg = strategy_cfg.get("regime_filter", {})
    if regime_cfg and not isinstance(regime_cfg, dict):
        raise ValueError("strategy.regime_filter must be an object")
    if isinstance(regime_cfg, dict):
        if "enabled" in regime_cfg and not isinstance(regime_cfg.get("enabled"), bool):
            raise ValueError("strategy.regime_filter.enabled must be boolean")
        if "require_uptrend" in regime_cfg and not isinstance(regime_cfg.get("require_uptrend"), bool):
            raise ValueError("strategy.regime_filter.require_uptrend must be boolean")
        if "require_slow_ema_rising" in regime_cfg and not isinstance(
            regime_cfg.get("require_slow_ema_rising"), bool
        ):
            raise ValueError("strategy.regime_filter.require_slow_ema_rising must be boolean")
        for key in ("min_trend_pct", "min_ema_slope_pct"):
            if key in regime_cfg and to_float(regime_cfg.get(key), 0) < 0:
                raise ValueError(f"strategy.regime_filter.{key} must be >= 0")
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
        autoscale_cfg = comp_cfg.get("autoscale", {})
        if autoscale_cfg and not isinstance(autoscale_cfg, dict):
            raise ValueError("risk.compounding.autoscale must be an object")
        if isinstance(autoscale_cfg, dict):
            if "enabled" in autoscale_cfg and not isinstance(autoscale_cfg.get("enabled"), bool):
                raise ValueError("risk.compounding.autoscale.enabled must be boolean")
            if "lookback_days" in autoscale_cfg and int(to_float(autoscale_cfg.get("lookback_days"), 0)) < 0:
                raise ValueError("risk.compounding.autoscale.lookback_days must be >= 0")
            if "min_trades" in autoscale_cfg and int(to_float(autoscale_cfg.get("min_trades"), 0)) < 0:
                raise ValueError("risk.compounding.autoscale.min_trades must be >= 0")
            for key in (
                "min_win_rate_pct",
                "min_profit_factor",
                "max_drawdown_limit_usdt",
            ):
                if key in autoscale_cfg and to_float(autoscale_cfg.get(key), 0) < 0:
                    raise ValueError(f"risk.compounding.autoscale.{key} must be >= 0")
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
    if "max_execution_events" in journal_cfg and int(journal_cfg.get("max_execution_events", 0)) <= 0:
        raise ValueError("journal.max_execution_events must be > 0")
    exec_cfg = config.get("execution", {})
    env_mode = os.getenv("TRADING_BOT_EXECUTION_MODE", "")
    mode = str(env_mode or exec_cfg.get("mode", "paper")).lower()
    if mode not in {"paper", "live"}:
        raise ValueError("execution.mode must be 'paper' or 'live'")
    if mode == "live" and exchange_name != "bybit":
        raise ValueError("execution.mode='live' currently supports only exchange.name='bybit'")
    if mode == "live" and exchange_name == "bybit":
        bybit_category = get_bybit_default_category(config["exchange"])
        if bybit_category != "spot":
            raise ValueError(
                "Live mode is spot-only in this project. Set exchange.category='spot'."
            )
        if bybit_category == "spot" and bool(exec_cfg.get("assume_filled_on_submit", False)):
            raise ValueError(
                "execution.assume_filled_on_submit must be false for Bybit spot live mode."
            )
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
    if "allow_unprotected_spot_entry" in live_safety_cfg and not isinstance(
        live_safety_cfg.get("allow_unprotected_spot_entry"), bool
    ):
        raise ValueError("execution.live_safety.allow_unprotected_spot_entry must be boolean")
    bybit_exec_cfg = exec_cfg.get("bybit", {})
    if "spot_native_tpsl_on_entry" in bybit_exec_cfg and not isinstance(
        bybit_exec_cfg.get("spot_native_tpsl_on_entry"), bool
    ):
        raise ValueError("execution.bybit.spot_native_tpsl_on_entry must be boolean")
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
