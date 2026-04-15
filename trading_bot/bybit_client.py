#!/usr/bin/env python3
from __future__ import annotations

import hashlib
import hmac
import json
import time
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

BYBIT_DEFAULT_BASE_URLS = [
    "https://api.bybit.com",
    "https://api.bytick.com",
]


def to_float(value: Any, fallback: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return fallback


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


def now_utc_str() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")


def get_bybit_base_urls(exchange_cfg: Dict[str, Any]) -> List[str]:
    base_url = str(exchange_cfg.get("base_url", "")).strip()
    backup_urls = exchange_cfg.get("backup_base_urls", [])
    candidates: List[str] = []
    if base_url:
        candidates.append(base_url)
    if isinstance(backup_urls, list):
        candidates.extend(str(url) for url in backup_urls)
    candidates.extend(BYBIT_DEFAULT_BASE_URLS)
    return dedupe_urls(candidates)


def fetch_bybit_instrument_constraints(
    base_urls: List[str],
    category: str,
    symbol: str,
) -> Dict[str, float]:
    params = urllib.parse.urlencode({"category": category, "symbol": symbol})
    payload: Optional[Dict[str, Any]] = None
    errors: List[str] = []
    for base_url in dedupe_urls(base_urls):
        url = f"{base_url.rstrip('/')}/v5/market/instruments-info?{params}"
        req = urllib.request.Request(url, headers={"User-Agent": "crypto-alert-bot/1.0"})
        try:
            with urllib.request.urlopen(req, timeout=20) as res:
                payload = json.loads(res.read().decode("utf-8"))
            break
        except Exception as e:
            errors.append(f"{base_url}: {e}")

    if payload is None:
        raise RuntimeError(
            f"All Bybit endpoints failed for instruments-info ({symbol}): " + " | ".join(errors)
        )
    if payload.get("retCode") != 0:
        raise ValueError(
            f"Bybit instruments-info error for {symbol}: retCode={payload.get('retCode')} "
            f"retMsg={payload.get('retMsg')}"
        )

    rows = payload.get("result", {}).get("list", [])
    if not rows:
        raise ValueError(f"No instruments-info rows for {symbol} ({category})")
    row = rows[0]
    lot = row.get("lotSizeFilter", {})
    price_filter = row.get("priceFilter", {})
    status = str(row.get("status", "")).strip()
    qty_step = to_float(lot.get("qtyStep"), 0.0)
    if qty_step <= 0:
        # Spot instruments can expose basePrecision without qtyStep.
        qty_step = to_float(lot.get("basePrecision"), 0.0)
    if qty_step <= 0:
        # Last-resort fallback: minOrderQty still gives a valid floor granularity.
        qty_step = to_float(lot.get("minOrderQty"), 0.0)
    return {
        "min_qty": to_float(lot.get("minOrderQty"), 0.0),
        "max_qty": to_float(lot.get("maxOrderQty"), 0.0),
        "qty_step": qty_step,
        "tick_size": to_float(price_filter.get("tickSize"), 0.0),
        "status": status,
        "tradable": status.upper() == "TRADING",
    }


def fetch_bybit_tickers(base_urls: List[str], category: str = "spot") -> List[Dict[str, Any]]:
    params = urllib.parse.urlencode({"category": category})
    payload: Optional[Dict[str, Any]] = None
    errors: List[str] = []
    for base_url in dedupe_urls(base_urls):
        url = f"{base_url.rstrip('/')}/v5/market/tickers?{params}"
        req = urllib.request.Request(url, headers={"User-Agent": "crypto-alert-bot/1.0"})
        try:
            with urllib.request.urlopen(req, timeout=20) as res:
                payload = json.loads(res.read().decode("utf-8"))
            break
        except Exception as e:
            errors.append(f"{base_url}: {e}")
    if payload is None:
        raise RuntimeError(
            f"All Bybit endpoints failed for tickers ({category}): " + " | ".join(errors)
        )

    if payload.get("retCode") != 0:
        raise ValueError(
            f"Bybit ticker API error: retCode={payload.get('retCode')} "
            f"retMsg={payload.get('retMsg')}"
        )
    return payload.get("result", {}).get("list", [])


def build_json_compact(payload: Dict[str, Any]) -> str:
    return json.dumps(payload, separators=(",", ":"), sort_keys=True)


def bybit_signed_post(
    base_url: str,
    path: str,
    payload: Dict[str, Any],
    api_key: str,
    api_secret: str,
    recv_window: int = 5000,
) -> Dict[str, Any]:
    timestamp = str(int(time.time() * 1000))
    body = build_json_compact(payload)
    sign_payload = f"{timestamp}{api_key}{recv_window}{body}"
    signature = hmac.new(
        api_secret.encode("utf-8"),
        sign_payload.encode("utf-8"),
        hashlib.sha256,
    ).hexdigest()
    headers = {
        "Content-Type": "application/json",
        "X-BAPI-API-KEY": api_key,
        "X-BAPI-SIGN": signature,
        "X-BAPI-SIGN-TYPE": "2",
        "X-BAPI-TIMESTAMP": timestamp,
        "X-BAPI-RECV-WINDOW": str(recv_window),
        "User-Agent": "crypto-alert-bot/1.0",
    }
    url = f"{base_url.rstrip('/')}{path}"
    req = urllib.request.Request(
        url,
        data=body.encode("utf-8"),
        headers=headers,
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=20) as res:
        return json.loads(res.read().decode("utf-8"))


def bybit_signed_post_with_fallback(
    base_urls: List[str],
    path: str,
    payload: Dict[str, Any],
    api_key: str,
    api_secret: str,
    recv_window: int = 5000,
) -> Dict[str, Any]:
    errors: List[str] = []
    for base_url in base_urls:
        try:
            return bybit_signed_post(
                base_url=base_url,
                path=path,
                payload=payload,
                api_key=api_key,
                api_secret=api_secret,
                recv_window=recv_window,
            )
        except Exception as e:
            errors.append(f"{base_url}: {e}")
    raise RuntimeError("All Bybit endpoints failed for signed POST: " + " | ".join(errors))


def bybit_signed_get(
    base_url: str,
    path: str,
    params: Dict[str, Any],
    api_key: str,
    api_secret: str,
    recv_window: int = 5000,
) -> Dict[str, Any]:
    timestamp = str(int(time.time() * 1000))
    query_items = [(k, str(v)) for k, v in params.items() if v is not None and str(v) != ""]
    query_items.sort(key=lambda x: x[0])
    query = urllib.parse.urlencode(query_items)
    sign_payload = f"{timestamp}{api_key}{recv_window}{query}"
    signature = hmac.new(
        api_secret.encode("utf-8"),
        sign_payload.encode("utf-8"),
        hashlib.sha256,
    ).hexdigest()
    headers = {
        "X-BAPI-API-KEY": api_key,
        "X-BAPI-SIGN": signature,
        "X-BAPI-SIGN-TYPE": "2",
        "X-BAPI-TIMESTAMP": timestamp,
        "X-BAPI-RECV-WINDOW": str(recv_window),
        "User-Agent": "crypto-alert-bot/1.0",
    }
    url = f"{base_url.rstrip('/')}{path}"
    if query:
        url = f"{url}?{query}"
    req = urllib.request.Request(url, headers=headers, method="GET")
    with urllib.request.urlopen(req, timeout=20) as res:
        return json.loads(res.read().decode("utf-8"))


def bybit_signed_get_with_fallback(
    base_urls: List[str],
    path: str,
    params: Dict[str, Any],
    api_key: str,
    api_secret: str,
    recv_window: int = 5000,
) -> Dict[str, Any]:
    errors: List[str] = []
    for base_url in base_urls:
        try:
            return bybit_signed_get(
                base_url=base_url,
                path=path,
                params=params,
                api_key=api_key,
                api_secret=api_secret,
                recv_window=recv_window,
            )
        except Exception as e:
            errors.append(f"{base_url}: {e}")
    raise RuntimeError("All Bybit endpoints failed for signed GET: " + " | ".join(errors))


def is_bybit_duplicate_order_response(response: Dict[str, Any]) -> bool:
    msg = str(response.get("retMsg", "")).lower()
    if "duplicate" in msg and "order" in msg:
        return True
    ret_code = int(to_float(response.get("retCode"), 0))
    return ret_code in {10014}


def extract_bybit_order_id(response: Dict[str, Any]) -> str:
    result = response.get("result", {})
    return str(result.get("orderId", "")).strip()


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
    payload: Dict[str, Any] = {
        "category": category,
        "symbol": symbol,
    }
    if order_id:
        payload["orderId"] = order_id
    elif order_link_id:
        payload["orderLinkId"] = order_link_id
    else:
        raise ValueError("cancel_bybit_order requires order_id or order_link_id")
    return bybit_signed_post_with_fallback(
        base_urls=base_urls,
        path="/v5/order/cancel",
        payload=payload,
        api_key=api_key,
        api_secret=api_secret,
        recv_window=recv_window,
    )


def is_active_open_order_status(status: str) -> bool:
    normalized = str(status).strip()
    return normalized in {"New", "PartiallyFilled", "Untriggered"}


def fetch_bybit_open_orders_for_symbol(
    base_urls: List[str],
    api_key: str,
    api_secret: str,
    recv_window: int,
    category: str,
    symbol: str,
) -> List[Dict[str, Any]]:
    payload = bybit_signed_get_with_fallback(
        base_urls=base_urls,
        path="/v5/order/realtime",
        params={
            "category": category,
            "symbol": symbol,
            "openOnly": 0,
            "limit": 50,
        },
        api_key=api_key,
        api_secret=api_secret,
        recv_window=recv_window,
    )
    if payload.get("retCode") != 0:
        raise ValueError(
            f"Bybit realtime order error for {symbol}: retCode={payload.get('retCode')} "
            f"retMsg={payload.get('retMsg')}"
        )
    rows = payload.get("result", {}).get("list", [])
    return [row for row in rows if is_active_open_order_status(row.get("orderStatus", ""))]


def fetch_bybit_live_position_for_symbol(
    base_urls: List[str],
    api_key: str,
    api_secret: str,
    recv_window: int,
    category: str,
    symbol: str,
) -> Optional[Dict[str, Any]]:
    payload = bybit_signed_get_with_fallback(
        base_urls=base_urls,
        path="/v5/position/list",
        params={
            "category": category,
            "symbol": symbol,
        },
        api_key=api_key,
        api_secret=api_secret,
        recv_window=recv_window,
    )
    if payload.get("retCode") != 0:
        raise ValueError(
            f"Bybit position error for {symbol}: retCode={payload.get('retCode')} "
            f"retMsg={payload.get('retMsg')}"
        )
    rows = payload.get("result", {}).get("list", [])
    for row in rows:
        if str(row.get("symbol", "")).upper() != symbol.upper():
            continue
        size = abs(to_float(row.get("size"), 0.0))
        entry_price = to_float(row.get("avgPrice"), 0.0)
        if size <= 0 or entry_price <= 0:
            continue
        return {
            "entry": entry_price,
            "opened_at": now_utc_str(),
            "qty": size,
            "tp_price": to_float(row.get("takeProfit"), 0.0),
            "sl_price": to_float(row.get("stopLoss"), 0.0),
            "source": "LIVE_SYNC",
            "updated_at": now_utc_str(),
        }
    return None
