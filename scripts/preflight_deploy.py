#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path
from typing import Any, Dict, List, Tuple

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from trading_bot.bot import (  # noqa: E402
    load_env_file,
    parse_env_bool,
    prepare_config_for_runtime,
    validate_config,
)
from trading_bot.state_store import (  # noqa: E402
    describe_json_storage_backend,
    get_database_url,
)


def is_testnet_url(url: str) -> bool:
    return "testnet" in str(url).lower()


def read_json(path: Path) -> Dict[str, Any]:
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def is_set_env(name: str) -> bool:
    return bool(str(os.getenv(name, "")).strip())


def check(condition: bool, ok: str, fail: str, failures: List[str], passes: List[str]) -> None:
    if condition:
        passes.append(ok)
    else:
        failures.append(fail)


def main() -> int:
    parser = argparse.ArgumentParser(description="Trading bot deployment preflight checks")
    parser.add_argument("--config", default="configs/config.json", help="Path to active config JSON")
    parser.add_argument(
        "--target",
        choices=["vercel", "local"],
        default="vercel",
        help="Deployment target profile",
    )
    parser.add_argument(
        "--scheduler",
        choices=["cron-job.org", "vercel", "none"],
        default="cron-job.org",
        help="Expected scheduler source",
    )
    args = parser.parse_args()

    os.chdir(ROOT_DIR)
    load_env_file(str(ROOT_DIR / ".env"), override=False)

    failures: List[str] = []
    passes: List[str] = []
    warnings: List[str] = []

    config_path = (ROOT_DIR / args.config).resolve()
    check(
        config_path.exists(),
        f"Config exists: {config_path.relative_to(ROOT_DIR)}",
        f"Config file missing: {config_path}",
        failures,
        passes,
    )
    if not config_path.exists():
        for msg in failures:
            print(f"[FAIL] {msg}")
        return 1

    config = read_json(config_path)

    try:
        validate_config(config)
        passes.append("Base config schema is valid.")
    except Exception as e:
        failures.append(f"Base config validation failed: {e}")

    runtime_config = prepare_config_for_runtime(config)
    try:
        validate_config(runtime_config)
        passes.append("Runtime-adjusted config is valid.")
    except Exception as e:
        failures.append(f"Runtime config validation failed: {e}")

    exec_cfg = config.get("execution", {})
    mode = str(exec_cfg.get("mode", "paper")).lower()
    runtime_mode = str(runtime_config.get("execution", {}).get("mode", "paper")).lower()
    runtime_exchange_cfg = runtime_config.get("exchange", {})
    runtime_category = str(runtime_exchange_cfg.get("category", "linear")).lower()
    runtime_assume_filled = bool(runtime_config.get("execution", {}).get("assume_filled_on_submit", False))
    runtime_live_safety = runtime_config.get("execution", {}).get("live_safety", {})
    runtime_bybit_cfg = runtime_config.get("execution", {}).get("bybit", {})
    runtime_allow_unprotected_spot = bool(
        runtime_live_safety.get("allow_unprotected_spot_entry", False)
    )
    runtime_spot_native_tpsl = bool(runtime_bybit_cfg.get("spot_native_tpsl_on_entry", True))
    runtime_state_file = str(runtime_config.get("state_file", ""))
    storage_info = describe_json_storage_backend(path=runtime_state_file, purpose="state")
    storage_backend = storage_info.get("backend", "file")
    comp_cfg = config.get("risk", {}).get("compounding", {})
    comp_enabled = bool(comp_cfg.get("enabled", False))
    exchange_cfg = config.get("exchange", {})
    base_url = str(exchange_cfg.get("base_url", ""))
    required_ack = str(
        exec_cfg.get("live_safety", {}).get("required_ack_phrase", "I_UNDERSTAND_LIVE_TRADING_RISK")
    ).strip()

    check(
        ".env" in (ROOT_DIR / ".gitignore").read_text(encoding="utf-8"),
        ".gitignore includes .env",
        ".gitignore must include .env",
        failures,
        passes,
    )

    check(
        "state/bot_state.json" in (ROOT_DIR / ".gitignore").read_text(encoding="utf-8"),
        ".gitignore includes state/bot_state.json",
        ".gitignore should include state/bot_state.json",
        failures,
        passes,
    )

    if args.scheduler == "cron-job.org":
        vercel_cfg = read_json(ROOT_DIR / "vercel.json")
        has_crons = bool(vercel_cfg.get("crons"))
        check(
            not has_crons,
            "vercel.json has no internal crons (external scheduler only).",
            "vercel.json has crons enabled; disable to avoid duplicate scheduler runs.",
            failures,
            passes,
        )

    if args.target == "vercel":
        check(
            parse_env_bool(os.getenv("TRADING_BOT_REQUIRE_SCAN_AUTH"), True),
            "API auth is enabled (/api/scan and /api/status).",
            "TRADING_BOT_REQUIRE_SCAN_AUTH must be true for deployment safety.",
            failures,
            passes,
        )
        check(
            is_set_env("TRADING_BOT_SCAN_TOKEN") or is_set_env("CRON_SECRET"),
            "Scan auth secret is configured.",
            "Set TRADING_BOT_SCAN_TOKEN or CRON_SECRET.",
            failures,
            passes,
        )
        status_token = str(os.getenv("TRADING_BOT_STATUS_TOKEN", "")).strip()
        scan_token = str(
            os.getenv("TRADING_BOT_SCAN_TOKEN", "") or os.getenv("CRON_SECRET", "")
        ).strip()
        check(
            bool(status_token),
            "Status auth secret is configured.",
            "Set TRADING_BOT_STATUS_TOKEN. /api/status requires a dedicated token.",
            failures,
            passes,
        )
        check(
            not (scan_token and status_token and scan_token == status_token),
            "Status token is different from scan token.",
            "TRADING_BOT_STATUS_TOKEN must be different from TRADING_BOT_SCAN_TOKEN/CRON_SECRET.",
            failures,
            passes,
        )
        if storage_backend == "postgres":
            check(
                bool(get_database_url()),
                "PostgreSQL URL is configured for state backend.",
                "State backend is postgres but no NEW_TRADING_BOT_POSTGRES_URL/"
                "TRADING_BOT_POSTGRES_URL/NEON_DATABASE_URL/DATABASE_URL is set.",
                failures,
                passes,
            )
        else:
            warnings.append(
                "State backend is file-based on Vercel. Consider PostgreSQL (Neon) to avoid state loss on cold starts."
            )

    if mode == "live":
        check(
            is_set_env("BYBIT_API_KEY"),
            "BYBIT_API_KEY is set.",
            "Missing BYBIT_API_KEY.",
            failures,
            passes,
        )
        check(
            is_set_env("BYBIT_API_SECRET"),
            "BYBIT_API_SECRET is set.",
            "Missing BYBIT_API_SECRET.",
            failures,
            passes,
        )
        check(
            parse_env_bool(os.getenv("TRADING_BOT_ALLOW_LIVE"), False),
            "TRADING_BOT_ALLOW_LIVE=true",
            "Set TRADING_BOT_ALLOW_LIVE=true for live mode.",
            failures,
            passes,
        )
        check(
            str(os.getenv("TRADING_BOT_LIVE_ACK", "")).strip() == required_ack,
            "TRADING_BOT_LIVE_ACK matches required phrase.",
            "TRADING_BOT_LIVE_ACK does not match required ack phrase.",
            failures,
            passes,
        )
        if not is_testnet_url(base_url):
            check(
                parse_env_bool(os.getenv("TRADING_BOT_ALLOW_MAINNET"), False),
                "TRADING_BOT_ALLOW_MAINNET=true",
                "Set TRADING_BOT_ALLOW_MAINNET=true for mainnet live mode.",
                failures,
                passes,
            )

        if args.target == "vercel":
            check(
                parse_env_bool(os.getenv("TRADING_BOT_ALLOW_LIVE_ON_VERCEL"), False),
                "TRADING_BOT_ALLOW_LIVE_ON_VERCEL=true",
                "Set TRADING_BOT_ALLOW_LIVE_ON_VERCEL=true or mode will be forced to paper.",
                failures,
                passes,
            )
            if runtime_mode != "live":
                failures.append(
                    f"Runtime mode resolves to '{runtime_mode}' on Vercel; expected 'live'."
                )

    if runtime_mode == "live":
        risk_cfg = runtime_config.get("risk", {})
        max_notional = float(risk_cfg.get("max_position_notional_usdt", 0.0))
        if max_notional > 100:
            warnings.append(
                f"max_position_notional_usdt={max_notional:.2f} is high. Confirm this is intentional."
            )
        if runtime_category != "spot":
            failures.append(
                "Live mode must use exchange.category='spot' (derivatives are disabled in this project)."
            )
        if runtime_category == "spot" and runtime_assume_filled:
            failures.append(
                "Spot live mode must set execution.assume_filled_on_submit=false "
                "to avoid fake fills and state drift."
            )
        if runtime_category == "spot" and not runtime_spot_native_tpsl and not runtime_allow_unprotected_spot:
            failures.append(
                "Spot live mode has no native TP/SL-on-entry protection and no explicit risk acknowledgment. "
                "Set execution.bybit.spot_native_tpsl_on_entry=true (recommended), "
                "or set execution.live_safety.allow_unprotected_spot_entry=true only if you accept this risk."
            )
        if args.target == "vercel" and comp_enabled and runtime_state_file.startswith("/tmp/"):
            warnings.append(
                "Compounding is enabled while runtime state file is ephemeral "
                f"({runtime_state_file}). Size may drift after cold starts unless live equity fetch is available."
            )

    print("=== Deployment Preflight ===")
    print(f"Config: {config_path.relative_to(ROOT_DIR)}")
    print(f"Target: {args.target} | Scheduler: {args.scheduler}")
    print(f"Mode: config={mode} runtime={runtime_mode}")
    print(f"State backend: {storage_backend}")
    print()

    for msg in passes:
        print(f"[PASS] {msg}")
    for msg in warnings:
        print(f"[WARN] {msg}")
    for msg in failures:
        print(f"[FAIL] {msg}")

    print()
    if failures:
        print("Preflight status: FAILED")
        return 1
    print("Preflight status: OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
