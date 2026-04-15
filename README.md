# Free Crypto Alert Bot

This bot scans crypto pairs and sends simple trade alerts:
- `LIMIT BUY NOW`
- `SELL NOW`
- `TAKE PROFIT NOW`
- `STOP LOSS NOW`

It uses **free public Bybit candle data** (default endpoint: `https://api.bybit.com`) and can optionally send alerts to Telegram.

## Project Structure

Clean structure:

```text
api/
  scan.py              # Vercel serverless scan endpoint (/api/scan)
apps/
  crypto_alert_bot.py  # bot CLI entrypoint
  ui_dashboard.py      # Streamlit dashboard entrypoint
  streamlit_app.py     # deploy-friendly Streamlit entrypoint
configs/
  config.json
  config.example.json
  presets/
    paper.conservative.json
    paper.balanced.json
    paper.aggressive.json
    live.aggressive.json
scripts/
  run_stack.sh         # run bot + UI together
state/
  bot_state.json       # runtime state (generated/updated)
trading_bot/
  bot.py             # strategy scan + execution engine
  bybit_client.py    # Bybit HTTP/signing helpers
  ui_dashboard.py    # Streamlit UI logic
  __main__.py        # python -m trading_bot
vercel.json          # Vercel function runtime config
```

## What it does

- Reads candles for each symbol in your config
- Uses EMA crossover + RSI filter to detect entries
- Tracks local paper positions in `state/bot_state.json` so TP/SL alerts can trigger
- Prints ranked symbols so you can see which coin has strongest momentum now
- Shows a clear action per coin: `BUY_LIMIT`, `WAIT_PULLBACK`, `WAIT`, `HOLD`, `SELL`
- Shows a `WAIT_AT` price so you know where to place/watch entry
- Shows `TP` (limit-sell target) and `SL` (stop-loss) for each suggested entry
- Can auto-add the best Bybit spot coin each cycle (`spot_discovery`)
- Adds risk-based position sizing (0.5%-1% style risk per trade)
- Includes circuit breakers (max daily loss and max consecutive losses)
- Adds loss cooldown and max-open-position protection before new entries
- Adds spread + 24h turnover liquidity filter before accepting entries
- Uses fee/slippage-adjusted TP/SL decision logic for more realistic paper results
- Auto-journals closed trades in local state and computes performance metrics
- Auto-checks Bybit min order qty/step and blocks invalid order sizes with guidance
- Auto-checks Bybit tick size and rounds entry/TP/SL to valid price increments
- Shows Bybit-ready bracket payload fields (`reduceOnly`, `closeOnTrigger`) for derivatives
- Adds idempotent `orderLinkId` and rollback safety if bracket legs fail mid-submit
- In live mode (`assume_filled_on_submit=false`), syncs open positions + open entry orders from Bybit to avoid duplicate entries

## Setup

For daily use, you only need:
- `configs/config.json` (active config)
- `.env` (secrets and live safety flags)

1. Copy config:
```bash
cp configs/config.example.json configs/config.json
```
2. Create env file (recommended for secrets):
```bash
cp .env.example .env
```
3. Edit `configs/config.json`:
- Set your symbols
- Optional auto-discovery of best Bybit spot coin:
  - `spot_discovery.enabled: true` to auto-add
  - `add_count`: how many spot coins to add per cycle
  - `min_turnover_usdt`: liquidity filter
  - `min_price_change_pct`: momentum filter (decimal form, `0.05` = +5%)
  - `fallback_to_best`: if no coin passes momentum filter, still pick best liquid spot coin
- Tune strategy numbers
- Risk controls (`risk`):
  - `account_equity_usdt`: account size used for sizing
  - `risk_per_trade_pct`: risk budget per trade (for qty calc)
  - `max_position_notional_usdt`: hard cap on position value to avoid oversized qty on cheap coins
  - `max_daily_loss_pct`: circuit breaker daily loss limit
  - `max_consecutive_losses`: circuit breaker loss streak limit
  - `max_open_positions`: cap simultaneous open positions
  - `cooldown_minutes_after_loss`: pause new entries after a losing close
  - `pause_on_limit`: pause new entries when circuit breaker is hit
- Execution costs (`execution_costs`):
  - `entry_fee_pct`, `exit_fee_pct`
  - `entry_slippage_pct`, `exit_slippage_pct`
  - used in TP/SL sell decisions and realized PnL calculations
- Liquidity filter (`liquidity_filter`):
  - `enabled`
  - `max_spread_pct`
  - `min_turnover_24h_usdt`
  - `block_when_ticker_missing`
- Price filter (`price_filter`):
  - `enabled`
  - `max_price_usdt` (example: `0.1` to allow only <= $0.10 tokens)
  - `apply_to_watchlist`
  - `apply_to_spot_discovery`
- Journal (`journal`):
  - `enabled`: store closed trades in `state/bot_state.json`
  - `max_closed_trades`: history retention cap (older rows are trimmed)
- Execution controls (`execution`):
  - `mode`: `paper` or `live`
  - `assume_filled_on_submit`:
    - `true`: treat successful submission as locally filled
    - `false`: use live Bybit position/order sync (safer for real execution)
  - `live_safety`:
    - `require_manual_unlock`: require env unlock before live execution
    - `required_ack_phrase`: exact phrase that must be present in env
    - `require_mainnet_flag`: require a separate env flag before live mainnet orders
  - `bybit.api_key` / `bybit.api_secret`: optional (env is recommended instead)
- Exchange options:
  - Bybit (default): `"name": "bybit", "base_url": "https://api.bytick.com", "backup_base_urls": ["https://api.bybit.com"], "category": "linear"`
  - Binance: `"name": "binance", "base_url": "https://data-api.binance.vision"`
- (Optional) enable Telegram with `bot_token` and `chat_id`
4. Run:
```bash
python3 apps/crypto_alert_bot.py --config configs/config.json
```

Preferred package entrypoint:
```bash
python3 -m trading_bot --config configs/config.json
```

Single scan test:
```bash
python3 apps/crypto_alert_bot.py --config configs/config.json --once
```

## Daily Run (One Command)

Start bot + UI together:

```bash
./scripts/run_stack.sh
```

With a specific config:

```bash
./scripts/run_stack.sh configs/presets/paper.balanced.json
```

Behavior:
- Bot runs in background and writes logs to `runtime_logs/`
- Streamlit UI runs in foreground at `http://127.0.0.1:8501`
- `Ctrl+C` stops both UI and bot cleanly

## Risk Profiles

Ready-to-use presets are included:
- `configs/presets/paper.conservative.json`
- `configs/presets/paper.balanced.json`
- `configs/presets/paper.aggressive.json`
- `configs/presets/live.aggressive.json`

Quick switch:
```bash
cp configs/presets/paper.conservative.json configs/config.json
# or
cp configs/presets/paper.balanced.json configs/config.json
# or
cp configs/presets/paper.aggressive.json configs/config.json
# or (real money)
cp configs/presets/live.aggressive.json configs/config.json
```

Profile idea:
- Conservative: strict liquidity, smaller risk, longer cooldown
- Balanced: default profile (good starting point)
- Aggressive: wider entries, higher risk, shorter cooldown

### TLS / Certificate Issues

If your network returns SSL errors for `api.bybit.com` (hostname mismatch or handshake failure), the bot now retries Bybit backup endpoints automatically. Keep both:

```json
"exchange": {
  "name": "bybit",
  "base_url": "https://api.bytick.com",
  "backup_base_urls": ["https://api.bybit.com"],
  "category": "linear"
}
```

If both endpoints fail on your network, use another network/VPN or temporarily switch to Binance for paper-only signal testing.

## Environment Variables (Safe Defaults)

The bot auto-loads `.env` at startup (if present). This keeps secrets out of `configs/config.json`.

Minimum live vars:
```env
BYBIT_API_KEY=...
BYBIT_API_SECRET=...
TRADING_BOT_ALLOW_LIVE=true
TRADING_BOT_LIVE_ACK=I_UNDERSTAND_LIVE_TRADING_RISK
TRADING_BOT_ALLOW_MAINNET=true
```

Safety behavior in `execution.mode = "live"`:
- If `TRADING_BOT_ALLOW_LIVE` is not true, live submissions are blocked.
- If `TRADING_BOT_LIVE_ACK` does not exactly match `required_ack_phrase`, live submissions are blocked.
- If using mainnet URL and `TRADING_BOT_ALLOW_MAINNET` is not true, live submissions are blocked.

Optional env overrides:
- `TRADING_BOT_EXECUTION_MODE=paper|live`
- `TELEGRAM_BOT_TOKEN`
- `TELEGRAM_CHAT_ID`

## UI Dashboard

Run a browser UI for the same strategy:

```bash
pip install streamlit
python3 -m streamlit run apps/ui_dashboard.py
```

In the UI you can:
- Run scans with one click
- See `BUY_LIMIT / WAIT / SELL` actions in a table
- See suggested entry (`ENTRY`) with `TP` and `SL`
- See risk-based qty and risk budget
- See spread/turnover quality checks and net return %
- See auto performance metrics (overall and last 7 days)
- See a recent closed-trades journal table
- Preview Bybit order payloads for derivatives
- View alerts and network/API errors

## Deploy (Streamlit Cloud)

Fastest way to deploy and access from any device:

1. Push this project to GitHub.
2. Go to Streamlit Community Cloud and click **New app**.
3. Select your repo/branch and set the app file to:
   - `apps/streamlit_app.py`
4. Deploy.

### Secrets (for live execution only)

If you use `execution.mode = "live"`, add these in Streamlit Cloud **Secrets**:

```toml
BYBIT_API_KEY = "your_key"
BYBIT_API_SECRET = "your_secret"
TRADING_BOT_ALLOW_LIVE = "true"
TRADING_BOT_LIVE_ACK = "I_UNDERSTAND_LIVE_TRADING_RISK"
TRADING_BOT_ALLOW_MAINNET = "true"
```

Local template:
- `.streamlit/secrets.toml.example`

### Important cloud note

- `state/bot_state.json` is local file state. On cloud restarts/redeploys, this state may reset.
- For persistent production state, move state to a DB/KV store.

## Deploy (Vercel Safe Mode)

Vercel is good for API + scheduled scans, not for a 24/7 bot loop process.

This repo includes:
- `api/scan.py` as a serverless scan endpoint
- `vercel.json` for function runtime only (scheduler handled by cron-job.org)
- Runtime safety override on Vercel:
  - `execution.mode=live` is forced to `paper` unless `TRADING_BOT_ALLOW_LIVE_ON_VERCEL=true`
  - state file defaults to `/tmp/trading_bot_state.json` on Vercel

### Deploy steps

1. Import repo to Vercel.
2. Keep root directory as project root.
3. Add environment variables:
   - `TRADING_BOT_REQUIRE_SCAN_AUTH=true`
   - `TRADING_BOT_SCAN_TOKEN=<strong-random-token>` (or use `CRON_SECRET`)
   - Keep safety default: `TRADING_BOT_ALLOW_LIVE_ON_VERCEL=false` (set `true` only if you want live orders)
4. Deploy.

### If you intentionally want LIVE orders on Vercel

Set all of these environment variables in Vercel Project Settings:

- `BYBIT_API_KEY=<your-live-key>`
- `BYBIT_API_SECRET=<your-live-secret>`
- `TRADING_BOT_ALLOW_LIVE=true`
- `TRADING_BOT_LIVE_ACK=I_UNDERSTAND_LIVE_TRADING_RISK`
- `TRADING_BOT_ALLOW_MAINNET=true`
- `TRADING_BOT_ALLOW_LIVE_ON_VERCEL=true`

### Trigger scan manually

Use bearer token auth:

```bash
curl -sS "https://<your-app>.vercel.app/api/scan?config=configs/config.json" \
  -H "Authorization: Bearer <TRADING_BOT_SCAN_TOKEN>"
```

Token query param is also supported:

```bash
curl -sS "https://<your-app>.vercel.app/api/scan?config=configs/config.json&token=<TRADING_BOT_SCAN_TOKEN>"
```

### Use cron-job.org instead of Vercel Cron

You can use `cron-job.org` as the external scheduler and keep Vercel only as API hosting.

1. Create a cron-job.org job with URL:
   - `https://<your-app>.vercel.app/api/scan?config=configs/config.json&token=<TRADING_BOT_SCAN_TOKEN>`
2. Set schedule (for example every 15 minutes).
3. Save and run a manual test in cron-job.org.
4. This repo keeps scheduling external on purpose (cron-job.org).
   - If you later re-enable Vercel `crons`, keep only one scheduler to avoid duplicate scans.

### Preflight before deploy

Run one command to verify safety checks:

```bash
python3 scripts/preflight_deploy.py --config configs/config.json --target vercel --scheduler cron-job.org
```

It validates:
- live safety env flags
- API auth secret for `/api/scan`
- scheduler mismatch (cron-job.org vs vercel crons)
- config/runtime validation

### Notes for Vercel plans

- This setup uses cron-job.org for scheduling, so Vercel cron plan limits do not apply.

## Notes

- Default mode is **paper** execution. Use live mode only after paper testing.
- Live order execution is currently supported only when `exchange.name` is `bybit`.
- Signals are heuristic and can be wrong, especially in choppy markets.
- Test on paper or very small size first.
- In live mode, derivative order payload uses `reduceOnly` and `closeOnTrigger` on exits.
- Live mode now includes an explicit env safety lock and mainnet lock.
- Paper exits now account for configured fee/slippage assumptions.
- If `assume_filled_on_submit` is `false`, the bot now syncs live open positions and pending entry orders from Bybit each cycle.
