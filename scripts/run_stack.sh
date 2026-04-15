#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CONFIG_PATH="${1:-configs/config.json}"
UI_HOST="${UI_HOST:-127.0.0.1}"
UI_PORT="${UI_PORT:-8501}"
LOG_DIR="${ROOT_DIR}/runtime_logs"

cd "${ROOT_DIR}"

if ! command -v python3 >/dev/null 2>&1; then
  echo "python3 not found in PATH. Install Python first."
  exit 1
fi

if [ ! -f "${CONFIG_PATH}" ]; then
  echo "Config file not found: ${CONFIG_PATH}"
  echo "Usage: ./scripts/run_stack.sh [configs/config.json]"
  exit 1
fi

if ! python3 -c "import streamlit" >/dev/null 2>&1; then
  echo "Streamlit is not installed for this python3 environment."
  echo "Run: python3 -m pip install -r requirements.txt streamlit"
  exit 1
fi

mkdir -p "${LOG_DIR}"
BOT_LOG="${LOG_DIR}/bot-$(date +%Y%m%d-%H%M%S).log"
BOT_PID=""

cleanup() {
  if [ -n "${BOT_PID}" ] && kill -0 "${BOT_PID}" >/dev/null 2>&1; then
    echo
    echo "Stopping bot (pid ${BOT_PID})..."
    kill "${BOT_PID}" >/dev/null 2>&1 || true
    wait "${BOT_PID}" 2>/dev/null || true
  fi
}

trap cleanup EXIT INT TERM

echo "Starting bot with config: ${CONFIG_PATH}"
echo "Bot log: ${BOT_LOG}"
python3 -m trading_bot --config "${CONFIG_PATH}" >"${BOT_LOG}" 2>&1 &
BOT_PID="$!"

sleep 1
if ! kill -0 "${BOT_PID}" >/dev/null 2>&1; then
  echo "Bot failed to stay running. Check log: ${BOT_LOG}"
  exit 1
fi

echo "Starting Streamlit UI at http://${UI_HOST}:${UI_PORT}"
echo "Press Ctrl+C to stop UI and bot together."
python3 -m streamlit run apps/streamlit_app.py --server.address "${UI_HOST}" --server.port "${UI_PORT}"
