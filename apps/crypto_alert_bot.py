#!/usr/bin/env python3
"""Bot CLI entrypoint."""

from pathlib import Path
import sys

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from trading_bot.bot import *  # noqa: F401,F403
from trading_bot.bot import main


if __name__ == "__main__":
    main()
