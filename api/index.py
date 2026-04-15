#!/usr/bin/env python3
"""Vercel Python entrypoint shim.

Vercel's Python detector can require an entrypoint like `api/index.py`.
We keep the existing scan logic in `api/scan.py` and expose the same handler here.
"""

from __future__ import annotations

import sys
from pathlib import Path

# Ensure sibling module `scan.py` is importable in Vercel runtime.
API_DIR = Path(__file__).resolve().parent
if str(API_DIR) not in sys.path:
    sys.path.insert(0, str(API_DIR))

from scan import handler as ScanHandler


class handler(ScanHandler):
    pass
