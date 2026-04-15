#!/usr/bin/env python3
"""Vercel Python entrypoint shim.

Vercel's Python detector can require an entrypoint like `api/index.py`.
We keep the existing scan logic in `api/scan.py` and expose the same handler here.
"""

from __future__ import annotations

from scan import handler as ScanHandler


class handler(ScanHandler):
    pass

