#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Flashback — Log Maintenance Utility

Purpose
-------
Keep logs/ from exploding:
  - Truncate oversized logs
  - Delete very old logs

Usage examples:
  python tools/log_maintenance.py
  python tools/log_maintenance.py --max-size-mb 50 --max-age-days 14
  python tools/log_maintenance.py --dry-run

NOTE:
  - Intended to be run periodically, ideally when bots are not under heavy load.
  - It is conservative: it only touches files under logs/.
"""

from __future__ import annotations

import argparse
import os
import time
from pathlib import Path
from typing import List, Tuple


ROOT = Path(__file__).resolve().parents[1]
LOGS_DIR = ROOT / "logs"


def human_size(num_bytes: int) -> str:
    units = ["B", "KB", "MB", "GB"]
    size = float(num_bytes)
    for u in units:
        if size < 1024.0:
            return f"{size:.1f}{u}"
        size /= 1024.0
    return f"{size:.1f}TB"


def collect_logs() -> List[Path]:
    if not LOGS_DIR.exists():
        return []
    out: List[Path] = []
    for root, _dirs, files in os.walk(LOGS_DIR):
        for name in files:
            p = Path(root) / name
            if p.is_file():
                out.append(p)
    return out


def process_logs(max_size_mb: float, max_age_days: float, dry_run: bool) -> Tuple[int, int]:
    now = time.time()
    max_size_bytes = max_size_mb * 1024 * 1024
    max_age_seconds = max_age_days * 86400

    truncated = 0
    deleted = 0

    logs = collect_logs()
    if not logs:
        print(f"[INFO] No logs found under {LOGS_DIR}")
        return truncated, deleted

    print(f"[INFO] Found {len(logs)} log file(s) under {LOGS_DIR}")

    for path in logs:
        try:
            st = path.stat()
        except FileNotFoundError:
            continue

        size = st.st_size
        age = now - st.st_mtime

        # Delete if too old
        if age > max_age_seconds:
            if dry_run:
                print(f"[DRY] Would delete OLD log: {path} (age={age/86400:.1f} days)")
            else:
                print(f"[DEL] Deleting OLD log: {path} (age={age/86400:.1f} days)")
                try:
                    path.unlink()
                except Exception as exc:
                    print(f"[WARN] Failed to delete {path}: {exc}")
                else:
                    deleted += 1
            continue

        # Truncate if too large
        if size > max_size_bytes:
            if dry_run:
                print(f"[DRY] Would TRUNCATE large log: {path} (size={human_size(size)})")
            else:
                print(f"[TRUNCATE] Truncating log: {path} (size={human_size(size)})")
                try:
                    with path.open("w", encoding="utf-8"):
                        # Opening in 'w' mode truncates the file
                        pass
                except Exception as exc:
                    print(f"[WARN] Failed to truncate {path}: {exc}")
                else:
                    truncated += 1

    return truncated, deleted


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(description="Flashback log maintenance tool.")
    ap.add_argument("--max-size-mb", type=float, default=25.0,
                    help="Max allowed log file size before truncation (MB). Default: 25")
    ap.add_argument("--max-age-days", type=float, default=7.0,
                    help="Max age before deletion (days). Default: 7")
    ap.add_argument("--dry-run", action="store_true",
                    help="Show what would be done without modifying files.")
    return ap.parse_args()


def main() -> int:
    args = parse_args()

    print("=== Flashback Log Maintenance ===")
    print(f"ROOT:      {ROOT}")
    print(f"LOGS_DIR:  {LOGS_DIR}")
    print(f"max_size:  {args.max_size_mb} MB")
    print(f"max_age:   {args.max_age_days} days")
    print(f"dry_run:   {args.dry_run}")
    print("")

    truncated, deleted = process_logs(
        max_size_mb=args.max_size_mb,
        max_age_days=args.max_age_days,
        dry_run=args.dry_run,
    )

    print("")
    print(f"[SUMMARY] Truncated: {truncated} files, Deleted: {deleted} files")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
