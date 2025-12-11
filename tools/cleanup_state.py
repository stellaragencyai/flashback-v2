#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Flashback — State & Log Cleanup Utility

Purpose
-------
Single CLI tool to keep the repo lean and the runtime state under control.

Functions:
    - Truncate large JSONL streams (public trades, AI events).
    - Clear paper-trading snapshots.
    - Clear logs.
    - Dry-run mode to see what *would* be done.

Usage examples:
    python tools/cleanup_state.py --truncate-public-trades 200000
    python tools/cleanup_state.py --prune-ai-events 100000
    python tools/cleanup_state.py --clear-paper --clear-logs
    python tools/cleanup_state.py --truncate-public-trades 100000 --prune-ai-events 50000 --dry-run

This script is intentionally conservative: it only touches known paths under the project root.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Iterable


# ---------------------------------------------------------------------------
# Root resolution
# ---------------------------------------------------------------------------

HERE = Path(__file__).resolve()
ROOT = HERE.parents[1]  # C:\Flashback
STATE_DIR = ROOT / "state"
LOGS_DIR = ROOT / "logs"

AI_EVENTS_DIR = STATE_DIR / "ai_events"
PAPER_DIR = STATE_DIR / "paper"
PUBLIC_TRADES_FILE = STATE_DIR / "public_trades.jsonl"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def log(msg: str) -> None:
    print(msg, file=sys.stdout)


def iter_jsonl_tail(path: Path, max_lines: int) -> Iterable[str]:
    """
    Read only the last `max_lines` lines from a potentially huge JSONL file.

    This is a simple, memory-aware tail implementation:
        - Reads from the end backward in chunks.
        - Not super-optimized, but fine for occasional maintenance.
    """
    if max_lines <= 0:
        return []

    # Fast path: if file is small, just read all.
    with path.open("rb") as f:
        f.seek(0, 2)
        file_size = f.tell()

    # If file is small (< 5 MB), just read whole file.
    if file_size < 5 * 1024 * 1024:
        with path.open("r", encoding="utf-8", errors="ignore") as f:
            lines = f.readlines()
        return lines[-max_lines:]

    # Otherwise, do a crude backward scan
    chunk_size = 8192
    buffer = b""
    lines = []

    with path.open("rb") as f:
        f.seek(0, 2)
        position = f.tell()

        while position > 0 and len(lines) <= max_lines:
            read_size = min(chunk_size, position)
            position -= read_size
            f.seek(position)
            chunk = f.read(read_size)
            buffer = chunk + buffer
            parts = buffer.split(b"\n")

            # Keep the first (possibly partial) chunk in buffer
            buffer = parts[0]
            lines_chunk = parts[1:]
            # Prepend new lines (we're moving backwards)
            lines = [ln for ln in lines_chunk] + lines

        if buffer:
            lines = [buffer] + lines

    # Convert to str and trim
    decoded_lines = [ln.decode("utf-8", errors="ignore") for ln in lines]
    return decoded_lines[-max_lines:]


def truncate_jsonl_file(path: Path, max_lines: int, dry_run: bool = False) -> None:
    if not path.exists():
        log(f"[INFO] File not found, skipping: {path}")
        return

    log(f"[INFO] Truncating JSONL file: {path}")
    try:
        with path.open("r", encoding="utf-8", errors="ignore") as f:
            line_count = sum(1 for _ in f)
    except Exception as exc:
        log(f"[WARN] Could not count lines in {path}: {exc}")
        return

    if line_count <= max_lines:
        log(f"[INFO] {path.name}: {line_count} ≤ {max_lines}, nothing to do.")
        return

    log(f"[INFO] {path.name}: {line_count} lines → will keep last {max_lines}.")

    if dry_run:
        log(f"[DRY-RUN] Would rewrite {path} with last {max_lines} lines.")
        return

    tail_lines = iter_jsonl_tail(path, max_lines=max_lines)
    tmp_path = path.with_suffix(path.suffix + ".tmp")

    try:
        with tmp_path.open("w", encoding="utf-8") as out:
            out.writelines(tail_lines)

        tmp_path.replace(path)
        log(f"[OK] Truncated {path.name} to last {len(tail_lines)} lines.")
    except Exception as exc:
        if tmp_path.exists():
            tmp_path.unlink(missing_ok=True)
        log(f"[ERROR] Failed to truncate {path}: {exc}")


def remove_tree(path: Path, dry_run: bool = False) -> None:
    if not path.exists():
        log(f"[INFO] Path does not exist, skipping: {path}")
        return

    if not path.is_dir():
        log(f"[WARN] Not a directory (skipping): {path}")
        return

    if dry_run:
        log(f"[DRY-RUN] Would delete directory tree: {path}")
        return

    # Manual recursive delete is more explicit than shutil.rmtree
    for sub in sorted(path.rglob("*"), reverse=True):
        try:
            if sub.is_file() or sub.is_symlink():
                sub.unlink(missing_ok=True)
            elif sub.is_dir():
                sub.rmdir()
        except Exception as exc:
            log(f"[WARN] Failed to delete {sub}: {exc}")
    try:
        path.rmdir()
    except Exception:
        # Might already be gone
        pass

    log(f"[OK] Deleted directory tree: {path}")


# ---------------------------------------------------------------------------
# Main logic
# ---------------------------------------------------------------------------

def run_cleanup(
    truncate_public_trades: int | None,
    prune_ai_events: int | None,
    clear_paper: bool,
    clear_logs: bool,
    dry_run: bool,
) -> int:
    log(f"[INFO] Project root: {ROOT}")
    log(f"[INFO] Dry run: {'YES' if dry_run else 'NO'}")

    # 1) public_trades.jsonl truncation
    if truncate_public_trades is not None:
        if truncate_public_trades <= 0:
            log("[WARN] --truncate-public-trades must be > 0, skipping.")
        else:
            truncate_jsonl_file(PUBLIC_TRADES_FILE, truncate_public_trades, dry_run=dry_run)

    # 2) AI events pruning
    if prune_ai_events is not None:
        if prune_ai_events <= 0:
            log("[WARN] --prune-ai-events must be > 0, skipping.")
        else:
            if not AI_EVENTS_DIR.exists():
                log(f"[INFO] AI events dir does not exist, skipping: {AI_EVENTS_DIR}")
            else:
                for path in sorted(AI_EVENTS_DIR.glob("*.jsonl")):
                    truncate_jsonl_file(path, prune_ai_events, dry_run=dry_run)

    # 3) Clear paper snapshots
    if clear_paper:
        if not PAPER_DIR.exists():
            log(f"[INFO] Paper dir does not exist, skipping: {PAPER_DIR}")
        else:
            if dry_run:
                log(f"[DRY-RUN] Would clear all files under: {PAPER_DIR}")
            else:
                for item in sorted(PAPER_DIR.glob("*")):
                    try:
                        if item.is_file() or item.is_symlink():
                            item.unlink(missing_ok=True)
                        elif item.is_dir():
                            remove_tree(item, dry_run=False)
                    except Exception as exc:
                        log(f"[WARN] Could not delete {item}: {exc}")
                log(f"[OK] Cleared paper state under: {PAPER_DIR}")

    # 4) Clear logs
    if clear_logs:
        if not LOGS_DIR.exists():
            log(f"[INFO] Logs dir does not exist, skipping: {LOGS_DIR}")
        else:
            remove_tree(LOGS_DIR, dry_run=dry_run)

    log("[INFO] Cleanup completed.")
    return 0


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Flashback state/log cleanup utility.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--truncate-public-trades",
        type=int,
        default=None,
        help="Keep only the last N lines in state/public_trades.jsonl.",
    )
    parser.add_argument(
        "--prune-ai-events",
        type=int,
        default=None,
        help="Keep only the last N lines in each state/ai_events/*.jsonl file.",
    )
    parser.add_argument(
        "--clear-paper",
        action="store_true",
        help="Delete all paper-trading state files under state/paper/.",
    )
    parser.add_argument(
        "--clear-logs",
        action="store_true",
        help="Delete the entire logs/ directory tree.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be done without modifying any files.",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_arg_parser()
    args = parser.parse_args(argv)

    return run_cleanup(
        truncate_public_trades=args.truncate_public_trades,
        prune_ai_events=args.prune_ai_events,
        clear_paper=args.clear_paper,
        clear_logs=args.clear_logs,
        dry_run=args.dry_run,
    )


if __name__ == "__main__":
    raise SystemExit(main())
