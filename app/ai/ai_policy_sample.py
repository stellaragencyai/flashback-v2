#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Flashback — Sample AI Policy

This policy is intentionally minimal and safe. Its job is to bootstrap the
AI pipeline in DRY_RUN so we can prove:

ai_pilot -> ai_action_bus -> queue router -> executor -> setup/outcome files

Safety rails:
- Never emits a new entry while snapshot_v2 shows open positions.
- Uses a per-lane cooldown file so it cannot spam entries every poll cycle.
- Emits deterministic test actions only in DRY_RUN by default.
"""

from __future__ import annotations

import json
import os
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

try:
    import yaml
except Exception:  # pragma: no cover
    yaml = None  # type: ignore[assignment]


_MANIFEST_CACHE: Dict[str, Any] = {
    "path": None,
    "mtime_ns": None,
    "defaults": {},
}


def _env_bool(name: str, default: str = "true") -> bool:
    raw = os.getenv(name, default)
    return str(raw).strip().lower() in ("1", "true", "yes", "y", "on")


def _normalize_side(side: Any) -> Optional[str]:
    if side is None:
        return None
    s = str(side).strip().lower()
    if not s:
        return None
    if s in ("buy", "long"):
        return "buy"
    if s in ("sell", "short"):
        return "sell"
    return None


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


def _state_dir() -> Path:
    path = _repo_root() / "state" / "ai_policy_sample"
    path.mkdir(parents=True, exist_ok=True)
    return path


def _now_ms() -> int:
    return int(time.time() * 1000)


def _cooldown_path(label: str) -> Path:
    safe = "".join(ch for ch in label if ch.isalnum() or ch in ("-", "_")) or "unknown"
    return _state_dir() / f"{safe}.json"


def _load_last_emit_ms(label: str) -> int:
    path = _cooldown_path(label)
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        return int(data.get("last_emit_ms") or 0)
    except Exception:
        return 0


def _save_last_emit_ms(label: str, ts_ms: int, symbol: str, side: str) -> None:
    path = _cooldown_path(label)
    payload = {
        "label": label,
        "last_emit_ms": int(ts_ms),
        "symbol": symbol,
        "side": side,
    }
    path.write_text(json.dumps(payload, separators=(",", ":")), encoding="utf-8")


def _fallback_setup_type_for_timeframe(timeframe: str) -> str:
    tf = str(timeframe or "").strip().lower()
    if tf in ("1m", "5m", "15m"):
        return "scalp_liquidity_sweep"
    return "trend_pullback"


def _resolve_lane_defaults(label: str) -> Tuple[str, str, str]:
    fallback_symbol = os.getenv("AI_PILOT_SAMPLE_TEST_SYMBOL", "BTCUSDT").strip().upper() or "BTCUSDT"
    fallback_tf = os.getenv("AI_PILOT_DEFAULT_TIMEFRAME", "5m").strip() or "5m"
    fallback_setup_type = os.getenv("AI_PILOT_SAMPLE_TEST_SETUP_TYPE", "").strip() or _fallback_setup_type_for_timeframe(fallback_tf)

    manifest_path = _repo_root() / "config" / "fleet_manifest.yaml"
    try:
        if yaml is None:
            return fallback_symbol, fallback_tf, fallback_setup_type
        try:
            mtime_ns = int(manifest_path.stat().st_mtime_ns)
        except Exception:
            mtime_ns = 0

        if (
            _MANIFEST_CACHE.get("path") != str(manifest_path)
            or _MANIFEST_CACHE.get("mtime_ns") != mtime_ns
        ):
            payload = yaml.safe_load(manifest_path.read_text(encoding="utf-8")) or {}
            defaults: Dict[str, Tuple[str, str, str]] = {}
            fleet = payload.get("fleet") or []
            if isinstance(fleet, list):
                for row in fleet:
                    if not isinstance(row, dict):
                        continue
                    row_label = str(row.get("account_label") or "").strip()
                    if not row_label:
                        continue

                    symbols = row.get("symbols") or []
                    timeframes = row.get("timeframes") or []
                    setup_types = row.get("setup_types") or []

                    symbol = (
                        str(symbols[0]).strip().upper()
                        if isinstance(symbols, list) and symbols and str(symbols[0]).strip()
                        else fallback_symbol
                    )
                    timeframe = (
                        str(timeframes[0]).strip()
                        if isinstance(timeframes, list) and timeframes and str(timeframes[0]).strip()
                        else fallback_tf
                    )
                    setup_type = (
                        str(setup_types[0]).strip()
                        if isinstance(setup_types, list) and setup_types and str(setup_types[0]).strip()
                        else fallback_setup_type
                    )
                    if timeframe.isdigit():
                        timeframe = f"{timeframe}m"
                    defaults[row_label] = (symbol, timeframe, setup_type)
            _MANIFEST_CACHE["path"] = str(manifest_path)
            _MANIFEST_CACHE["mtime_ns"] = mtime_ns
            _MANIFEST_CACHE["defaults"] = defaults

        cached = _MANIFEST_CACHE.get("defaults", {}).get(label)
        if cached:
            return cached
    except Exception:
        pass

    return fallback_symbol, fallback_tf, fallback_setup_type


def evaluate_state(ai_state: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Emit simple sample actions for ai_pilot._run_sample_policy().

    Behavior:
    - If the snapshot reports open positions, emit one action per position.
    - If there are no positions and we are in DRY_RUN, emit one deterministic
      forced test action by default so the rest of the pipeline can be verified.
    """
    actions: List[Dict[str, Any]] = []

    label = str(ai_state.get("label", "unknown") or "unknown")
    dry_run = bool(ai_state.get("dry_run", True))

    snap = ai_state.get("snapshot_v2") or {}
    pos_block = snap.get("positions") or {}
    pos_map = pos_block.get("by_symbol") or {}

    if isinstance(pos_map, dict):
        for symbol, row in pos_map.items():
            if not isinstance(row, dict):
                continue

            sym = str(symbol).upper().strip()
            if not sym:
                continue

            side_raw = row.get("side") or row.get("positionSide")
            size_raw = row.get("size") or row.get("qty")

            side = _normalize_side(side_raw)
            if side is None:
                continue

            try:
                if size_raw is not None and float(size_raw) == 0.0:
                    continue
            except Exception:
                pass

            # If a position already exists, sample policy should stay quiet.
            return []

    force_action = _env_bool("AI_PILOT_SAMPLE_FORCE_ACTION", "true")
    cooldown_sec = int(os.getenv("AI_PILOT_SAMPLE_COOLDOWN_SEC", "300") or "300")
    symbol, timeframe, setup_type = _resolve_lane_defaults(label)
    side = os.getenv("AI_PILOT_SAMPLE_TEST_SIDE", "buy").strip().lower() or "buy"
    confidence = float(os.getenv("AI_PILOT_SAMPLE_TEST_CONF", "0.60") or "0.60")

    if dry_run and force_action and not actions:
        now_ms = _now_ms()
        last_emit_ms = _load_last_emit_ms(label)
        if last_emit_ms > 0 and (now_ms - last_emit_ms) < max(0, cooldown_sec) * 1000:
            return []

        _save_last_emit_ms(label, now_ms, symbol, side)
        actions.append(
            {
                "symbol": symbol,
                "side": side,
                "confidence": confidence,
                "reason": "sample_policy_forced_test_action",
                "timeframe": timeframe,
                "setup_type": setup_type,
                "trade_id": f"SAMPLE_{label}_{now_ms}_{symbol}_{side}".replace(" ", ""),
            }
        )

    return actions
