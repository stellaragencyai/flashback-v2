#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Flashback — Subaccount Registry & Round-Robin (v2, YAML-driven)

Purpose
-------
Central place to understand which logical accounts exist, what they are called,
and how to rotate through them for DRIP / profit distribution / canary tests.

Primary source of truth:
    config/subaccounts.yaml

This file declares entries like:

    - account_label: flashback01
      sub_uid: 524630315
      role: trend_follow
      strategy_name: Sub1_Trend
      enabled: true
      telegram_channel: sub1
      risk_profile: default_sub
      enable_tp_sl: true
      enable_journal: true
      enable_ai_stack: true
      ai_profile: trend_v1
      automation_mode: LEARN_DRY
      notes: ...

Public API
----------
all_subs() -> List[dict]
    Each dict contains at least:
        {
          "uid": str | None,           # sub_uid or None for main
          "label": str,                # human-friendly label (strategy_name or account_label)
          "account_label": str,        # "main", "flashback01", ...
          "enabled": bool,
          "role": str | "",
          "strategy_name": str | "",
          "risk_profile": str | None,
          "ai_profile": str | None,
          "automation_mode": str | None,
          "telegram_channel": str | None,
          "enable_tp_sl": bool,
          "enable_journal": bool,
          "enable_ai_stack": bool,
        }

rr_next() -> dict | None
    Round-robin next subaccount:
        {
          "uid": str,
          "label": str,
          "account_label": str,
        }

peek_current() -> dict | None
reset_rr() -> None

Backward compatibility
----------------------
If config/subaccounts.yaml is missing or invalid, we fall back to the older
env-based behavior:

    SUB_UID_1..SUB_UID_10       -> individual sub UIDs
    SUB_UIDS_ROUND_ROBIN        -> explicit rotation list
    SUB_LABELS                  -> "uid:Label,uid:Label,..."
"""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import List, Dict, Optional, Any

# ---------------------------------------------------------------------------
# ROOT / paths
# ---------------------------------------------------------------------------

try:
    from app.core.config import settings  # type: ignore
    ROOT: Path = getattr(settings, "ROOT", Path(__file__).resolve().parents[2])
except Exception:
    ROOT = Path(__file__).resolve().parents[2]

STATE_DIR = ROOT / "state"
STATE_DIR.mkdir(parents=True, exist_ok=True)

CONFIG_DIR = ROOT / "config"
SUBACCOUNTS_PATH = CONFIG_DIR / "subaccounts.yaml"

RR_STATE_PATH = STATE_DIR / "subs_rr.json"

# YAML loader (tolerant)
try:
    import yaml  # type: ignore
except Exception:  # pragma: no cover
    yaml = None  # type: ignore


# ---------------------------------------------------------------------------
# YAML-based registry
# ---------------------------------------------------------------------------

def _load_subaccounts_yaml() -> Dict[str, Any]:
    """
    Load config/subaccounts.yaml if present, else {}.
    """
    if yaml is None:
        return {}
    try:
        if not SUBACCOUNTS_PATH.exists():
            return {}
        raw = SUBACCOUNTS_PATH.read_text(encoding="utf-8")
        data = yaml.safe_load(raw) or {}
        if not isinstance(data, dict):
            return {}
        return data
    except Exception:
        return {}


def _normalize_account_entry(entry: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """
    Normalize a single account entry from subaccounts.yaml.

    Ensures consistent keys and types. Returns None if invalid.
    """
    try:
        account_label = str(entry.get("account_label") or "").strip()
        if not account_label:
            return None

        # sub_uid: may be int, str, or None
        sub_uid_raw = entry.get("sub_uid", None)
        uid: Optional[str]
        if sub_uid_raw is None:
            uid = None
        else:
            uid = str(sub_uid_raw).strip() or None

        # use strategy_name if available, else account_label as "label"
        strategy_name = str(entry.get("strategy_name") or "").strip()
        label = strategy_name or account_label

        enabled = bool(entry.get("enabled", True))

        role = str(entry.get("role") or "").strip()
        risk_profile = entry.get("risk_profile")
        ai_profile = entry.get("ai_profile")
        automation_mode = entry.get("automation_mode")
        telegram_channel = entry.get("telegram_channel")

        enable_tp_sl = bool(entry.get("enable_tp_sl", True))
        enable_journal = bool(entry.get("enable_journal", True))
        enable_ai_stack = bool(entry.get("enable_ai_stack", False))

        return {
            "uid": uid,
            "label": label,
            "account_label": account_label,
            "enabled": enabled,
            "role": role,
            "strategy_name": strategy_name,
            "risk_profile": risk_profile,
            "ai_profile": ai_profile,
            "automation_mode": automation_mode,
            "telegram_channel": telegram_channel,
            "enable_tp_sl": enable_tp_sl,
            "enable_journal": enable_journal,
            "enable_ai_stack": enable_ai_stack,
            # keep original raw entry in case future code needs extra fields
            "_raw": dict(entry),
        }
    except Exception:
        return None


def _yaml_accounts_list() -> List[Dict[str, Any]]:
    """
    Return the normalized list of accounts from YAML.

    If YAML is missing or invalid, returns [].
    """
    cfg = _load_subaccounts_yaml()
    accounts = cfg.get("accounts") or []
    if not isinstance(accounts, list):
        return []

    out: List[Dict[str, Any]] = []
    for entry in accounts:
        if not isinstance(entry, dict):
            continue
        norm = _normalize_account_entry(entry)
        if norm is not None:
            out.append(norm)
    return out


_YAML_ACCOUNTS: List[Dict[str, Any]] = _yaml_accounts_list()


# ---------------------------------------------------------------------------
# Legacy env-based registry (fallback)
# ---------------------------------------------------------------------------

_env = os.environ

# Collect SUB_UID_1..SUB_UID_10 (if present)
_sub_uids_raw: List[str] = []
for i in range(1, 11):
    val = _env.get(f"SUB_UID_{i}")
    if val:
        val = val.strip()
        if val:
            _sub_uids_raw.append(val)

# Fallback/explicit rotation list
_rr_env = _env.get("SUB_UIDS_ROUND_ROBIN", "")
_rr_uids: List[str] = [x.strip() for x in _rr_env.split(",") if x.strip()]

# If no explicit RR list, use SUB_UID_1..N
if not _rr_uids and _sub_uids_raw:
    _rr_uids = list(_sub_uids_raw)

# Label mapping from SUB_LABELS env
# Example:
#   SUB_LABELS=524630315:Sub1_Trend,524633243:Sub2_Breakout,...
_labels_raw = _env.get("SUB_LABELS", "")

_label_map: Dict[str, str] = {}
if _labels_raw:
    for chunk in _labels_raw.split(","):
        chunk = chunk.strip()
        if not chunk or ":" not in chunk:
            continue
        uid_str, label = chunk.split(":", 1)
        uid_str = uid_str.strip()
        label = label.strip()
        if not uid_str or not label:
            continue
        _label_map[uid_str] = label


def _legacy_label_for_uid(uid: str) -> str:
    """
    Legacy helper: get label from SUB_LABELS or fall back to "sub-<uid>".
    """
    uid_s = str(uid)
    return _label_map.get(uid_s, f"sub-{uid_s}")


# ---------------------------------------------------------------------------
# Round-robin state helpers
# ---------------------------------------------------------------------------

def _load_rr_state() -> Dict[str, Any]:
    try:
        if RR_STATE_PATH.exists():
            return json.loads(RR_STATE_PATH.read_text(encoding="utf-8"))
    except Exception:
        pass
    return {"index": 0}


def _save_rr_state(st: Dict[str, Any]) -> None:
    RR_STATE_PATH.write_text(
        json.dumps(st, separators=(",", ":"), ensure_ascii=False),
        encoding="utf-8",
    )


# ---------------------------------------------------------------------------
# Public API: registry
# ---------------------------------------------------------------------------

def all_subs() -> List[Dict[str, Any]]:
    """
    Return a list of all known subaccounts.

    Preferred source: config/subaccounts.yaml.
    Fallback: SUB_UID_1..N + SUB_LABELS env.

    For YAML entries, main is included (uid=None), others have uid=str(sub_uid).
    """
    # Preferred: YAML registry
    if _YAML_ACCOUNTS:
        return list(_YAML_ACCOUNTS)

    # Fallback: env-based logic
    uids = _sub_uids_raw or _rr_uids
    subs: List[Dict[str, Any]] = []
    for uid in uids:
        subs.append(
            {
                "uid": uid,
                "label": _legacy_label_for_uid(uid),
                "account_label": _legacy_label_for_uid(uid),
                "enabled": True,
                "role": "",
                "strategy_name": "",
                "risk_profile": None,
                "ai_profile": None,
                "automation_mode": None,
                "telegram_channel": None,
                "enable_tp_sl": True,
                "enable_journal": True,
                "enable_ai_stack": False,
                "_raw": {},
            }
        )
    return subs


def get_sub_by_label(account_label: str) -> Optional[Dict[str, Any]]:
    """
    Return a single subaccount dict by its account_label ("main", "flashback01", ...).
    """
    label_norm = str(account_label or "").strip()
    if not label_norm:
        return None
    for sub in all_subs():
        if sub.get("account_label") == label_norm:
            return sub
    return None


def get_sub_by_uid(uid: str) -> Optional[Dict[str, Any]]:
    """
    Return a subaccount dict by its sub_uid (stringified).
    """
    uid_norm = str(uid or "").strip()
    if not uid_norm:
        return None
    for sub in all_subs():
        if sub.get("uid") == uid_norm:
            return sub
    return None


# ---------------------------------------------------------------------------
# Public API: round-robin
# ---------------------------------------------------------------------------

def _rr_candidates() -> List[Dict[str, Any]]:
    """
    Internal helper: build the rotation list.

    Priority:
        1) YAML accounts: all entries with sub_uid != None and enabled=True.
        2) Legacy env: SUB_UIDS_ROUND_ROBIN or SUB_UID_1..N.
    """
    if _YAML_ACCOUNTS:
        out: List[Dict[str, Any]] = []
        for acct in _YAML_ACCOUNTS:
            uid = acct.get("uid")
            if uid is None:
                # main has no sub_uid; skip it for round-robin DRIP
                continue
            if not acct.get("enabled", True):
                continue
            out.append(
                {
                    "uid": uid,
                    "label": str(acct.get("label") or acct.get("account_label")),
                    "account_label": str(acct.get("account_label")),
                }
            )
        return out

    # Legacy env-based behavior
    uids = _rr_uids or _sub_uids_raw
    out: List[Dict[str, Any]] = []
    for uid in uids:
        out.append(
            {
                "uid": uid,
                "label": _legacy_label_for_uid(uid),
                "account_label": _legacy_label_for_uid(uid),
            }
        )
    return out


def rr_next() -> Optional[Dict[str, str]]:
    """
    Get the next sub in the round-robin rotation and advance the pointer.

    Returns:
        {"uid": <str>, "label": <str>, "account_label": <str>}
        or None if no subs configured.
    """
    rr = _rr_candidates()
    if not rr:
        return None

    st = _load_rr_state()
    idx = int(st.get("index", 0))
    if idx < 0 or idx >= len(rr):
        idx = 0

    sub = rr[idx]

    # advance index
    idx = (idx + 1) % len(rr)
    st["index"] = idx
    _save_rr_state(st)

    # ensure types: everything stringified
    return {
        "uid": str(sub.get("uid")),
        "label": str(sub.get("label")),
        "account_label": str(sub.get("account_label")),
    }


def peek_current() -> Optional[Dict[str, str]]:
    """
    Peek at the current RR sub without advancing.

    Returns:
        {"uid": <str>, "label": <str>, "account_label": <str>}
        or None if no subs configured.
    """
    rr = _rr_candidates()
    if not rr:
        return None

    st = _load_rr_state()
    idx = int(st.get("index", 0))
    if idx < 0 or idx >= len(rr):
        idx = 0

    sub = rr[idx]
    return {
        "uid": str(sub.get("uid")),
        "label": str(sub.get("label")),
        "account_label": str(sub.get("account_label")),
    }


def reset_rr() -> None:
    """
    Reset the round-robin index to 0.
    Useful in tests or when re-seeding rotation.
    """
    _save_rr_state({"index": 0})
