#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Flashback — Notifier Bot (Restored API Surface)

This module MUST remain import-safe and lightweight.

Contract (do not break):
- get_notifier(channel: Optional[str] = None) -> object with .send(text, level="info", channel=None)
- tg_send(text: str, channel: Optional[str] = None, level: str = "info") -> None

Behavior:
- If Telegram env vars are missing, falls back to logging only.
- Supports per-subaccount channels via env var naming:
    TG_<CHANNEL>_BOT_TOKEN / TG_<CHANNEL>_CHAT_ID
  Example:
    TG_MAIN_BOT_TOKEN, TG_MAIN_CHAT_ID
    TG_FLASHBACK01_BOT_TOKEN, TG_FLASHBACK01_CHAT_ID
- Also supports legacy/common:
    TG_BOT_TOKEN / TG_CHAT_ID
"""

from __future__ import annotations

import os
import time
import json
import logging
import urllib.parse
import urllib.request
from dataclasses import dataclass
from typing import Optional, Dict, Any, Tuple


# -------------------------
# Logger (import-safe)
# -------------------------

def _get_logger() -> logging.Logger:
    try:
        from app.core.logger import get_logger  # type: ignore
        return get_logger("notifier_bot")
    except Exception:
        lg = logging.getLogger("notifier_bot")
        if not lg.handlers:
            h = logging.StreamHandler()
            fmt = logging.Formatter("%(asctime)s [%(levelname)s] [%(name)s] %(message)s")
            h.setFormatter(fmt)
            lg.addHandler(h)
        lg.setLevel(logging.INFO)
        return lg


LOG = _get_logger()


# -------------------------
# Env helpers
# -------------------------

def _upper_channel(channel: Optional[str]) -> str:
    if not channel:
        return "MAIN"
    return str(channel).strip().upper()


def _env(name: str, default: str = "") -> str:
    v = os.getenv(name, default)
    return str(v).strip()


def _env_int(name: str, default: int) -> int:
    try:
        return int(_env(name, str(default)))
    except Exception:
        return int(default)


def _resolve_tg_creds(channel: Optional[str]) -> Tuple[str, str, str]:
    """
    Returns (token, chat_id, source)
    """
    ch = _upper_channel(channel)

    # per-channel preferred
    token = _env(f"TG_{ch}_BOT_TOKEN")
    chat = _env(f"TG_{ch}_CHAT_ID")
    if token and chat:
        return token, chat, f"TG_{ch}_BOT_TOKEN/TG_{ch}_CHAT_ID"

    # common / legacy fallback
    token2 = _env("TG_BOT_TOKEN")
    chat2 = _env("TG_CHAT_ID")
    if token2 and chat2:
        return token2, chat2, "TG_BOT_TOKEN/TG_CHAT_ID"

    return "", "", "missing"


# -------------------------
# Telegram sender
# -------------------------

def _http_post(url: str, payload: Dict[str, Any], timeout_sec: int) -> None:
    data = urllib.parse.urlencode(payload).encode("utf-8")
    req = urllib.request.Request(url, data=data, method="POST")
    with urllib.request.urlopen(req, timeout=timeout_sec) as resp:
        _ = resp.read()


def tg_send(text: str, channel: Optional[str] = None, level: str = "info") -> None:
    """
    Public API. Safe to call from anywhere.
    """
    token, chat_id, source = _resolve_tg_creds(channel)

    # Always log locally (useful even if TG fails)
    lvl = str(level or "info").strip().lower()
    prefix = {"info": "INFO", "warn": "WARN", "warning": "WARN", "error": "ERROR"}.get(lvl, "INFO")
    LOG.info("[TG:%s:%s] %s", _upper_channel(channel), prefix, text)

    if not token or not chat_id:
        # quiet fallback, avoid spam
        return

    timeout = _env_int("TG_HTTP_TIMEOUT_SEC", 6)
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    payload = {
        "chat_id": chat_id,
        "text": text,
        "disable_web_page_preview": True,
    }

    try:
        _http_post(url, payload, timeout_sec=timeout)
    except Exception as e:
        LOG.warning("tg_send failed (%s via %s): %s", _upper_channel(channel), source, e)


# -------------------------
# Notifier object
# -------------------------

@dataclass
class _Notifier:
    default_channel: Optional[str] = None

    def send(self, text: str, level: str = "info", channel: Optional[str] = None) -> None:
        ch = channel if channel is not None else self.default_channel
        tg_send(text, channel=ch, level=level)


# -------------------------
# Public factory (required)
# -------------------------

def get_notifier(channel: Optional[str] = None) -> _Notifier:
    """
    Public API expected by other bots.
    """
    return _Notifier(default_channel=channel)


# -------------------------
# Compatibility aliases (some scripts use different names)
# -------------------------

def send_tg(text: str, channel: Optional[str] = None, level: str = "info") -> None:
    tg_send(text, channel=channel, level=level)


def notify(text: str, channel: Optional[str] = None, level: str = "info") -> None:
    tg_send(text, channel=channel, level=level)


if __name__ == "__main__":
    # tiny self-test (won't crash if TG isn't configured)
    tg_send("notifier_bot self-test ✅", channel=os.getenv("ACCOUNT_LABEL", "main"), level="info")
