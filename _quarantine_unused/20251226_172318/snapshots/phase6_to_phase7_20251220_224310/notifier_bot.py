#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Flashback — Telegram notifier hub (Phase-7 import-safe)

Key guarantees:
- NO filesystem I/O at import time (no .env load at import).
- NO network calls at import time.
- Env values are resolved lazily at notifier build-time (first get_notifier()).
- Optional, controlled init logging (default OFF).

Responsibilities:
- One TelegramNotifier per logical "channel" (main, journal, drip, subaccounts, etc.).
- Centralized rate limiting (per process, per channel).
- Simple .info() / .warn() / .error() / .trade() methods.
- Backwards-compatible tg_send(text, channel="main", level="info") helper for old code.
"""

from __future__ import annotations

import os
import time
import threading
from collections import deque
from dataclasses import dataclass
from typing import Deque, Dict, Optional, Tuple

import requests

from app.core.logger import get_logger

log = get_logger("notifier_bot")

# ---------------------------------------------------------------------------
# Config (import-safe)
# ---------------------------------------------------------------------------

HTTP_TIMEOUT = float(os.getenv("TG_HTTP_TIMEOUT", "6"))

# Global (per-process) send caps default
DEFAULT_MAX_30S = int(os.getenv("TG_MAX_MSGS_PER_30S", "10"))
DEFAULT_MAX_300S = int(os.getenv("TG_MAX_MSGS_PER_300S", "80"))

# Optional: log init details when a notifier is constructed (default OFF)
TG_LOG_INIT = os.getenv("TG_LOG_INIT", "false").strip().lower() in ("1", "true", "yes", "y", "on")

# Optional: allow on-demand .env loading when first building a notifier (default ON)
TG_LOAD_DOTENV_ON_DEMAND = os.getenv("TG_LOAD_DOTENV_ON_DEMAND", "true").strip().lower() in (
    "1",
    "true",
    "yes",
    "y",
    "on",
)

_ENV_LOADED = False
_ENV_LOCK = threading.Lock()


def _load_dotenv_once() -> None:
    """
    Load .env exactly once, and only on-demand (NOT at import time).

    Phase-7 note:
    - This is *allowed* because it's explicit runtime behavior (triggered by get_notifier/build).
    - Import sweeps should not accidentally perform filesystem reads.
    """
    global _ENV_LOADED
    if _ENV_LOADED:
        return
    if not TG_LOAD_DOTENV_ON_DEMAND:
        _ENV_LOADED = True
        return

    with _ENV_LOCK:
        if _ENV_LOADED:
            return
        try:
            from pathlib import Path
            from dotenv import load_dotenv  # type: ignore
        except Exception:
            _ENV_LOADED = True
            return

        try:
            root = Path(__file__).resolve().parents[2]
            env_path = root / ".env"
            if env_path.exists():
                load_dotenv(env_path)
        except Exception:
            # Never let env loading kill the process
            pass
        finally:
            _ENV_LOADED = True


# ---------------------------------------------------------------------------
# TelegramNotifier implementation
# ---------------------------------------------------------------------------

@dataclass
class TelegramNotifier:
    channel: str
    token: str
    chat_id: str
    level: str = "info"
    max_30s: int = DEFAULT_MAX_30S
    max_300s: int = DEFAULT_MAX_300S
    prefix: str = ""

    def __post_init__(self) -> None:
        self._lock = threading.Lock()
        self._times_30s: Deque[float] = deque(maxlen=512)
        self._times_300s: Deque[float] = deque(maxlen=2048)

        if TG_LOG_INIT:
            token_present = bool(self.token)
            token_prefix = (self.token[:8] + "...") if token_present else ""
            log.info(
                "[TG:init] channel='%s', token_present=%s, token_prefix=%s, chat_id='%s', "
                "level=%s, max_30s=%s, max_300s=%s, prefix='%s'",
                self.channel,
                token_present,
                token_prefix,
                self.chat_id or "",
                self.level,
                self.max_30s,
                self.max_300s,
                self.prefix,
            )

    # ---------- public API ----------

    def info(self, msg: str) -> None:
        self._send("INFO", msg)

    def warn(self, msg: str) -> None:
        self._send("WARN", f"⚠️ {msg}")

    def error(self, msg: str) -> None:
        self._send("ERROR", f"❌ {msg}")

    def trade(self, msg: str) -> None:
        self._send("TRADE", f"💹 {msg}")

    def debug(self, msg: str) -> None:
        self._send("DEBUG", msg)

    # ---------- internal helpers ----------

    def _should_send(self) -> bool:
        """
        Per-notifier, per-process rate limit:
          - at most max_30s in sliding 30s window
          - at most max_300s in sliding 300s window
        """
        if not self.token or not self.chat_id:
            return False

        now = time.time()
        with self._lock:
            while self._times_30s and now - self._times_30s[0] > 30.0:
                self._times_30s.popleft()
            while self._times_300s and now - self._times_300s[0] > 300.0:
                self._times_300s.popleft()

            if self.max_30s > 0 and len(self._times_30s) >= self.max_30s:
                log.debug(
                    "[TG:%s] Rate limit hit; dropping message. last_30s=%d, last_300s=%d",
                    self.channel,
                    len(self._times_30s),
                    len(self._times_300s),
                )
                return False

            if self.max_300s > 0 and len(self._times_300s) >= self.max_300s:
                log.debug(
                    "[TG:%s] 5-min rate limit hit; dropping message. last_30s=%d, last_300s=%d",
                    self.channel,
                    len(self._times_30s),
                    len(self._times_300s),
                )
                return False

            self._times_30s.append(now)
            self._times_300s.append(now)
            return True

    def _send(self, level: str, msg: str) -> None:
        if not self._should_send():
            return

        text = f"{self.prefix}{msg}" if self.prefix else msg
        url = f"https://api.telegram.org/bot{self.token}/sendMessage"

        try:
            requests.post(
                url,
                json={"chat_id": self.chat_id, "text": text, "disable_web_page_preview": True},
                timeout=HTTP_TIMEOUT,
            )
        except Exception as e:
            # Do NOT let TG flakiness break bots
            log.debug("[TG:%s] send failed at level %s: %r", self.channel, level, e)


# ---------------------------------------------------------------------------
# Notifier factory & registry
# ---------------------------------------------------------------------------

_NOTIFIERS: Dict[str, TelegramNotifier] = {}
_BUILD_LOCK = threading.Lock()


def _env_pair(token_key: str, chat_key: str) -> Tuple[str, str]:
    """
    Fetch token/chat_id at build-time (not import-time).
    """
    return os.getenv(token_key, ""), os.getenv(chat_key, "")


def _build_notifier(channel: str) -> TelegramNotifier:
    """
    Internal: construct a TelegramNotifier for a given logical channel.

    Known channels:
      - main
      - journal
      - drip
      - profit_sweeper
      - health
      - flashback01..flashback10

    Unknown channels fall back to main.
    """
    _load_dotenv_once()

    ch = channel.lower().strip()

    # Defaults
    token, chat_id = _env_pair("TG_TOKEN_MAIN", "TG_CHAT_MAIN")
    prefix = ""
    max_30s = DEFAULT_MAX_30S
    max_300s = DEFAULT_MAX_300S

    if ch == "main":
        token, chat_id = _env_pair("TG_TOKEN_MAIN", "TG_CHAT_MAIN")
        prefix = ""
    elif ch == "journal":
        token, chat_id = _env_pair("TG_TOKEN_JOURNAL", "TG_CHAT_JOURNAL")
        prefix = "📒 "
    elif ch == "drip":
        token, chat_id = _env_pair("TG_TOKEN_DRIP", "TG_CHAT_DRIP")
        prefix = "💧 "
    elif ch == "profit_sweeper":
        token, chat_id = _env_pair("TG_TOKEN_PROFIT_SWEEPER", "TG_CHAT_PROFIT_SWEEPER")
        prefix = "🧹 "
    elif ch == "health":
        token, chat_id = _env_pair("TG_TOKEN_HEALTH", "TG_CHAT_HEALTH")
        prefix = "🩺 "

    # Subaccounts: flashback01..flashback10
    elif ch == "flashback01":
        token, chat_id = _env_pair("TG_TOKEN_SUB_1", "TG_CHAT_SUB_1")
        prefix = "🧪[fb01] "
    elif ch == "flashback02":
        token, chat_id = _env_pair("TG_TOKEN_SUB_2", "TG_CHAT_SUB_2")
        prefix = "🧪[fb02] "
    elif ch == "flashback03":
        token, chat_id = _env_pair("TG_TOKEN_SUB_3", "TG_CHAT_SUB_3")
        prefix = "🧪[fb03] "
    elif ch == "flashback04":
        token, chat_id = _env_pair("TG_TOKEN_SUB_4", "TG_CHAT_SUB_4")
        prefix = "🧪[fb04] "
    elif ch == "flashback05":
        token, chat_id = _env_pair("TG_TOKEN_SUB_5", "TG_CHAT_SUB_5")
        prefix = "🧪[fb05] "
    elif ch == "flashback06":
        token, chat_id = _env_pair("TG_TOKEN_SUB_6", "TG_CHAT_SUB_6")
        prefix = "🧪[fb06] "
    elif ch == "flashback07":
        token, chat_id = _env_pair("TG_TOKEN_SUB_7", "TG_CHAT_SUB_7")
        prefix = "🧪[fb07] "
    elif ch == "flashback08":
        token, chat_id = _env_pair("TG_TOKEN_SUB_8", "TG_CHAT_SUB_8")
        prefix = "🧪[fb08] "
    elif ch == "flashback09":
        token, chat_id = _env_pair("TG_TOKEN_SUB_9", "TG_CHAT_SUB_9")
        prefix = "🧪[fb09] "
    elif ch == "flashback10":
        token, chat_id = _env_pair("TG_TOKEN_SUB_10", "TG_CHAT_SUB_10")
        prefix = "🧪[fb10] "

    return TelegramNotifier(
        channel=ch,
        token=token,
        chat_id=chat_id,
        level="info",
        max_30s=max_30s,
        max_300s=max_300s,
        prefix=prefix,
    )


def get_notifier(channel: str = "main") -> TelegramNotifier:
    """
    Get (and cache) a TelegramNotifier for a given channel.
    """
    ch = (channel or "main").lower().strip()
    with _BUILD_LOCK:
        if ch not in _NOTIFIERS:
            _NOTIFIERS[ch] = _build_notifier(ch)
        return _NOTIFIERS[ch]


# ---------------------------------------------------------------------------
# Backwards-compatible helper: tg_send
# ---------------------------------------------------------------------------

def tg_send(text: str, channel: str = "main", level: str = "info") -> None:
    """
    Legacy helper, kept for older bots like executor_v2.
    """
    try:
        n = get_notifier(channel)
        lvl = (level or "info").lower()
        if lvl == "warn":
            n.warn(text)
        elif lvl == "error":
            n.error(text)
        elif lvl == "trade":
            n.trade(text)
        elif lvl == "debug":
            n.debug(text)
        else:
            n.info(text)
    except Exception as e:
        # This must NEVER crash bots
        log.debug("tg_send failed for channel=%s: %r", channel, e)
