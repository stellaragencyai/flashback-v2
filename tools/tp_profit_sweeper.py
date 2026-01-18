# -*- coding: utf-8 -*-
r"""
TP Profit Sweeper (Main -> Subaccounts) + Telegram

Terminal Proof:
- Prints a loud terminal line for EVERY sweep attempt:
  - SUCCESS, FAIL, or EXCEPTION
- Maintains counters: seen executions, qualified sweeps, success/fail/exception totals

Proof-of-life:
- Sends Telegram on startup.
- Confirms WebSocket execution stream is delivering messages (first msg => WS OK).
- Optional: executes a tiny transfer smoke test at startup (disabled by default).

Trigger:
- Every private WS execution event where execPnl > MIN_TP_PNL_USD (positive only)

Action:
- Transfer TRANSFER_PCT of execPnl (USDT) from MAIN UID -> next sub UID (RR: 1,2,4,5,6,7,8,9)
- Telegram notify on every transfer success/failure

State:
- Persists RR index + seen execIds to disk (restart-safe)
"""

from __future__ import annotations

import json
import math
import os
import signal
import time
import uuid
from collections import deque
from dataclasses import dataclass
from typing import Any, Dict

import requests
from pybit.unified_trading import HTTP, WebSocket

try:
    from dotenv import load_dotenv  # type: ignore
    _HAS_DOTENV = True
except Exception:
    _HAS_DOTENV = False


ENV_PATH = os.environ.get("FLASHBACK_ENV_PATH", r"C:\Flashback\.env")
STATE_PATH = os.environ.get("TP_SWEEPER_STATE_PATH", r"C:\Flashback\state\tp_sweeper_state.json")

COIN = (os.environ.get("TP_SWEEPER_COIN", "USDT") or "USDT").strip()
FROM_ACCOUNT_TYPE = (os.environ.get("TP_SWEEPER_FROM_ACCOUNT_TYPE", "UNIFIED") or "UNIFIED").strip()
TO_ACCOUNT_TYPE = (os.environ.get("TP_SWEEPER_TO_ACCOUNT_TYPE", "UNIFIED") or "UNIFIED").strip()

MIN_TP_PNL_USD = float(os.environ.get("TP_SWEEPER_MIN_TP_PNL_USD", "25"))
TRANSFER_PCT = float(os.environ.get("TP_SWEEPER_TRANSFER_PCT", "0.10"))
MIN_TRANSFER_USD = float(os.environ.get("TP_SWEEPER_MIN_TRANSFER_USD", "1.00"))

RR_ORDER = [1, 2, 4, 5, 6, 7, 8, 9]

EXEC_ID_CACHE_MAX = int(os.environ.get("TP_SWEEPER_EXEC_ID_CACHE_MAX", "10000"))
WS_PING_INTERVAL = int(os.environ.get("TP_SWEEPER_WS_PING_INTERVAL", "20"))

# WS proof
WS_FIRST_MSG_TIMEOUT_SEC = int(os.environ.get("TP_SWEEPER_WS_FIRST_MSG_TIMEOUT_SEC", "30"))

# Optional startup smoke transfer
SMOKE_TRANSFER = (os.environ.get("TP_SWEEPER_SMOKE_TRANSFER", "0") or "0").strip() == "1"
SMOKE_AMOUNT = (os.environ.get("TP_SWEEPER_SMOKE_AMOUNT", "0.01") or "0.01").strip()
SMOKE_TO_SLOT = int((os.environ.get("TP_SWEEPER_SMOKE_TO_SLOT", "1") or "1").strip())


def die(msg: str) -> None:
    raise SystemExit(f"[FATAL] {msg}")


def ts() -> str:
    return time.strftime("%Y-%m-%d %H:%M:%S")


def info(msg: str) -> None:
    print(f"[{ts()}] [INFO] {msg}", flush=True)


def ok(msg: str) -> None:
    print(f"[{ts()}] [OK] {msg}", flush=True)


def warn(msg: str) -> None:
    print(f"[{ts()}] [WARN] {msg}", flush=True)


def loud(event: str, msg: str) -> None:
    """
    Always-visible terminal line. Use for sweep attempts and outcomes.
    """
    print(f"[{ts()}] [SWEEP-{event}] {msg}", flush=True)


def round_down(value: float, decimals: int = 2) -> float:
    p = 10 ** decimals
    return math.floor(value * p) / p


def load_state(path: str) -> Dict[str, Any]:
    if not os.path.exists(path):
        return {"rr_index": 0, "seen_exec_ids": []}
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {"rr_index": 0, "seen_exec_ids": []}


def save_state(path: str, state: Dict[str, Any]) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    tmp = f"{path}.tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(state, f, indent=2, sort_keys=True)
    os.replace(tmp, path)


@dataclass
class SubMap:
    main_uid: int
    subs: Dict[int, int]  # rr slot -> uid


class TelegramNotifier:
    def __init__(self) -> None:
        token = (os.environ.get("TG_TOKEN_DRIP", "") or "").strip() or (os.environ.get("TG_BOT_TOKEN", "") or "").strip()
        chat = (os.environ.get("TG_CHAT_DRIP", "") or "").strip() or (os.environ.get("TG_CHAT_ID", "") or "").strip()
        if not token or not chat:
            die("Missing Telegram creds. Set TG_TOKEN_DRIP + TG_CHAT_DRIP (or TG_BOT_TOKEN + TG_CHAT_ID).")
        self.token = token
        self.chat_id = chat

    def send(self, text: str) -> None:
        url = f"https://api.telegram.org/bot{self.token}/sendMessage"
        payload = {"chat_id": self.chat_id, "text": text, "disable_web_page_preview": True}
        try:
            r = requests.post(url, json=payload, timeout=10)
            if r.status_code != 200:
                warn(f"Telegram send failed HTTP {r.status_code}: {r.text}")
        except Exception as e:
            warn(f"Telegram exception: {repr(e)}")


class TPSweeper:
    def __init__(self) -> None:
        if _HAS_DOTENV and os.path.exists(ENV_PATH):
            load_dotenv(ENV_PATH, override=False)

        self.testnet = (os.environ.get("BYBIT_TESTNET", "0") or "0").strip() == "1"

        # WS creds
        self.ws_key = (os.environ.get("BYBIT_MAIN_WEBSOCKET_KEY", "") or "").strip() or (os.environ.get("BYBIT_API_KEY", "") or "").strip()
        self.ws_secret = (os.environ.get("BYBIT_MAIN_WEBSOCKET_SECRET", "") or "").strip() or (os.environ.get("BYBIT_API_SECRET", "") or "").strip()
        if not self.ws_key or not self.ws_secret:
            die("Missing WS creds. Set BYBIT_MAIN_WEBSOCKET_KEY/SECRET or BYBIT_API_KEY/SECRET.")

        # Transfer creds
        self.xfer_key = (os.environ.get("BYBIT_MAIN_TRANSFER_KEY", "") or "").strip() or (os.environ.get("BYBIT_API_KEY", "") or "").strip()
        self.xfer_secret = (os.environ.get("BYBIT_MAIN_TRANSFER_SECRET", "") or "").strip() or (os.environ.get("BYBIT_API_SECRET", "") or "").strip()
        if not self.xfer_key or not self.xfer_secret:
            die("Missing transfer creds. Set BYBIT_MAIN_TRANSFER_KEY/SECRET or BYBIT_API_KEY/SECRET.")

        main_uid_str = (os.environ.get("BYBIT_MAIN_UID", "") or "").strip()
        if not main_uid_str:
            die("Missing BYBIT_MAIN_UID in env.")
        main_uid = int(main_uid_str)

        subs: Dict[int, int] = {}
        for n in RR_ORDER:
            v = (os.environ.get(f"SUB_UID_{n}", "") or "").strip()
            if not v:
                die(f"Missing SUB_UID_{n} in env.")
            subs[n] = int(v)

        self.submap = SubMap(main_uid=main_uid, subs=subs)

        self.state = load_state(STATE_PATH)
        self.rr_index = int(self.state.get("rr_index", 0)) if int(self.state.get("rr_index", 0)) >= 0 else 0
        seen = self.state.get("seen_exec_ids", [])
        self.seen_exec_ids = deque(seen[-EXEC_ID_CACHE_MAX:], maxlen=EXEC_ID_CACHE_MAX)

        self.http = HTTP(testnet=self.testnet, api_key=self.xfer_key, api_secret=self.xfer_secret)
        self.ws = WebSocket(
            testnet=self.testnet,
            channel_type="private",
            api_key=self.ws_key,
            api_secret=self.ws_secret,
            ping_interval=WS_PING_INTERVAL,
            ping_timeout=10,
        )

        self.tg = TelegramNotifier()
        self._stop = False

        # WS proof flags
        self._ws_first_msg_seen = False
        self._ws_started_at = time.time()

        # Counters
        self.c_exec_seen = 0
        self.c_exec_dedup_skipped = 0
        self.c_exec_not_qualified = 0
        self.c_sweep_attempts = 0
        self.c_sweep_success = 0
        self.c_sweep_fail = 0
        self.c_sweep_exception = 0

    def stop(self) -> None:
        self._stop = True

    def _persist(self) -> None:
        self.state["rr_index"] = int(self.rr_index)
        self.state["seen_exec_ids"] = list(self.seen_exec_ids)
        save_state(STATE_PATH, self.state)

    def _already_processed(self, exec_id: str) -> bool:
        return exec_id in self.seen_exec_ids

    def _mark_processed(self, exec_id: str) -> None:
        self.seen_exec_ids.append(exec_id)
        self._persist()

    def _next_rr_slot(self) -> int:
        slot = RR_ORDER[self.rr_index % len(RR_ORDER)]
        self.rr_index = (self.rr_index + 1) % len(RR_ORDER)
        return slot

    def _parse_float(self, x: Any) -> float:
        try:
            return float(x)
        except Exception:
            return 0.0

    def _transfer(self, amount_usdt: float, to_uid: int) -> Dict[str, Any]:
        transfer_id = str(uuid.uuid4())
        return self.http.create_universal_transfer(
            transferId=transfer_id,
            coin=COIN,
            amount=f"{amount_usdt:.2f}",
            fromMemberId=self.submap.main_uid,
            toMemberId=to_uid,
            fromAccountType=FROM_ACCOUNT_TYPE,
            toAccountType=TO_ACCOUNT_TYPE,
        )

    def _smoke_transfer(self) -> None:
        if not SMOKE_TRANSFER:
            return
        if SMOKE_TO_SLOT not in self.submap.subs:
            self.tg.send(f"TP Sweeper smoke transfer: ❌ invalid slot {SMOKE_TO_SLOT}. Valid: {sorted(self.submap.subs.keys())}")
            return

        to_uid = self.submap.subs[SMOKE_TO_SLOT]
        try:
            amt = float(SMOKE_AMOUNT)
        except Exception:
            amt = 0.01
        if amt <= 0:
            amt = 0.01

        self.tg.send(f"TP Sweeper smoke transfer: attempting {amt:.2f} {COIN} -> sub#{SMOKE_TO_SLOT} uid={to_uid}")
        try:
            resp = self.http.create_universal_transfer(
                transferId=str(uuid.uuid4()),
                coin=COIN,
                amount=f"{amt:.2f}",
                fromMemberId=self.submap.main_uid,
                toMemberId=to_uid,
                fromAccountType=FROM_ACCOUNT_TYPE,
                toAccountType=TO_ACCOUNT_TYPE,
            )
            rc = resp.get("retCode")
            rm = resp.get("retMsg")
            status = (resp.get("result") or {}).get("status")
            if rc == 0:
                self.tg.send(f"TP Sweeper smoke transfer: ✅ OK (status={status})")
                ok(f"Smoke transfer OK -> sub#{SMOKE_TO_SLOT} amt={amt:.2f} {COIN} status={status}")
            else:
                self.tg.send(f"TP Sweeper smoke transfer: ❌ FAIL retCode={rc} retMsg={rm}")
                warn(f"Smoke transfer FAIL retCode={rc} retMsg={rm}")
        except Exception as e:
            self.tg.send(f"TP Sweeper smoke transfer: ❌ EXCEPTION {repr(e)}")
            warn(f"Smoke transfer EXCEPTION {repr(e)}")

    def on_execution(self, msg: Dict[str, Any]) -> None:
        # WS proof-of-life
        if not self._ws_first_msg_seen:
            self._ws_first_msg_seen = True
            self.tg.send("TP Sweeper WS ✅ receiving execution events (subscription confirmed).")
            ok("WS subscription confirmed (first execution msg received).")

        data = msg.get("data", [])
        if not isinstance(data, list):
            return

        for item in data:
            self.c_exec_seen += 1

            exec_id = str(item.get("execId", "")).strip()
            if not exec_id:
                continue

            if self._already_processed(exec_id):
                self.c_exec_dedup_skipped += 1
                continue

            pnl = self._parse_float(item.get("execPnl", "0"))

            # Gate: PnL-only, positive, above threshold
            if pnl <= 0 or pnl <= MIN_TP_PNL_USD:
                self.c_exec_not_qualified += 1
                self._mark_processed(exec_id)
                continue

            skim = round_down(pnl * TRANSFER_PCT, 2)
            if skim < MIN_TRANSFER_USD:
                self.c_exec_not_qualified += 1
                self._mark_processed(exec_id)
                continue

            slot = self._next_rr_slot()
            to_uid = self.submap.subs[slot]

            symbol = str(item.get("symbol", ""))
            side = str(item.get("side", ""))
            exec_price = str(item.get("execPrice", ""))
            exec_qty = str(item.get("execQty", ""))
            exec_time = str(item.get("execTime", ""))

            # Always-visible terminal attempt line
            self.c_sweep_attempts += 1
            loud(
                "ATTEMPT",
                f"#{self.c_sweep_attempts} execId={exec_id} symbol={symbol} side={side} pnl={pnl:.2f} skim={skim:.2f} -> sub#{slot} uid={to_uid}"
            )

            base = (
                f"TP Profit Sweeper\n"
                f"execId: {exec_id}\n"
                f"symbol: {symbol} side: {side}\n"
                f"price: {exec_price} qty: {exec_qty}\n"
                f"execPnl: {pnl:.2f} USDT\n"
                f"skim: {skim:.2f} USDT ({int(TRANSFER_PCT*100)}%)\n"
                f"RR -> sub#{slot} uid={to_uid}\n"
                f"time: {exec_time}"
            )

            try:
                resp = self._transfer(skim, to_uid)
                rc = resp.get("retCode")
                rm = resp.get("retMsg")
                status = (resp.get("result") or {}).get("status")

                if rc == 0:
                    self.c_sweep_success += 1
                    self.tg.send(base + f"\nTRANSFER: ✅ OK (status={status})")
                    loud(
                        "SUCCESS",
                        f"#{self.c_sweep_attempts} execId={exec_id} skim={skim:.2f} {COIN} -> sub#{slot} status={status} | totals: ok={self.c_sweep_success} fail={self.c_sweep_fail} ex={self.c_sweep_exception}"
                    )
                else:
                    self.c_sweep_fail += 1
                    self.tg.send(base + f"\nTRANSFER: ❌ FAIL retCode={rc} retMsg={rm}")
                    loud(
                        "FAIL",
                        f"#{self.c_sweep_attempts} execId={exec_id} retCode={rc} retMsg={rm} | totals: ok={self.c_sweep_success} fail={self.c_sweep_fail} ex={self.c_sweep_exception}"
                    )

            except Exception as e:
                self.c_sweep_exception += 1
                self.tg.send(base + f"\nTRANSFER: ❌ EXCEPTION {repr(e)}")
                loud(
                    "EXCEPTION",
                    f"#{self.c_sweep_attempts} execId={exec_id} err={repr(e)} | totals: ok={self.c_sweep_success} fail={self.c_sweep_fail} ex={self.c_sweep_exception}"
                )

            # Always mark processed so WS repeats don't double-send money
            self._mark_processed(exec_id)

    def run(self) -> None:
        boot = (
            f"TP Sweeper ONLINE ✅\n"
            f"testnet={self.testnet}\n"
            f"coin={COIN}\n"
            f"threshold={MIN_TP_PNL_USD} pct={TRANSFER_PCT} min_transfer={MIN_TRANSFER_USD}\n"
            f"from={FROM_ACCOUNT_TYPE} to={TO_ACCOUNT_TYPE}\n"
            f"rr={RR_ORDER}\n"
            f"state={STATE_PATH}"
        )
        info(boot.replace("\n", " | "))
        self.tg.send(boot)

        # Optional smoke transfer proves HTTP transfer key works immediately.
        self._smoke_transfer()

        # Subscribe to execution stream
        self.ws.execution_stream(callback=self.on_execution)

        # Watchdog: if no WS msg arrives in time, warn.
        while not self._stop:
            if not self._ws_first_msg_seen:
                if (time.time() - self._ws_started_at) > WS_FIRST_MSG_TIMEOUT_SEC:
                    self.tg.send(
                        f"TP Sweeper WS warning ⚠️ No execution events received after {WS_FIRST_MSG_TIMEOUT_SEC}s.\n"
                        f"This can be normal if you didn't trade, OR WS auth/permissions/IP whitelist is wrong."
                    )
                    warn(f"No execution events received after {WS_FIRST_MSG_TIMEOUT_SEC}s (could be normal if no trades).")
                    # avoid spamming
                    self._ws_started_at = time.time() + 10**9
            time.sleep(1)

        self.tg.send(
            "TP Sweeper stopping 🛑\n"
            f"c_exec_seen={self.c_exec_seen} c_dedup_skipped={self.c_exec_dedup_skipped} c_not_qualified={self.c_exec_not_qualified}\n"
            f"c_sweep_attempts={self.c_sweep_attempts} ok={self.c_sweep_success} fail={self.c_sweep_fail} ex={self.c_sweep_exception}"
        )
        self._persist()
        ok("Stopped. State persisted.")

def main() -> None:
    sweeper = TPSweeper()

    def handle(sig: int, _frame: Any) -> None:
        warn(f"Signal {sig} received, stopping...")
        sweeper.stop()

    signal.signal(signal.SIGINT, handle)
    signal.signal(signal.SIGTERM, handle)

    sweeper.run()


if __name__ == "__main__":
    main()
