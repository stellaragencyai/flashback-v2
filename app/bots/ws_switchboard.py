import asyncio
import json
import logging
import time
import websockets
from pathlib import Path

log = logging.getLogger("ws_switchboard")

ROOT = Path(__file__).resolve().parents[2]
STATE = ROOT / "state"
STATE.mkdir(exist_ok=True)

ORDERBOOK_PATH = STATE / "orderbook_bus.json"
HB_PATH = STATE / "ws_switchboard_heartbeat_main.txt"

BYBIT_PUBLIC_WS = "wss://stream.bybit.com/v5/public/linear"
SYMBOL = "BTCUSDT"

async def main():
    while True:
        try:
            async with websockets.connect(BYBIT_PUBLIC_WS, ping_interval=20) as ws:
                sub = {
                    "op": "subscribe",
                    "args": [f"orderbook.1.{SYMBOL}"]
                }
                await ws.send(json.dumps(sub))
                log.info("Subscribed to Bybit orderbook")

                async for msg in ws:
                    data = json.loads(msg)
                    if "data" in data:
                        payload = {
                            "symbol": SYMBOL,
                            "ts": time.time(),
                            "data": data["data"]
                        }
                        ORDERBOOK_PATH.write_text(json.dumps(payload))
                        HB_PATH.write_text(str(time.time()))
        except Exception as e:
            log.error(f"WS error: {e}")
            await asyncio.sleep(5)


def loop():
    asyncio.run(main())

