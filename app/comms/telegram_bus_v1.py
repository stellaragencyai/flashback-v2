import os
import asyncio
from dotenv import load_dotenv
from telegram import Bot

# Auto-load .env from project root
load_dotenv()

BOTS = {}

def _register(label, token, chat):
    if token and chat:
        BOTS[label] = (Bot(token=token), chat)

# MAIN / CORE CHANNELS
_register("main", os.getenv("TG_TOKEN_MAIN"), os.getenv("TG_CHAT_MAIN"))
_register("notif", os.getenv("TG_TOKEN_NOTIF"), os.getenv("TG_CHAT_NOTIF"))
_register("journal", os.getenv("TG_TOKEN_JOURNAL"), os.getenv("TG_CHAT_JOURNAL"))
_register("drip", os.getenv("TG_TOKEN_DRIP"), os.getenv("TG_CHAT_DRIP"))
_register("ema_auto", os.getenv("TG_TOKEN_EMA_AUTO"), os.getenv("TG_CHAT_EMA_AUTO"))

# SUBACCOUNTS 01–10
for i in range(1, 11):
    _register(
        f"flashback_{i:02d}",
        os.getenv(f"TG_TOKEN_SUB_{i}"),
        os.getenv(f"TG_CHAT_SUB_{i}")
    )

async def _send(bot, chat, message):
    await bot.send_message(chat_id=chat, text=message)

def send_alert(label, message):
    entry = BOTS.get(label)
    if not entry:
        return
    bot, chat = entry
    try:
        asyncio.run(_send(bot, chat, message))
    except RuntimeError:
        loop = asyncio.get_event_loop()
        loop.create_task(_send(bot, chat, message))
    except Exception:
        pass
