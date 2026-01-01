import os
from app.comms.telegram_bus_v1 import register_bot

def init_subaccount_bots():
    for i in range(1, 11):
        token = os.getenv(f'TG_TOKEN_SUB_{i}')
        if not token:
            continue
        register_bot(f'flashback_{i:02d}', token)
