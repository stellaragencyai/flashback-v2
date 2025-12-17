from typing import Dict, Any

REQUIRED_SETUP_KEYS = (
    "trade_id",
    "symbol",
    "account_label",
    "strategy",
    "setup_type",
    "timeframe",
    "features",
    "memory_fingerprint",
)

def has_valid_setup_context(ctx: Dict[str, Any]) -> bool:
    if not isinstance(ctx, dict):
        return False
    for k in REQUIRED_SETUP_KEYS:
        if k not in ctx:
            return False
    return True
