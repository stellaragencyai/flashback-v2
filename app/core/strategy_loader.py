import os
from pathlib import Path
from typing import Dict, List, Optional, Any

import yaml
import jsonschema
import json

from app.core.config import settings
from app.core.logger import get_logger

log = get_logger("strategy_loader")

# Paths
MASTER_PATH = Path(settings.ROOT) / "config" / "master_strategies.yaml"
SCHEMA_PATH = Path(settings.ROOT) / "config" / "master_strategies.schema.json"


class UnifiedStrategy:
    def __init__(self, payload: Dict[str, Any]):
        self.id: str = payload.get("id")
        self.account_label: str = payload.get("account_label")
        self.sub_uid: Optional[str] = payload.get("sub_uid")
        self.enabled: bool = bool(payload.get("enabled", False))
        self.symbols: List[str] = payload.get("symbols", [])
        self.timeframes: List[str] = payload.get("timeframes", [])
        self.setup_types: List[str] = payload.get("setup_types", [])
        self.automation_mode: str = payload.get("automation_mode", "OFF")
        self.ai_profile: str = payload.get("ai_profile")
        self.risk_profile: str = payload.get("risk_profile")
        self.exit_profile: str = payload.get("exit_profile")
        self.promotion_rules: Dict[str, Any] = payload.get("promotion_rules", {})
        self.regime: Dict[str, Any] = payload.get("regime", {})
        self.tags: List[str] = payload.get("tags", [])
        self.telegram: Dict[str, str] = payload.get("telegram", {})
        self._raw_payload: Dict[str, Any] = payload

    def __repr__(self):
        return f"<UnifiedStrategy {self.id} [{self.account_label}]>"

    @property
    def bot_token(self) -> str:
        return os.getenv(self.telegram.get("bot_token_env", "")) or ""

    @property
    def chat_id(self) -> str:
        return os.getenv(self.telegram.get("chat_id_env", "")) or ""

    @property
    def api_key(self) -> str:
        return os.getenv(self._raw_payload.get("api_env", {}).get("key", "")) or ""

    @property
    def api_secret(self) -> str:
        return os.getenv(self._raw_payload.get("api_env", {}).get("secret", "")) or ""


# --- Internal loaders + validator ---

def _load_yaml(path: Path) -> Dict[str, Any]:
    if not path.exists():
        log.error(f"Unified config missing: {path}")
        return {}
    with path.open("r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


def _load_json(path: Path) -> Dict[str, Any]:
    if not path.exists():
        log.error(f"Schema missing: {path}")
        return {}
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def _validate_master_config(data: Dict[str, Any]) -> None:
    """
    Validate the unified config against the JSON schema.
    Raises a detailed exception if invalid.
    """
    schema = _load_json(SCHEMA_PATH)
    try:
        jsonschema.validate(instance=data, schema=schema)
        log.info("master_strategies.yaml validated successfully against schema")
    except jsonschema.ValidationError as e:
        # Format the error for clarity
        msg_lines = [
            "JSON Schema validation failed for master_strategies.yaml:",
            f"- Error message: {e.message}",
            f"- At path: {list(e.path)}",
            f"- Schema path: {list(e.schema_path)}",
        ]
        # Log details
        for line in msg_lines:
            log.error(line)
        # Raise a new exception so startup halts
        raise RuntimeError("\n".join(msg_lines)) from e


def load_master_strategies(validate: bool = True) -> List[UnifiedStrategy]:
    data = _load_yaml(MASTER_PATH)

    if validate:
        _validate_master_config(data)

    raw_strats = data.get("strategies", [])
    unified: List[UnifiedStrategy] = [UnifiedStrategy(raw) for raw in raw_strats]

    log.info(f"Loaded {len(unified)} unified strategies")
    return unified


def get_strategy_by_id(strategy_id: str) -> Optional[UnifiedStrategy]:
    for s in load_master_strategies():
        if s.id == strategy_id:
            return s
    return None


def strategies_for_symbol_timeframe(symbol: str, timeframe: str) -> List[UnifiedStrategy]:
    sym = symbol.upper()
    tf = str(timeframe)
    return [
        s for s in load_master_strategies()
        if s.enabled and sym in s.symbols and tf in s.timeframes
    ]
