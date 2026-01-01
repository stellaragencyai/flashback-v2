"""
Flashback Exchange Balance Snapshot
----------------------------------

Canonical balance snapshot contract.

This module is the single source of truth for:
- Exchange balances
- Subaccount capital
- DRY-RUN vs LIVE enforcement
- Capital Flow Engine inputs
- AI state grounding

DO NOT DUPLICATE OR BYPASS THIS MODULE.
"""

from __future__ import annotations

import time
from dataclasses import dataclass, asdict
from typing import Dict, Optional

# ============================================================
# CONFIG
# ============================================================

DRY_RUN_DEFAULT = True

# ============================================================
# DATA MODELS
# ============================================================

@dataclass(frozen=True)
class AssetBalance:
    asset: str
    free: float
    locked: float

    @property
    def total(self) -> float:
        return self.free + self.locked


@dataclass(frozen=True)
class BalanceSnapshot:
    timestamp_ns: int
    exchange: str
    subaccount: str
    balances: Dict[str, AssetBalance]
    dry_run: bool

    def total_equity(self, quote_asset: str = "USDT") -> float:
        if quote_asset not in self.balances:
            return 0.0
        return self.balances[quote_asset].total


# ============================================================
# SNAPSHOT ENGINE
# ============================================================

class BalanceSnapshotEngine:
    """
    Deterministic, side-effect controlled snapshot engine.
    """

    def __init__(
        self,
        exchange_name: str,
        subaccount: str,
        dry_run: bool = DRY_RUN_DEFAULT,
    ):
        self.exchange_name = exchange_name
        self.subaccount = subaccount
        self.dry_run = dry_run

    def fetch_raw_balances(self) -> Dict[str, Dict[str, float]]:
        """
        DRY-RUN:
            Returns deterministic mock balances.

        LIVE:
            Must be implemented via exchange adapter.
        """
        if self.dry_run:
            return {
                "USDT": {"free": 100_000.0, "locked": 0.0},
            }

        raise NotImplementedError(
            "LIVE balance fetching not wired. DRY-RUN enforced."
        )

    def normalize(self, raw: Dict[str, Dict[str, float]]) -> Dict[str, AssetBalance]:
        balances: Dict[str, AssetBalance] = {}
        for asset, data in raw.items():
            balances[asset] = AssetBalance(
                asset=asset,
                free=float(data.get("free", 0.0)),
                locked=float(data.get("locked", 0.0)),
            )
        return balances

    def snapshot(self) -> BalanceSnapshot:
        raw = self.fetch_raw_balances()
        normalized = self.normalize(raw)

        return BalanceSnapshot(
            timestamp_ns=time.time_ns(),
            exchange=self.exchange_name,
            subaccount=self.subaccount,
            balances=normalized,
            dry_run=self.dry_run,
        )


# ============================================================
# PUBLIC API
# ============================================================

def get_balance_snapshot(
    exchange_name: str,
    subaccount: str,
    dry_run: Optional[bool] = None,
) -> BalanceSnapshot:
    engine = BalanceSnapshotEngine(
        exchange_name=exchange_name,
        subaccount=subaccount,
        dry_run=DRY_RUN_DEFAULT if dry_run is None else dry_run,
    )
    return engine.snapshot()


def snapshot_to_dict(snapshot: BalanceSnapshot) -> dict:
    return {
        "timestamp_ns": snapshot.timestamp_ns,
        "exchange": snapshot.exchange,
        "subaccount": snapshot.subaccount,
        "dry_run": snapshot.dry_run,
        "balances": {
            asset: asdict(balance)
            for asset, balance in snapshot.balances.items()
        },
    }
