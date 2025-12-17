#!/usr/bin/env python3
# app/core/corr_gate_v2.py
from __future__ import annotations

import os
from decimal import Decimal
from typing import Dict, Any, List, Tuple

from app.core.flashback_common import list_open_positions

# In DRY_RUN / PAPER training, correlation gating is not meaningful because:
# - positions may be simulated (PaperBroker) not in Bybit
# - Bybit endpoints can 403 and should not kill training
EXEC_DRY_RUN: bool = str(os.getenv('EXEC_DRY_RUN', 'false')).strip().lower() in ('1','true','yes','y','on')

# Simple static correlation map. Adjust over time.
# Values: 0.0–1.0 (1.0 = highly correlated).
_CORR: Dict[Tuple[str, str], float] = {}


def _norm(sym: str) -> str:
    return sym.upper().replace('PERP', '').replace('USDT', '')


def set_corr(sym_a: str, sym_b: str, corr: float) -> None:
    a = _norm(sym_a)
    b = _norm(sym_b)
    if a == b:
        return
    corr = max(0.0, min(1.0, float(corr)))
    _CORR[(a, b)] = corr
    _CORR[(b, a)] = corr


def get_corr(sym_a: str, sym_b: str) -> float:
    a = _norm(sym_a)
    b = _norm(sym_b)
    if a == b:
        return 1.0
    return _CORR.get((a, b), 0.0)


def correlated_exposure_too_high(symbol: str, max_corr: float = 0.8, max_pairs: int = 1) -> bool:
    open_pos: List[Dict[str, Any]] = list_open_positions()
    if not open_pos:
        return False

    base = _norm(symbol)
    hits = 0
    for p in open_pos:
        sym = p.get('symbol') or ''
        try:
            size = Decimal(str(p.get('size', '0')))
        except Exception:
            size = Decimal('0')
        if size <= 0:
            continue
        corr = get_corr(base, sym)
        if corr >= max_corr:
            hits += 1
            if hits > max_pairs:
                return True
    return False


def allow(symbol: str | None = None, *args: Any, **kwargs: Any) -> Tuple[bool, str]:
    # DRY_RUN bypass: never let training die because Bybit endpoints are blocked.
    if EXEC_DRY_RUN:
        return True, 'corr_gate_v2: DRY_RUN bypass'

    if symbol is None:
        symbol = kwargs.get('symbol')

    if not symbol:
        return True, 'corr_gate_v2: no symbol provided, bypassing correlation check'

    max_corr = float(kwargs.get('max_corr', 0.8))
    max_pairs = int(kwargs.get('max_pairs', 1))

    try:
        if correlated_exposure_too_high(symbol, max_corr=max_corr, max_pairs=max_pairs):
            return (
                False,
                f'corr_gate_v2: blocked {symbol} — correlated exposure above max_corr={max_corr}, max_pairs={max_pairs}',
            )
    except Exception as e:
        # Fail open; executor must never crash because corr gate couldn't fetch positions
        return True, f'corr_gate_v2: exception -> bypass ({e})'

    return True, f'corr_gate_v2: OK for {symbol} (max_corr={max_corr}, max_pairs={max_pairs})'
