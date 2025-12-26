from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, Optional

from app.core.observed_contract import normalize_observed_row, assert_normalized_row_ok

# Optional regime classifier (must NEVER break ingestion)
try:
    from app.ai.ai_regime_scanner import classify_from_indicators
except Exception:
    classify_from_indicators = None  # hard fallback

IN_PATH = Path("signals/observed.jsonl")
OUT_PATH = Path("state/ai_events/observed_ingested.jsonl")


def _read_jsonl(path: Path):
    if not path.exists():
        return
    for i, line in enumerate(path.read_text(encoding="utf-8", errors="ignore").splitlines(), start=1):
        s = line.strip()
        if not s:
            continue
        try:
            obj = json.loads(s)
        except Exception:
            continue
        if isinstance(obj, dict):
            yield i, obj


def _safe_float(x: Any) -> Optional[float]:
    try:
        v = float(x)
        if v != v:  # NaN
            return None
        return v
    except Exception:
        return None


def _extract_regime_indicators(norm: Dict[str, Any], raw: Dict[str, Any]) -> Optional[Dict[str, float]]:
    """
    Best-effort extraction of numeric regime indicators.
    Returns None if indicators are missing or invalid.
    """
    candidates = []

    # normalized debug
    dbg = norm.get("debug")
    if isinstance(dbg, dict):
        candidates.append(dbg.get("regime"))

    # raw debug
    dbg2 = raw.get("debug")
    if isinstance(dbg2, dict):
        candidates.append(dbg2.get("regime"))

    for r in candidates:
        if not isinstance(r, dict):
            continue

        adx = _safe_float(r.get("adx"))
        atr = _safe_float(r.get("atr_pct"))
        vol = _safe_float(r.get("vol_z"))

        if adx is None and atr is None and vol is None:
            continue

        return {
            "adx": adx or 0.0,
            "atr_pct": atr or 0.0,
            "vol_z": vol or 0.0,
        }

    return None


def main() -> int:
    if not IN_PATH.exists():
        print(f"FAIL: missing {IN_PATH}")
        return 2

    OUT_PATH.parent.mkdir(parents=True, exist_ok=True)

    total = 0
    wrote = 0
    bad = 0
    regime_attached = 0

    with OUT_PATH.open("w", encoding="utf-8", newline="\n") as f:
        for line_no, row in _read_jsonl(IN_PATH):
            total += 1
            try:
                norm = normalize_observed_row(row)
                norm["source"] = {"path": str(IN_PATH), "line_no": line_no}
                assert_normalized_row_ok(norm)
            except Exception as e:
                bad += 1
                print(f"BAD row line={line_no} err={e}")
                continue

            # Attach regime AFTER schema validation
            if classify_from_indicators is not None:
                regime_ind = _extract_regime_indicators(norm, row)
                if isinstance(regime_ind, dict):
                    try:
                        rr = classify_from_indicators(regime_ind)
                        norm["regime"] = rr.regime_tag
                        norm["regime_tags"] = rr.tags
                        norm["regime_confidence"] = rr.confidence
                        regime_attached += 1
                    except Exception:
                        pass  # NEVER fail ingestion

            f.write(json.dumps(norm, ensure_ascii=False) + "\n")
            wrote += 1

    print("=== OBSERVED INTAKE ===")
    print("rows_in=", total)
    print("rows_written=", wrote)
    print("rows_bad=", bad)
    print("regime_attached=", regime_attached)
    print("PASS" if (wrote > 0 and bad == 0) else "FAIL")

    return 0 if (wrote > 0 and bad == 0) else 1


if __name__ == "__main__":
    raise SystemExit(main())
