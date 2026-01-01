from __future__ import annotations

import json
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any, Dict, Tuple

OBSERVED_PATH = Path("state/ai_events/observed_ingested.jsonl")
DECISIONS_PATH = Path("state/ai_decisions.deduped.jsonl")

# Guardrail thresholds (v1)
MIN_OBSERVED_ROWS = 20
MIN_DECISION_ROWS = 1000  # decisions file is huge; sanity threshold
MIN_PER_SETUP = 5         # observed per setup_type
MAX_SINGLE_SETUP_SHARE = 0.85  # if one setup dominates, learning is skewed
MAX_SINGLE_REGIME_SHARE = 0.90 # placeholder; regime may be missing in observed stream

def _read_jsonl(path: Path):
    for line in path.read_text(encoding="utf-8", errors="ignore").splitlines():
        s = line.strip()
        if not s:
            continue
        try:
            obj = json.loads(s)
        except Exception:
            continue
        if isinstance(obj, dict):
            yield obj

def main() -> int:
    if not OBSERVED_PATH.exists():
        print(f"FAIL: missing {OBSERVED_PATH}")
        return 2
    if not DECISIONS_PATH.exists():
        print(f"FAIL: missing {DECISIONS_PATH}")
        return 2

    observed_rows = list(_read_jsonl(OBSERVED_PATH))
    decision_rows = list(_read_jsonl(DECISIONS_PATH))

    obs_n = len(observed_rows)
    dec_n = len(decision_rows)

    # Basic size gates
    fail_reasons = []
    if obs_n < MIN_OBSERVED_ROWS:
        fail_reasons.append(f"observed_rows<{MIN_OBSERVED_ROWS} (got {obs_n})")
    if dec_n < MIN_DECISION_ROWS:
        fail_reasons.append(f"decision_rows<{MIN_DECISION_ROWS} (got {dec_n})")

    # Observed distribution checks
    setup_counts = Counter()
    side_counts = Counter()
    tf_counts = Counter()

    for r in observed_rows:
        setup_counts[str(r.get("setup_type") or "MISSING")] += 1
        side_counts[str(r.get("side") or "MISSING")] += 1
        tf_counts[str(r.get("timeframe") or "MISSING")] += 1

    # Per-setup minimum
    weak_setups = [(k, v) for k, v in setup_counts.items() if v < MIN_PER_SETUP]
    # Dominance check
    top_setup, top_setup_n = setup_counts.most_common(1)[0] if setup_counts else ("NONE", 0)
    top_share = (top_setup_n / obs_n) if obs_n else 1.0
    if top_share > MAX_SINGLE_SETUP_SHARE and obs_n >= MIN_OBSERVED_ROWS:
        fail_reasons.append(f"setup_skewed: top_setup_share>{MAX_SINGLE_SETUP_SHARE:.2f} ({top_setup}={top_share:.2f})")

    # NOTE: regime is not present in observed stream; we cannot check it yet.
    regime_check = "SKIP (regime not in observed_ingested)"

    # Decision sanity checks
    decision_code_counts = Counter()
    snapshot_mode_counts = Counter()
    account_counts = Counter()

    for d in decision_rows:
        decision_code_counts[str(d.get("decision_code") or "MISSING")] += 1
        snapshot_mode_counts[str(d.get("snapshot_mode") or "MISSING")] += 1
        account_counts[str(d.get("account_label") or "MISSING")] += 1

    # Canary-only learning gate (v1): we don't allow LIVE learning unless explicitly canary.
    # Since this is a verifier, we just REPORT modes present.
    # You can later tighten this to fail if snapshot_mode shows LIVE when not in canary.
    modes_present = list(snapshot_mode_counts.keys())

    print("=== AI SAMPLING GUARDRAILS (v1) ===")
    print("observed_path=", str(OBSERVED_PATH))
    print("decisions_path=", str(DECISIONS_PATH))
    print("observed_rows=", obs_n)
    print("decision_rows=", dec_n)
    print("--- observed breakdown ---")
    print("top_setup=", top_setup, "count=", top_setup_n, "share=", round(top_share, 4))
    print("setup_types=", len(setup_counts))
    print("timeframes=", dict(tf_counts))
    print("sides=", dict(side_counts))
    print("weak_setups(<%d)=" % MIN_PER_SETUP, len(weak_setups))
    print("--- decisions breakdown ---")
    print("accounts=", len(account_counts))
    print("decision_codes_top5=", decision_code_counts.most_common(5))
    print("snapshot_modes=", dict(snapshot_mode_counts))
    print("regime_check=", regime_check)

    if fail_reasons:
        print("FAIL")
        for r in fail_reasons[:10]:
            print("reason=", r)
        return 1

    print("PASS")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
