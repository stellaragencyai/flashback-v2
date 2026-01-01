from __future__ import annotations
import math
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict

from app.data.append_store import append_rows, load_progress
from app.data.feature_registry import enforce_schema

# ---------------- JSON ----------------
try:
    import orjson
    loads = orjson.loads
    dumps = orjson.dumps
except Exception:
    import json
    def loads(b):
        if isinstance(b, (bytes, bytearray)):
            b = b.decode('utf-8')
        return json.loads(b)
    def dumps(o):
        return json.dumps(o, separators=(',', ':')).encode('utf-8')

# ---------------- PATHS ----------------
ROOT = Path(__file__).resolve().parents[2]
STATE = ROOT / 'state'

FEATURES_TRADES = STATE / 'features_trades.jsonl'
OUTCOMES = STATE / 'ai_events' / 'outcomes.enriched.backfill.jsonl'
OUT = STATE / 'feature_store.jsonl'

# ---------------- HELPERS ----------------
def ffloat(x):
    try:
        v = float(x)
        if math.isnan(v) or math.isinf(v):
            return None
        return v
    except Exception:
        return None

def fint(x):
    try:
        return int(x)
    except Exception:
        return None

def session(hour):
    if hour is None: return 'OTHER'
    if hour < 7: return 'ASIA'
    if hour < 13: return 'EU'
    if hour < 21: return 'US'
    return 'OTHER'

def regime(row):
    adx = ffloat(row.get('adx')) or 0
    atr = ffloat(row.get('atr_pct')) or 0
    vz  = ffloat(row.get('vol_zscore')) or 0
    if adx >= 20: return 'trend'
    if atr >= 1.0 or abs(vz) >= 1.5: return 'high_vol'
    if adx < 20 and atr < 1.0: return 'range'
    return 'other'

def load_jsonl(path: Path):
    if not path.exists():
        return []
    out = []
    with path.open('rb') as f:
        for line in f:
            try:
                obj = loads(line)
                if isinstance(obj, dict):
                    out.append(obj)
            except Exception:
                continue
    return out

# ---------------- NORMALIZERS ----------------
def normalize_feature_trade(r: Dict[str, Any]):
    try:
        ts = fint(r.get('ts_open_ms'))
        dt = datetime.fromtimestamp(ts/1000, tz=timezone.utc) if ts else None
        feat = r.get('features') or {}

        row = {
            'trade_id': r.get('trade_id'),
            'symbol': r.get('symbol'),
            'strategy_name': r.get('strategy_name'),
            'account_label': r.get('account_label'),
            'mode': r.get('mode'),
            'ts_open_ms': ts,
            'ts_open_iso': dt.isoformat() if dt else None,
            'dow': dt.weekday() if dt else None,
            'hour_utc': dt.hour if dt else None,
            'session': session(dt.hour if dt else None),
            'atr_pct': ffloat(feat.get('atr_pct')),
            'vol_zscore': ffloat(feat.get('volume_zscore')),
            'adx': ffloat(feat.get('adx')),
        }

        row['regime'] = regime(row)

        for k, v in feat.items():
            if k not in ('atr_pct', 'volume_zscore', 'adx'):
                row[f'f.{k}'] = v

        return row
    except Exception:
        return None

def normalize_outcome(evt: Dict[str, Any]):
    try:
        if evt.get('event_type') != 'outcome_enriched':
            return None

        extra = evt.get('outcome', {}).get('payload', {}).get('extra', {})
        ts = fint(extra.get('opened_ms'))
        dt = datetime.fromtimestamp(ts/1000, tz=timezone.utc) if ts else None

        row = {
            'trade_id': evt.get('trade_id'),
            'symbol': evt.get('symbol'),
            'strategy_name': evt.get('strategy'),
            'account_label': evt.get('account_label'),
            'mode': extra.get('mode'),
            'ts_open_ms': ts,
            'ts_open_iso': dt.isoformat() if dt else None,
            'dow': dt.weekday() if dt else None,
            'hour_utc': dt.hour if dt else None,
            'session': session(dt.hour if dt else None),
            'entry_price': ffloat(extra.get('entry_price')),
            'exit_price': ffloat(extra.get('exit_price')),
            'pnl_usd': ffloat(evt.get('outcome', {}).get('payload', {}).get('pnl_usd')),
            'r_multiple': ffloat(evt.get('outcome', {}).get('payload', {}).get('r_multiple')),
            'win': evt.get('outcome', {}).get('payload', {}).get('win'),
        }

        row['regime'] = regime(row)
        return row
    except Exception:
        return None

# ---------------- BUILD ----------------
def main():
    progress = load_progress()
    last_ts = progress.get('last_ts')

    rows = []
    max_ts = last_ts or 0

    for r in load_jsonl(FEATURES_TRADES):
        o = normalize_feature_trade(r)
        if not o:
            continue
        ts = o.get('ts_open_ms')
        if not last_ts or (ts and ts > last_ts):
            rows.append(o)
            if ts and ts > max_ts:
                max_ts = ts

    for e in load_jsonl(OUTCOMES):
        o = normalize_outcome(e)
        if not o:
            continue
        ts = o.get('ts_open_ms')
        if not last_ts or (ts and ts > last_ts):
            rows.append(o)
            if ts and ts > max_ts:
                max_ts = ts

    enforce_schema(rows)
    append_rows(OUT, rows, max_ts)
    print(f'[feature_builder] appended={len(rows)} last_ts={max_ts}')

if __name__ == '__main__':
    main()
