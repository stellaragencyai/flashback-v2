import json
import statistics
from pathlib import Path

AI_METRICS = Path("state/ai_metrics.jsonl")

def load_metrics(model_id: str):
    data = []
    with AI_METRICS.open("r") as f:
        for line in f:
            row = json.loads(line)
            if row["model_id"] == model_id:
                data.append(row)
    return data

def check_drift(model_id: str, metric: str, lookback: int = 20, threshold: float = 0.05):
    rows = load_metrics(model_id)
    if len(rows) < lookback:
        return False

    recent = rows[-lookback:]
    values = [r["metrics"].get(metric, None) for r in recent if r["metrics"].get(metric) is not None]

    if not values: return False

    avg = statistics.mean(values)
    baseline = statistics.mean(v for v in values[:-lookback//2])

    if avg < baseline * (1 - threshold):
        return True
    return False
