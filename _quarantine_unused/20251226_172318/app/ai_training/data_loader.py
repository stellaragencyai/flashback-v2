import json
import pandas as pd
from pathlib import Path
from typing import Tuple

from .config import TRADE_DNA_FILE

def load_trade_dna() -> pd.DataFrame:
    """
    Load trade DNA JSONL into a DataFrame.
    """
    if not TRADE_DNA_FILE.exists():
        raise FileNotFoundError(f"Trade DNA file not found: {TRADE_DNA_FILE}")

    rows = []
    with open(TRADE_DNA_FILE, "r", encoding="utf-8") as f:
        for line in f:
            try:
                rows.append(json.loads(line))
            except json.JSONDecodeError:
                continue

    return pd.json_normalize(rows)

def split_train_test(df, test_ratio: float = 0.2, seed: int = 42) -> Tuple[pd.DataFrame, pd.DataFrame]:
    return (
        df.sample(frac=1 - test_ratio, random_state=seed),
        df.drop(df.sample(frac=1 - test_ratio, random_state=seed).index)
    )
