import pandas as pd

def load_candles(
    file_path: str,
    start_ts: int = None,
    end_ts: int = None,
) -> pd.DataFrame:
    """
    Loads historical candles from CSV/Parquet.
    Columns expected: ts, open, high, low, close, volume
    """
    df = pd.read_parquet(file_path) if file_path.endswith(".parquet") else pd.read_csv(file_path)
    df["ts"] = pd.to_datetime(df["ts"], unit="ms")
    df = df.sort_values("ts")

    if start_ts:
        df = df[df["ts"] >= pd.to_datetime(start_ts, unit="ms")]
    if end_ts:
        df = df[df["ts"] <= pd.to_datetime(end_ts, unit="ms")]

    return df.reset_index(drop=True)
