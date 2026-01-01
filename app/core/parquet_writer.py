import os, time
import polars as pl
from pathlib import Path

def write_chunk(df: pl.DataFrame, base_dir: str, prefix: str):
    base = Path(base_dir)
    tmp = base / '_chunks'
    out = base / '_committed'
    tmp.mkdir(parents=True, exist_ok=True)
    out.mkdir(parents=True, exist_ok=True)

    ts = int(time.time() * 1_000_000)
    tmp_file = tmp / f'{prefix}_{ts}.parquet.tmp'
    final_file = out / f'{prefix}_{ts}.parquet'

    df.write_parquet(tmp_file, compression='zstd', statistics=True)
    os.replace(tmp_file, final_file)

    return final_file
