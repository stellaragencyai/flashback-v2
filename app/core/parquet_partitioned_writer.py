import os, time
import polars as pl
from pathlib import Path
from datetime import datetime

def write_partitioned_chunk(df: pl.DataFrame, base_dir: str, prefix: str):
    if 'symbol' not in df.columns or 'received_ms' not in df.columns:
        raise ValueError('required columns: symbol, received_ms')

    df = df.with_columns(
        pl.from_epoch('received_ms', time_unit='ms').dt.strftime('%Y-%m-%d').alias('day')
    )

    base = Path(base_dir) / '_committed'

    for (symbol, day), part in df.partition_by(['symbol', 'day'], as_dict=True).items():
        out = base / f'symbol={symbol}' / f'day={day}'
        out.mkdir(parents=True, exist_ok=True)

        ts = int(time.time() * 1_000_000)
        tmp = out / f'{prefix}_{ts}.parquet.tmp'
        final = out / f'{prefix}_{ts}.parquet'

        part.write_parquet(tmp, compression='zstd', statistics=True)
        os.replace(tmp, final)

    return True
