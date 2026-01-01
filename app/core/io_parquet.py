import polars as pl
from pathlib import Path

def read_parquet(path, columns=None, filters=None):
    path = Path(path)

    # Predicate path → scan_parquet (pushdown happens here)
    if filters is not None:
        lf = pl.scan_parquet(path)
        if columns is not None:
            lf = lf.select(columns)
        return lf.filter(filters).collect()

    # Simple read path → read_parquet (adaptive mmap)
    size = path.stat().st_size if path.exists() else 0
    mmap = size > 200_000_000

    return pl.read_parquet(
        path,
        columns=columns,
        memory_map=mmap
    )
