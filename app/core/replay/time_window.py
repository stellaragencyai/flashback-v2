import polars as pl

def with_time_window(lf: pl.LazyFrame, start_ms: int | None, end_ms: int | None):
    if start_ms is not None:
        lf = lf.filter(pl.col('received_ms') >= start_ms)
    if end_ms is not None:
        lf = lf.filter(pl.col('received_ms') <= end_ms)
    return lf
