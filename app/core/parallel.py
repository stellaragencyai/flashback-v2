import multiprocessing
from concurrent.futures import ProcessPoolExecutor

MAX_WORKERS = max(2, multiprocessing.cpu_count() - 1)

EXECUTOR = ProcessPoolExecutor(
    max_workers=MAX_WORKERS,
    mp_context=multiprocessing.get_context("spawn")
)
