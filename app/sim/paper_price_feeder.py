import time
import logging

log = logging.getLogger("paper_price_feeder")

def loop():
    log.info("paper_price_feeder stub active (no-op)")
    while True:
        time.sleep(5)
