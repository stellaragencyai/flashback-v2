from __future__ import annotations

import re
from pathlib import Path

TARGET = Path(r"app\sim\paper_broker.py")

def main():
    s = TARGET.read_text(encoding="utf-8", errors="ignore")

    # Replace the existing def main() -> None: ... if __name__ == "__main__": main()
    # with an argparse-based CLI.
    pat = r"(?ms)^def\s+main\(\)\s*->\s*None:\s*\n.*?^\s*if\s+__name__\s*==\s*[\"']__main__[\"']:\s*\n\s*main\(\)\s*\n?\s*$"
    m = re.search(pat, s)
    if not m:
        raise SystemExit("FAIL: could not find main() block to replace in paper_broker.py")

    replacement = r'''def main() -> None:
    """
    PaperBroker CLI (dev/test only)

    Examples:
      python -m app.sim.paper_broker --account flashback01 --force-close-all --exit-price-mode tp
      python -m app.sim.paper_broker --account flashback01 --force-close-all --exit-price-mode sl
      python -m app.sim.paper_broker --account flashback01 --force-close-all --exit-price-mode manual --exit-price 0.50
      python -m app.sim.paper_broker --account flashback01 --poke-price --symbol XRPUSDT --price 0.5012
    """
    import argparse

    p = argparse.ArgumentParser(prog="paper_broker", add_help=True)
    p.add_argument("--account", "--account-label", dest="account_label", default=None, help="Account label (e.g., flashback01)")
    p.add_argument("--force-close-all", action="store_true", help="Force close all open PAPER positions for the account")
    p.add_argument("--exit-price-mode", dest="exit_price_mode", default="entry",
                   choices=["tp", "sl", "entry", "manual"],
                   help="How to choose exit price when force-closing")
    p.add_argument("--exit-price", dest="exit_price", type=float, default=None, help="Manual exit price (required for exit-price-mode=manual)")
    p.add_argument("--reason", dest="reason", default="force_close_all", help="Exit reason label")
    p.add_argument("--poke-price", action="store_true", help="Call update_price(symbol, price) once (to trigger tp/sl closes)")
    p.add_argument("--symbol", dest="symbol", default=None, help="Symbol for --poke-price")
    p.add_argument("--price", dest="price", type=float, default=None, help="Price for --poke-price")

    args = p.parse_args()

    if not args.account_label:
        log.info("PaperBroker CLI: no --account provided. Nothing to do.")
        return

    broker = PaperBroker.load_or_create(args.account_label)

    if args.poke_price:
        if not args.symbol or args.price is None:
            raise SystemExit("FAIL: --poke-price requires --symbol and --price")
        broker.update_price(args.symbol, float(args.price))
        log.info("PaperBroker CLI: poked price symbol=%s price=%.8f", args.symbol, float(args.price))
        return

    if args.force_close_all:
        open_positions = broker.list_open_positions()
        if not open_positions:
            log.info("PaperBroker CLI: no open positions for %s", args.account_label)
            return

        # Determine exit price for each position
        for pos in list(open_positions):
            if args.exit_price_mode == "tp":
                px = float(pos.take_profit_price)
            elif args.exit_price_mode == "sl":
                px = float(pos.stop_price)
            elif args.exit_price_mode == "manual":
                if args.exit_price is None or args.exit_price <= 0:
                    raise SystemExit("FAIL: --exit-price-mode manual requires --exit-price > 0")
                px = float(args.exit_price)
            else:
                px = float(pos.entry_price)

            broker._close_position(pos, exit_price=px, exit_reason=str(args.reason))

        log.info("PaperBroker CLI: force-closed %d positions for %s", len(open_positions), args.account_label)
        return

    log.info("PaperBroker CLI: no action flags provided. Nothing to do.")


if __name__ == "__main__":
    main()
'''
    s2 = re.sub(pat, replacement, s)
    TARGET.write_text(s2, encoding="utf-8")
    print("OK: patched paper_broker.py (added CLI: --force-close-all / --poke-price)")

if __name__ == "__main__":
    main()
