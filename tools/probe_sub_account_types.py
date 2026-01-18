import os
import sys
from pybit.unified_trading import HTTP

def env(k):
    v = os.environ.get(k, "").strip()
    if not v:
        print(f"Missing env var: {k}")
        sys.exit(1)
    return v

def main():
    testnet = os.environ.get("BYBIT_TESTNET", "0").strip() == "1"
    api_key = env("BYBIT_API_KEY")
    api_secret = env("BYBIT_API_SECRET")

    # Sub UIDs you care about (1,2,4,5,6,7,8,9)
    sub_keys = ["BYBIT_SUB_UID_01","BYBIT_SUB_UID_02","BYBIT_SUB_UID_04","BYBIT_SUB_UID_05","BYBIT_SUB_UID_06","BYBIT_SUB_UID_07","BYBIT_SUB_UID_08","BYBIT_SUB_UID_09"]
    sub_uids = []
    for k in sub_keys:
        v = os.environ.get(k, "").strip()
        if v:
            sub_uids.append((k, v))

    if not sub_uids:
        print("No BYBIT_SUB_UID_XX found in environment.")
        sys.exit(1)

    http = HTTP(testnet=testnet, api_key=api_key, api_secret=api_secret)

    # These are the documented accountType values for asset balance queries.
    # We'll probe each until one succeeds for each sub.
    candidates = ["UNIFIED", "CONTRACT", "SPOT", "FUND", "OPTION", "INVESTMENT"]

    print(f"testnet={testnet}")
    for k, uid in sub_uids:
        print(f"\n== {k} (uid={uid}) ==")
        for acct in candidates:
            try:
                # Using "get_all_coins_balance" style endpoint
                # Some pybit versions name it get_all_coins_balance or get_all_coins_balance
                # We'll try the common method name first; if your pybit differs, you’ll see it immediately.
                resp = http.get_all_coins_balance(memberId=uid, accountType=acct, coin="USDT")
                rc = resp.get("retCode")
                msg = resp.get("retMsg")
                if rc == 0:
                    print(f"OK  accountType={acct}")
                    break
                else:
                    print(f"NO  accountType={acct}  retCode={rc} retMsg={msg}")
            except Exception as e:
                print(f"ERR accountType={acct}  {repr(e)}")

if __name__ == "__main__":
    main()
