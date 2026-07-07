import requests
import pandas as pd
from datetime import datetime
from pathlib import Path
import time

COINS = {
    "BTC": "BTC",
}

OUTPUT_DIR = Path("daily_prices")
OUTPUT_DIR.mkdir(exist_ok=True)

MIN_DELAY = 0.8  # để tránh spam

def fetch_2024_daily(symbol):

    print(f"\n===== Fetch {symbol}  =====")

    url = (
        f"https://min-api.cryptocompare.com/data/v2/histoday"
        f"?fsym={symbol}&tsym=USD&limit=2000"
    )

    r = requests.get(url)
    time.sleep(MIN_DELAY)

    js = r.json()

    if js["Response"] != "Success":
        print("API error:", js)
        return

    data = js["Data"]["Data"]

    rows = []
    for row in data:
        ts = row["time"]
        dt = datetime.utcfromtimestamp(ts).date()

        if dt.year:
            rows.append({
                "date": str(dt),
                "symbol": symbol,
                "open": row["open"],
                "high": row["high"],
                "low": row["low"],
                "close": row["close"],
                "volume": row["volumeto"],
            })

    df = pd.DataFrame(rows)
    df.to_csv(OUTPUT_DIR / f"{symbol}.csv", index=False)

    print(f"Done → {OUTPUT_DIR}/{symbol}.csv")


def main():
    for symbol in COINS.values():
        fetch_2024_daily(symbol)


if __name__ == "__main__":
    main()
