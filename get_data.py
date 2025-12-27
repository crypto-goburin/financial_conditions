import requests
import pandas as pd
import datetime
import time

def to_milliseconds(dt):
    return int(time.mktime(dt.timetuple()) * 1000)

def fetch_all_candles(symbol, timeframe, start, end, limit=1000):
    url = f"https://api.bitfinex.com/v2/candles/trade:{timeframe}:{symbol}/hist"
    all_data = []
    current_end = end

    while current_end > start:
        params = {
            "end": current_end,
            "limit": limit,
            "sort": -1  # descending order (newest first)
        }
        resp = requests.get(url, params=params)
        batch = resp.json()
        if not batch:
            break

        all_data.extend(batch)
        print(f"Fetched {len(batch)} candles ending at {pd.to_datetime(current_end, unit='ms')}")

        # Move the end marker back in time to the oldest candle we just got
        oldest_ts = batch[-1][0]
        current_end = oldest_ts - 1
        time.sleep(1.5)

    return all_data

# -----------------------------
# Usage
pair = "tETHUSD"
timeframe = "1h"

t_start = to_milliseconds(datetime.datetime(2016, 6, 14))
t_end   = to_milliseconds(datetime.datetime(2025, 11, 26))

data = fetch_all_candles(pair, timeframe, t_start, t_end)

columns = ["Date", "Open", "Close", "High", "Low", "Volume"]
df = pd.DataFrame(data, columns=columns)
df["Date"] = pd.to_datetime(df["Date"], unit="ms")
df.set_index("Date", inplace=True)
df.sort_index(inplace=True)

df.to_csv(f"data/{pair}_{timeframe}.csv")
print(f"Saved {len(df)} rows")
