import requests
import json
import sqlite3
import pandas as pd
from time import sleep
from pathlib import Path


lemon_db = sqlite3.connect(str(Path(__file__).absolute().parent) + "\\..\\..\\data\\lemon_markets.db")
key = json.loads(open(str(Path(__file__).absolute().parent) + "\\..\\..\\secrets\\lemon_keys.json").read())["default"]

isin_list = ["XS1188081936"]

end_date = pd.to_datetime("today").date()
start_date = end_date - pd.to_timedelta(350, "d")

# try:

for isin in isin_list:

    req_1d = requests.get("https://data.lemon.markets/v1/ohlc/d1/?isin=" + isin + "&from=" + str(start_date) + "&to=" + str(end_date), headers={"Authorization": "Bearer " + key})
    req_1h = requests.get("https://data.lemon.markets/v1/ohlc/h1/?isin=" + isin + "&from=" + str(start_date) + "&to=" + str(end_date), headers={"Authorization": "Bearer " + key})
    req_1m = requests.get("https://data.lemon.markets/v1/ohlc/m1/?isin=" + isin + "&from=" + str(start_date) + "&to=" + str(end_date), headers={"Authorization": "Bearer " + key})

    if req_1h.status_code >= 200 and req_1h.status_code < 300:
        res = json.loads(req_1h.content)
        ohlc = res["results"]
        ohlc

        lemon_db.executemany("""
            insert or ignore into sat_instrument_symbol_1h

        """, [])

        # TODO

        sleep(0.61 * 3)

    else:

        print(req.reason, req.content)


# except Exception as e:
#     print("aborded due to: ", e)