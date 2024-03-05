import os
import sqlite3
import requests
import time
import json
import pandas as pd
from time import sleep
from hashlib import md5
from pathlib import Path
from pandas.core.tools.datetimes import to_datetime

# ------------------------------------------------------------------------

def datetime_to_timestamp_s(datetime):
    return (pd.to_numeric(pd.to_datetime(datetime)) / pow(10, 9)).astype(int)

# ------------------------------------------------------------------------

if __name__ == "__main__":

    key = os.environ["LEMON_MARKET_DEFAULT_KEY"]
    lemon_db = sqlite3.connect("C:\\datasets\\lemon_market\\lemon_markets.db")

    record_source = "lemon_market"
    date_load = datetime_to_timestamp_s([to_datetime(time.time(), unit="s").date()])[0]

    try:

        res = json.loads(requests.get("https://data.lemon.markets/v1/instruments?page=1", headers={"Authorization": "Bearer " + key}).content)
        all_instruments = res["results"]

        for i in range(2, res["pages"]):

            print("fetch:", i - 1, " / ", res["pages"])
            sleep(0.61)

            # ---------------------------------------------------------------

            all_tradable_instruments = [(
                md5(r["isin"].encode("utf-8")).hexdigest(),
                date_load,
                record_source,
                r["isin"]) for r in res["results"] if "isin" in r]

            lemon_db.executemany("""
                insert or ignore into hub_instrument
                    (PK, date_load, record_source, isin)
                    values (?, ?, ?, ?);
            """, all_tradable_instruments)
            lemon_db.commit()

            # ---------------------------------------------------------------

            XMUN_tradable_instruments = [(
                md5(r["isin"].encode("utf-8")).hexdigest(),
                date_load,
                record_source,
                r.get("wkn", None),
                r.get("name", None),
                r.get("title", None),
                r.get("symbol", None),
                r.get("type", None)) for r in res["results"] if "isin" in r and [*[v for v in r["venues"] if v["mic"] == "XMUN"], {}][0].get("tradable", False) ]

            lemon_db.executemany("""
                insert or ignore into sat_XMUN_tradable_instrument_info
                    (PK, date_load, record_source, wkn, 
                    name, title, symbol, type)
                    values (?, ?, ?, ?, ?, ?, ?, ?);
            """, XMUN_tradable_instruments)
            lemon_db.commit()

            # ---------------------------------------------------------------

            res = json.loads(requests.get(res["next"], headers={"Authorization": "Bearer " + key}).content)
    
    except Exception as e:
        print("aborded due to: ", e)