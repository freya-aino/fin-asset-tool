import time
import sqlite3
import logging
import pandas as pd
import yfinance as yf
from pathlib import Path
from hashlib import md5

# ----------------------------------------------------------------

def timestamp_s_to_datetime(timestamp):
    return pd.to_datetime(timestamp, unit = "s")

def datetime_to_timestamp_s(datetime):
    return (pd.to_numeric(pd.to_datetime(datetime)) / pow(10, 9)).astype(int)

# ----------------------------------------------------------------

def fetch_all_prices(symbol_list, yf_db):

    tickers = yf.Tickers(symbol_list)

    today = pd.to_datetime("today").date()
    m_start_date = today - pd.to_timedelta(50, "d")
    h_start_date = today - pd.to_timedelta(700, "d")

    date_load = time.time()
    # date_load = datetime_to_timestamp_s([to_datetime(time.time(), unit="s").date()])[0]
    record_source = "yahoo finance"

    symbol_wise_indecies = [[("High", k), ("Low", k), ("Open", k), ("Close", k), ("Dividends", k), ("Stock Splits", k)] for k in symbol_list]

    # ---------------------------------
    
    logging.info("fetch minute data")

    min_hist = tickers.history(start = m_start_date, end = today, interval = "5m", auto_adjust = True, prepost = True)

    for (sym, sym_keys) in zip(symbol_list, symbol_wise_indecies):

        try:
            hub_pk = md5(sym.encode("utf-8")).hexdigest()

            h = pd.DataFrame(min_hist[sym_keys])
            dt = datetime_to_timestamp_s(pd.to_datetime(h.index).strftime("%Y-%m-%d %X"))
            h["date"] = dt
            hh = [dict(list(zip([c[0] for c in h.columns], r.values))) for (i, r) in h.iterrows()]

            sql = ("""
                insert or ignore into sat_instrument_symbol_5m 
                    (PK_hub, date_load, record_source, date,
                        open, high, low, close, volume, dividend, stock_split)
                    values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
                """,
                [(hub_pk, date_load, record_source, a.get("date", None), 
                    a.get("Open", None), a.get("High", None), a.get("Low", None), a.get("Close", None), a.get("Volume", None), 
                    a.get("Dividends", None), a.get("Stock Splits", None)) for a in hh])

            yf_db.executemany(sql[0], sql[1])
            yf_db.commit()

        except Exception as e:
            logging.warning(sym + "not fetched. " + str(e))

    # ---------------------------------

    logging.info("fetch hourly data")

    houre_hist = tickers.history(start = h_start_date, end = today, interval = "1h", auto_adjust = True, prepost = True)

    for (sym, sym_keys) in zip(symbol_list, symbol_wise_indecies):

        try:
            hub_pk = md5(sym.encode("utf-8")).hexdigest()

            h = pd.DataFrame(houre_hist[sym_keys])
            dt = datetime_to_timestamp_s(pd.to_datetime(h.index).strftime("%Y-%m-%d %X"))
            h["date"] = dt
            hh = [dict(list(zip([c[0] for c in h.columns], r.values))) for (i, r) in h.iterrows()]

            sql = ("""
                insert or ignore into sat_instrument_symbol_1h
                    (PK_hub, date_load, record_source, date,
                        open, high, low, close, volume, dividend, stock_split)
                    values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
                """,
                [(hub_pk, date_load, record_source, a.get("date", None), 
                    a.get("Open", None), a.get("High", None), a.get("Low", None), a.get("Close", None), a.get("Volume", None), 
                    a.get("Dividends", None), a.get("Stock Splits", None)) for a in hh])

            yf_db.executemany(sql[0], sql[1])
            yf_db.commit()

        except Exception as e:
            logging.warning(sym + "not fetched. " + str(e))

    # ---------------------------------

    logging.info("fetch daily data")

    day_hist = tickers.history(period="max", interval="1d", auto_adjust = True, prepost = True)

    for (sym, sym_keys) in zip(symbol_list, symbol_wise_indecies):

        try:
            hub_pk = md5(sym.encode("utf-8")).hexdigest()

            h = pd.DataFrame(day_hist[sym_keys])
            dt = datetime_to_timestamp_s(pd.to_datetime(h.index).strftime("%Y-%m-%d %X"))
            h["date"] = dt
            hh = [dict(list(zip([c[0] for c in h.columns], r.values))) for (i, r) in h.iterrows()]

            sql = ("""
                insert or ignore into sat_instrument_symbol_1d 
                    (PK_hub, date_load, record_source, date,
                        open, high, low, close, volume, dividend, stock_split)
                    values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
                """,
                [(hub_pk, date_load, record_source, a.get("date", None), 
                    a.get("Open", None), a.get("High", None), a.get("Low", None), a.get("Close", None), a.get("Volume", None), 
                    a.get("Dividends", None), a.get("Stock Splits", None)) for a in hh])
            
            yf_db.executemany(sql[0], sql[1])
            yf_db.commit()

        except Exception as e:
            logging.warning(sym + "not fetched. " + str(e))

    # wrap_tc(fetch_and_save_daily_ticker, ticker, hub_pk, date_load, record_source, yf_db)
    # # print("fetch_and_save_daily_ticker time: ", dt - time.time())
    # # dt = time.time()

    # wrap_tc(fetch_and_save_hourely_ticker, ticker, hub_pk, date_load, record_source, h_start_date, today, yf_db)
    # # print("fetch_and_save_hourely_ticker time: ", dt - time.time())
    # # dt = time.time()

    # wrap_tc(fetch_and_save_minute_ticker, ticker, hub_pk, date_load, record_source, m_start_date, today, yf_db)
    # # print("fetch_and_save_minute_ticker time: ", dt - time.time())
    # # dt = time.time()

# ----------------------------------------------------------------

if __name__ == "__main__":

    logging.basicConfig(level=logging.INFO)

    symbol_list = pd.read_csv("C:\\datasets\\american_stocks.csv")
    yf_db = sqlite3.connect("C:\\datasets\\yahoo_finance.db")


    # sym_list = " ".join([str(s["Symbol"]) for (_, s) in symbol_list.iterrows()][:10])
    # data = yf.download(sym_list, start="2017-01-01", end="2017-04-30")
    # print(data.index, data.columns)

    logging.info("fetch meta data for: " + str(symbol_list))

    syms = [s["Symbol"] for (_, s) in symbol_list.iterrows()]

    fetch_all_prices(syms, yf_db)
