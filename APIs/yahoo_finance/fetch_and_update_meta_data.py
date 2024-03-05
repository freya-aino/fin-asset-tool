import time
import sqlite3
import logging
import pandas as pd
import yfinance as yf
from pandas.core.tools.datetimes import to_datetime
from hashlib import md5
from pathlib import Path


# ------------------------------------------------------------

def timestamp_s_to_datetime(timestamp):
    return pd.to_datetime(timestamp, unit = "s")

def datetime_to_timestamp_s(datetime):
    return (pd.to_numeric(pd.to_datetime(datetime)) / pow(10, 9)).astype(int)

def wrap_tc(f, *args):
    try:
        f(*args)
    except Exception as e:
        logging.warning(e)
        
# ------------------------------------------------------------

def fetch_and_save_hub(ticker, hub_pk, date_load, record_source, symbol, yf_db):
    
    isin = ticker.isin
    if isin == "-":
        isin = None

    # ------------------------- hub_instrument

    hub_sql = ("""
        insert or replace into hub_instrument 
            (PK, date_load, record_source, symbol, isin) 
            values (?, ?, ?, ?, ?);""",
    (hub_pk, date_load, record_source, symbol, None))


    yf_db.execute(hub_sql[0], hub_sql[1])
    yf_db.commit()

def fetch_and_save_meta_infos(ticker, hub_pk, date_load, record_source, yf_db):

    # ------------------------- sat_instrument_info_meta

    info = ticker.info

    # [("zip", "95014"), ("sector", "Technology"), ("fullTimeEmployees", 154000), ("longBusinessSummary", "Apple Inc. designs, manufactures, and markets smartphones, personal computers, tablets, wearables, and accessories worldwide. It also sells various related services. In addition, the company offers iPhone, a line of smartphones; Mac, a line of personal computers; iPad, a line of multi-purpose tablets; AirPods Max, an over-ear wireless headphone; and wearables, home, and accessories comprising AirPods, Apple TV, Apple Watch, Beats products, HomePod, and iPod touch. Further, it provides AppleCare support services; cloud services store services; and operates various platforms, including the App Store that allow customers to discover and download applications and digital content, such as books, music, video, games, and podcasts. Additionally, the company offers various services, such as Apple Arcade, a game subscription service; Apple Music, which offers users a curated listening experience with on-demand radio stations; Apple News+, a subscription news and magazine service; Apple TV+, which offers exclusive original content; Apple Card, a co-branded credit card; and Apple Pay, a cashless payment service, as well as licenses its 
    # intellectual property. The company serves consumers, and small and mid-sized businesses; and the education, enterprise, and government markets. It distributes third-party applications for its products through the App Store. The company also sells its 
    # products through its retail and online stores, and direct sales force; and third-party cellular network carriers, wholesalers, retailers, and resellers. Apple Inc. was incorporated in 1977 and is headquartered in Cupertino, California."), ("city", "Cupertino"), ("phone", "408 996 1010"), ("state", "CA"), ("country", "United States"), ("companyOfficers", []), ("website", "https://www.apple.com"), ("maxAge", 1), ("address1", "One Apple Park Way"), ("industry", "Consumer Electronics"), ("ebitdaMargins", 0.32867), ("profitMargins", 0.25882), ("grossMargins", 0.41779), ("operatingCashflow", 104037998592), ("revenueGrowth", 0.288), ("operatingMargins", 0.29782), ("ebitda", 120233000960), ("targetLowPrice", 128.01), ("recommendationKey", "buy"), ("grossProfits", 152836000000), ("freeCashflow", 73295003648), ("targetMedianPrice", 172), ("currentPrice", 171.14), ("earningsGrowth", 0.662), ("currentRatio", 1.075), ("returnOnAssets", 0.20179), ("numberOfAnalystOpinions", 42), ("targetMeanPrice", 174.22), ("debtToEquity", 216.392), ("returnOnEquity", 1.47443), ("targetHighPrice", 210), ("totalCash", 62639001600), ("totalDebt", 136521998336), ("totalRevenue", 365817004032), ("totalCashPerShare", 3.818), ("financialCurrency", "USD"), ("revenuePerShare", 21.904), ("quickRatio", 0.91), ("recommendationMean", 1.8), ("exchange", "NMS"), ("shortName", "Apple Inc."), ("longName", "Apple Inc."), ("exchangeTimezoneName", "America/New_York"), ("exchangeTimezoneShortName", "EST"), ("isEsgPopulated", False), ("gmtOffSetMilliseconds", "-18000000"), ("quoteType", "EQUITY"), ("symbol", "AAPL"), ("messageBoardId", "finmb_24937"), ("market", "us_market"), ("annualHoldingsTurnover", None), ("enterpriseToRevenue", 7.877), ("beta3Year", None), ("enterpriseToEbitda", 23.967), ("52WeekChange", 0.3346331), ("morningStarRiskRating", None), ("forwardEps", 6.18), ("revenueQuarterlyGrowth", None), ("sharesOutstanding", 16406400000), ("fundInceptionDate", None), ("annualReportExpenseRatio", None), ("totalAssets", 
    # None), ("bookValue", 3.841), ("sharesShort", 112598907), ("sharesPercentSharesOut", 0.0069), ("fundFamily", None), ("lastFiscalYearEnd", 1632528000), ("heldPercentInstitutions", 0.58786), ("netIncomeToCommon", 94679998464), ("trailingEps", 5.61), ("lastDividendValue", 0.22), ("SandP52WeekChange", 0.2505386), ("priceToBook", 44.556103), ("heldPercentInsiders", 0.00071000005), ("nextFiscalYearEnd", 1695600000), ("yield", None), ("mostRecentQuarter", 1632528000), ("shortRatio", 1.4), ("sharesShortPreviousMonthDate", 1635465600), ("floatShares", 16389662475), ("beta", 1.203116), ("enterpriseValue", 2881673756672), ("priceHint", 2), ("threeYearAverageReturn", None), ("lastSplitDate", 1598832000), ("lastSplitFactor", "4:1"), ("legalType", None), ("lastDividendDate", 1636070400), ("morningStarOverallRating", None), ("earningsQuarterlyGrowth", 0.622), ("priceToSalesTrailing12Months", 7.675399), ("dateShortInterest", 1638230400), ("pegRatio", 1.91), ("ytdReturn", None), ("forwardPE", 27.692556), ("lastCapGain", None), ("shortPercentOfFloat", 0.0069), ("sharesShortPriorMonth", 100495541), ("impliedSharesOutstanding", None), ("category", None), ("fiveYearAverageReturn", None), ("previousClose", 172.26), ("regularMarketOpen", 169.93), ("twoHundredDayAverage", 141.48285), ("trailingAnnualDividendYield", 0.0049344017), ("payoutRatio", 0.1515), ("volume24Hr", None), ("regularMarketDayHigh", 173.47), ("navPrice", None), ("averageDailyVolume10Day", 133835230), ("regularMarketPreviousClose", 172.26), ("fiftyDayAverage", 156.335), ("trailingAnnualDividendRate", 0.85), ("open", 169.93), ("toCurrency", None), ("averageVolume10days", 133835230), ("expireDate", None), ("algorithm", None), ("dividendRate", 0.88), ("exDividendDate", 1636070400), ("circulatingSupply", None), ("startDate", None), ("regularMarketDayLow", 169.69), ("currency", "USD"), ("trailingPE", 30.506239), ("regularMarketVolume", 195923241), ("lastMarket", None), ("maxSupply", None), ("openInterest", None), ("marketCap", 2807791353856), ("volumeAllCurrencies", None), ("strikePrice", None), ("averageVolume", 89710154), ("dayLow", 169.69), ("ask", 171.9), ("askSize", 1000), ("volume", 195923241), ("fiftyTwoWeekHigh", 182.13), ("fromCurrency", None), ("fiveYearAvgDividendYield", 
    # 1.2), ("fiftyTwoWeekLow", 116.21), ("bid", 171.89), ("tradeable", False), ("dividendYield", 0.0050999997), ("bidSize", 800), ("dayHigh", 173.47), ("regularMarketPrice", 171.14), ("preMarketPrice", None), ("logo_url", "https://logo.clearbit.com/apple.com")]

    meta_sql = ("""
        insert or ignore into sat_instrument_info_meta
            (PK_hub, date_load, record_source, symbol, 
            short_name, long_name, zip_code, city, 
            phone_number, sector, long_business_summary, 
            country, address1, address2, industry, 
            full_time_employees, gmt_offset_milliseconds, 
            quote_type, market, currency)
            values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
        """,
    (hub_pk, date_load, record_source, info.get("symbol", None), 
        info.get("shortName", None), info.get("longName", None), info.get("zip", None), info.get("city", None), 
        info.get("phone", None), info.get("sector", None), info.get("longBusinessSummary", None), 
        info.get("country", None), info.get("address1", None), info.get("address2", None), info.get("industry", None), 
        info.get("fullTimeEmployees", None), info.get("gmtOffSetMilliseconds", None), 
        info.get("quoteType", None), info.get("market", None), info.get("currency", None)))
        
    yf_db.execute(meta_sql[0], meta_sql[1])
    yf_db.commit()

def fetch_and_save_technical_infos(ticker, hub_pk, date_load, record_source, yf_db):

    # ------------------------- sat_instrument_info_financial

    info = ticker.info

    # [("zip", "95014"), ("sector", "Technology"), ("fullTimeEmployees", 154000), ("longBusinessSummary", "Apple Inc. designs, manufactures, and markets smartphones, personal computers, tablets, wearables, and accessories worldwide. It also sells various related services. In addition, the company offers iPhone, a line of smartphones; Mac, a line of personal computers; iPad, a line of multi-purpose tablets; AirPods Max, an over-ear wireless headphone; and wearables, home, and accessories comprising AirPods, Apple TV, Apple Watch, Beats products, HomePod, and iPod touch. Further, it provides AppleCare support services; cloud services store services; and operates various platforms, including the App Store that allow customers to discover and download applications and digital content, such as books, music, video, games, and podcasts. Additionally, the company offers various services, such as Apple Arcade, a game subscription service; Apple Music, which offers users a curated listening experience with on-demand radio stations; Apple News+, a subscription news and magazine service; Apple TV+, which offers exclusive original content; Apple Card, a co-branded credit card; and Apple Pay, a cashless payment service, as well as licenses its 
    # intellectual property. The company serves consumers, and small and mid-sized businesses; and the education, enterprise, and government markets. It distributes third-party applications for its products through the App Store. The company also sells its 
    # products through its retail and online stores, and direct sales force; and third-party cellular network carriers, wholesalers, retailers, and resellers. Apple Inc. was incorporated in 1977 and is headquartered in Cupertino, California."), ("city", "Cupertino"), ("phone", "408 996 1010"), ("state", "CA"), ("country", "United States"), ("companyOfficers", []), ("website", "https://www.apple.com"), ("maxAge", 1), ("address1", "One Apple Park Way"), ("industry", "Consumer Electronics"), ("ebitdaMargins", 0.32867), ("profitMargins", 0.25882), ("grossMargins", 0.41779), ("operatingCashflow", 104037998592), ("revenueGrowth", 0.288), ("operatingMargins", 0.29782), ("ebitda", 120233000960), ("targetLowPrice", 128.01), ("recommendationKey", "buy"), ("grossProfits", 152836000000), ("freeCashflow", 73295003648), ("targetMedianPrice", 172), ("currentPrice", 171.14), ("earningsGrowth", 0.662), ("currentRatio", 1.075), ("returnOnAssets", 0.20179), ("numberOfAnalystOpinions", 42), ("targetMeanPrice", 174.22), ("debtToEquity", 216.392), ("returnOnEquity", 1.47443), ("targetHighPrice", 210), ("totalCash", 62639001600), ("totalDebt", 136521998336), ("totalRevenue", 365817004032), ("totalCashPerShare", 3.818), ("financialCurrency", "USD"), ("revenuePerShare", 21.904), ("quickRatio", 0.91), ("recommendationMean", 1.8), ("exchange", "NMS"), ("shortName", "Apple Inc."), ("longName", "Apple Inc."), ("exchangeTimezoneName", "America/New_York"), ("exchangeTimezoneShortName", "EST"), ("isEsgPopulated", False), ("gmtOffSetMilliseconds", "-18000000"), ("quoteType", "EQUITY"), ("symbol", "AAPL"), ("messageBoardId", "finmb_24937"), ("market", "us_market"), ("annualHoldingsTurnover", None), ("enterpriseToRevenue", 7.877), ("beta3Year", None), ("enterpriseToEbitda", 23.967), ("52WeekChange", 0.3346331), ("morningStarRiskRating", None), ("forwardEps", 6.18), ("revenueQuarterlyGrowth", None), ("sharesOutstanding", 16406400000), ("fundInceptionDate", None), ("annualReportExpenseRatio", None), ("totalAssets", 
    # None), ("bookValue", 3.841), ("sharesShort", 112598907), ("sharesPercentSharesOut", 0.0069), ("fundFamily", None), ("lastFiscalYearEnd", 1632528000), ("heldPercentInstitutions", 0.58786), ("netIncomeToCommon", 94679998464), ("trailingEps", 5.61), ("lastDividendValue", 0.22), ("SandP52WeekChange", 0.2505386), ("priceToBook", 44.556103), ("heldPercentInsiders", 0.00071000005), ("nextFiscalYearEnd", 1695600000), ("yield", None), ("mostRecentQuarter", 1632528000), ("shortRatio", 1.4), ("sharesShortPreviousMonthDate", 1635465600), ("floatShares", 16389662475), ("beta", 1.203116), ("enterpriseValue", 2881673756672), ("priceHint", 2), ("threeYearAverageReturn", None), ("lastSplitDate", 1598832000), ("lastSplitFactor", "4:1"), ("legalType", None), ("lastDividendDate", 1636070400), ("morningStarOverallRating", None), ("earningsQuarterlyGrowth", 0.622), ("priceToSalesTrailing12Months", 7.675399), ("dateShortInterest", 1638230400), ("pegRatio", 1.91), ("ytdReturn", None), ("forwardPE", 27.692556), ("lastCapGain", None), ("shortPercentOfFloat", 0.0069), ("sharesShortPriorMonth", 100495541), ("impliedSharesOutstanding", None), ("category", None), ("fiveYearAverageReturn", None), ("previousClose", 172.26), ("regularMarketOpen", 169.93), ("twoHundredDayAverage", 141.48285), ("trailingAnnualDividendYield", 0.0049344017), ("payoutRatio", 0.1515), ("volume24Hr", None), ("regularMarketDayHigh", 173.47), ("navPrice", None), ("averageDailyVolume10Day", 133835230), ("regularMarketPreviousClose", 172.26), ("fiftyDayAverage", 156.335), ("trailingAnnualDividendRate", 0.85), ("open", 169.93), ("toCurrency", None), ("averageVolume10days", 133835230), ("expireDate", None), ("algorithm", None), ("dividendRate", 0.88), ("exDividendDate", 1636070400), ("circulatingSupply", None), ("startDate", None), ("regularMarketDayLow", 169.69), ("currency", "USD"), ("trailingPE", 30.506239), ("regularMarketVolume", 195923241), ("lastMarket", None), ("maxSupply", None), ("openInterest", None), ("marketCap", 2807791353856), ("volumeAllCurrencies", None), ("strikePrice", None), ("averageVolume", 89710154), ("dayLow", 169.69), ("ask", 171.9), ("askSize", 1000), ("volume", 195923241), ("fiftyTwoWeekHigh", 182.13), ("fromCurrency", None), ("fiveYearAvgDividendYield", 
    # 1.2), ("fiftyTwoWeekLow", 116.21), ("bid", 171.89), ("tradeable", False), ("dividendYield", 0.0050999997), ("bidSize", 800), ("dayHigh", 173.47), ("regularMarketPrice", 171.14), ("preMarketPrice", None), ("logo_url", "https://logo.clearbit.com/apple.com")]

    info_fin_sql = ("""
        insert or ignore into sat_instrument_info_technical
            (PK_hub, date_load, record_source, ebitda_margins, 
            beta, held_percent_insiders, held_percent_institutions, 
            profit_margins, gross_margins, operating_cashflow, 
            revenue_growth, operating_margins, market_cap, ebitda, 
            target_low_price, target_median_price, target_mean_price, 
            target_high_price, recommendation_mean, recommendation_key, 
            gross_profits, free_cashflow, earnings_growth, 
            current_ratio, return_on_assets, number_of_analyst_opinions, 
            return_on_equity, total_cash, total_debt, 
            total_revenue, total_cash_per_share, revenue_per_share, 
            enterprise_value, shares_outstanding, 
            last_fiscal_year_end, next_fiscal_year_end,
            last_dividend_value, float_shares, last_split_date, last_split_factor, last_dividend_date, 
            payout_ratio, forward_eps, book_value, yield)
            values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
        """,
    (hub_pk, date_load, record_source, info.get("ebitdaMargins", None), 
        info.get("beta", None), info.get("heldPercentInsiders", None), info.get("heldPercentInstitutions", None), 
        info.get("profitMargins", None), info.get("grossMargins", None), info.get("operatingCashflow", None), 
        info.get("revenueGrowth", None), info.get("operatingMargins", None), info.get("marketCap", None), info.get("ebitda", None), 
        info.get("targetLowPrice", None), info.get("targetMedianPrice", None), info.get("targetMeanPrice", None), 
        info.get("targetHighPrice", None), info.get("recommendationMean", None), info.get("recommendationKey", None), 
        info.get("grossProfits", None), info.get("freeCashflow", None), info.get("earningsGrowth", None), 
        info.get("currentRatio", None), info.get("returnOnAssets", None), info.get("numberOfAnalystOpinions", None), 
        info.get("returnOnEquity", None), info.get("totalCash", None), info.get("totalDebt", None), 
        info.get("totalRevenue", None), info.get("totalCashPerShare", None), info.get("revenuePerShare", None), 
        info.get("enterpriseValue", None), info.get("sharesOutstanding", None), 
        info.get("lastFiscalYearEnd", None), info.get("nextFiscalYearEnd", None), 
        info.get("lastDividendValue", None), info.get("floatShares", None), info.get("lastSplitDate", None), info.get("lastSplitFactor", None), info.get("lastDividendDate", None), 
        info.get("payoutRatio", None), info.get("forwardEps", None), info.get("bookValue", None), info.get("yield", None)))

    yf_db.execute(info_fin_sql[0], info_fin_sql[1])
    yf_db.commit()

def fetch_and_save_metric_infos(ticker, hub_pk, date_load, record_source, yf_db):

    # ------------------------- sat_instrument_info_metrics

    info = ticker.info

    info_metric_sql = ("""
        insert or ignore into sat_instrument_info_metrics 
            (PK_hub, date_load, record_source, fifty_two_week_change, beta_3_year,
            sand_p_52_week_change, price_to_sales_trailing_12_months, 
            enterprise_to_revenue, enterprise_to_ebitda, debt_to_equity, 
            peg_ratio, quick_ratio, net_income_to_common, regular_market_open, 
            two_hundred_day_average, regular_market_day_high, regular_market_previous_close, 
            fifty_day_average, average_daily_volume_10_day, average_volume_10_days, 
            dividend_rate, ex_dividend_date, regular_market_day_low, regular_market_volume, 
            average_volume, fifty_two_week_high, fifty_two_week_low, five_year_avg_dividend_yield, 
            dividend_yield, regularMarketPrice, trailing_annual_dividend_yield, 
            trailing_annual_dividend_rate, pre_market_price, trailing_eps, trailing_pe, 
            forward_pe, price_to_book, short_ratio, three_year_average_return,
            five_year_average_return, short_percentage_of_float, circulating_supply)
            values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
        """,
    (hub_pk, date_load, record_source, info.get("52WeekChange", None), info.get("beta3Year", None),
        info.get("SandP52WeekChange", None), info.get("priceToSalesTrailing12Months", None), 
        info.get("enterpriseToRevenue", None), info.get("enterpriseToEbitda", None), info.get("debtToEquity", None), 
        info.get("pegRatio", None), info.get("quickRatio", None), info.get("netIncomeToCommon", None), info.get("regularMarketOpen", None), 
        info.get("twoHundredDayAverage", None), info.get("regularMarketDayHigh", None), info.get("regularMarketPreviousClose", None), 
        info.get("fiftyDayAverage", None), info.get("averageDailyVolume10Day", None), info.get("averageVolume10days", None), 
        info.get("dividendRate", None), info.get("exDividendDate", None), info.get("regularMarketDayLow", None), info.get("regularMarketVolume", None), 
        info.get("averageVolume", None), info.get("fiftyTwoWeekHigh", None), info.get("fiftyTwoWeekLow", None), info.get("fiveYearAvgDividendYield", None), 
        info.get("dividendYield", None), info.get("regularMarketPrice", None), info.get("trailingAnnualDividendYield", None), 
        info.get("trailingAnnualDividendRate", None), info.get("preMarketPrice", None), info.get("trailingEps", None), info.get("trailingPE", None), 
        info.get("forwardPE", None), info.get("priceToBook", None), info.get("shortRatio", None), info.get("threeYearAverageReturn", None),
        info.get("fiveYearAverageReturn", None), info.get("shortPercentOfFloat", None), info.get("circulatingSupply", None)))

    yf_db.execute(info_metric_sql[0], info_metric_sql[1])
    yf_db.commit()

def fetch_and_save_dividends(ticker, hub_pk, date_load, record_source, yf_db):

    # ------------------------- sat_instrument_dividend

    dividends = ticker.dividends
    dividends = list(zip(datetime_to_timestamp_s(dividends.index), dividends.values))

    # [(547689600, 0.000536), (555552000, 0.000536), (564105600, 0.000714), (571622400, 0.000714), (579744000, 0.000714), (587606400, 0.000714), (596073600, 0.000893), (603676800, 0.000893), (611798400, 0.000893), (619660800, 0.000893), (627264000, 0.000982), (635126400, 0.000982), (643248000, 0.000982), (651110400, 0.000982), (658713600, 0.001071), (666576000, 0.001071), (674697600, 0.001071), (682560000, 0.001071), (690422400, 0.001071), (698025600, 0.001071), (707356800, 0.001071), (714009600, 0.001071), (723081600, 0.001071), (729475200, 0.001071), (738547200, 0.001071), (745459200, 0.001071), (753667200, 0.001071), (760579200, 0.001071), (769996800, 0.001071), (776908800, 0.001071), (785116800, 0.001071), (792633600, 0.001071), (801446400, 0.001071), (808531200, 0.001071), (816912000, 0.001071), (1344470400, 0.094643), (1352246400, 0.094643), (1360195200, 0.094643), (1368057600, 0.108929), (1375920000, 0.108929), (1383696000, 0.108929), (1391644800, 0.108929), (1399507200, 0.1175), (1407369600, 0.1175), (1415232000, 0.1175), (1423094400, 0.1175), (1430956800, 0.13), (1438819200, 0.13), (1446681600, 0.13), (1454544000, 0.13), (1462406400, 0.1425), (1470268800, 0.1425), (1478131200, 0.1425), (1486598400, 0.1425), (1494460800, 0.1575), (1502323200, 0.1575), (1510272000, 0.1575), (1518134400, 0.1575), (1525996800, 0.1825), (1533859200, 0.1825), (1541635200, 0.1825), (1549584000, 0.1825), (1557446400, 0.1925), (1565308800, 0.1925), (1573084800, 0.1925), (1581033600, 0.1925), (1588896000, 0.205), (1596758400, 0.205), (1604620800, 0.205), (1612483200, 0.205), (1620345600, 0.22), (1628208000, 0.22), (1636070400, 0.22)]

    dividend_sql = ("""
        insert or ignore into sat_instrument_dividend 
            (PK_hub, date_load, record_source, date, dividend)
            values (?, ?, ?, ?, ?);
        """,
        [(hub_pk, date_load, record_source, a[0], a[1]) for a in dividends])

    yf_db.executemany(dividend_sql[0], dividend_sql[1]) # many
    yf_db.commit()

def fetch_and_save_splits(ticker, hub_pk, date_load, record_source, yf_db):

    # ------------------------- sat_instrument_split

    splits = ticker.splits
    splits = list(zip(datetime_to_timestamp_s(splits.index), splits.values))

    # [(550800000, 2.0), (961545600, 2.0), (1109548800, 2.0), (1402272000, 7.0), (1598832000, 4.0)]

    split_sql = ("""
        insert or ignore into sat_instrument_split 
            (PK_hub, date_load, record_source, date, split)
            values (?, ?, ?, ?, ?);
        """,
        [(hub_pk, date_load, record_source, a[0], a[1]) for a in splits])

    yf_db.executemany(split_sql[0], split_sql[1]) # many
    yf_db.commit()

def fetch_and_save_financials_year(ticker, hub_pk, date_load, record_source, yf_db):

    # ------------------------- sat_instrument_financials_year

    financials = ticker.financials
    financials = [dict([("date", datetime_to_timestamp_s([c])[0])] + list(zip(financials.index, financials[c]))) for c in financials.columns]

    # [{"date": 1632528000, "Research Development": 21914000000.0, "Effect Of Accounting Charges": None, "Income Before Tax": 109207000000.0, "Minority Interest": None, "Net Income": 94680000000.0, "Selling General Administrative": 21973000000.0, "Gross Profit": 152836000000.0, "Ebit": 108949000000.0, "Operating Income": 108949000000.0, "Other Operating Expenses": None, "Interest Expense": -2645000000.0, "Extraordinary Items": None, "Non Recurring": None, "Other Items": None, "Income Tax Expense": 14527000000.0, "Total Revenue": 365817000000.0, "Total Operating Expenses": 256868000000.0, "Cost Of Revenue": 212981000000.0, "Total Other Income Expense Net": 258000000.0, "Discontinued Operations": None, "Net Income From Continuing Ops": 94680000000.0, "Net Income Applicable To Common 
    # Shares": 94680000000.0}, {"date": 1601078400, "Research Development": 18752000000.0, "Effect Of Accounting Charges": None, "Income Before Tax": 67091000000.0, "Minority Interest": None, "Net Income": 57411000000.0, "Selling General Administrative": 19916000000.0, "Gross Profit": 104956000000.0, "Ebit": 66288000000.0, "Operating Income": 66288000000.0, "Other Operating Expenses": None, "Interest Expense": -2873000000.0, "Extraordinary Items": None, "Non Recurring": None, "Other Items": None, "Income Tax Expense": 9680000000.0, "Total Revenue": 274515000000.0, "Total Operating Expenses": 208227000000.0, "Cost Of Revenue": 169559000000.0, "Total Other Income Expense Net": 803000000.0, "Discontinued Operations": None, "Net Income From Continuing Ops": 57411000000.0, "Net Income Applicable To Common Shares": 57411000000.0}, {"date": 1569628800, "Research Development": 16217000000.0, "Effect Of Accounting Charges": None, "Income Before Tax": 65737000000.0, "Minority Interest": None, "Net Income": 55256000000.0, "Selling General Administrative": 18245000000.0, "Gross Profit": 98392000000.0, "Ebit": 63930000000.0, "Operating Income": 63930000000.0, "Other Operating Expenses": None, "Interest Expense": -3576000000.0, "Extraordinary Items": None, "Non Recurring": None, "Other Items": None, "Income Tax Expense": 10481000000.0, "Total Revenue": 260174000000.0, "Total Operating Expenses": 196244000000.0, "Cost Of Revenue": 161782000000.0, "Total Other Income Expense Net": 1807000000.0, "Discontinued Operations": None, "Net Income From Continuing Ops": 55256000000.0, "Net Income Applicable To Common Shares": 55256000000.0}, {"date": 1538179200, "Research Development": 14236000000.0, "Effect Of Accounting Charges": None, "Income Before Tax": 72903000000.0, "Minority Interest": None, "Net Income": 59531000000.0, "Selling General Administrative": 16705000000.0, "Gross Profit": 101839000000.0, "Ebit": 70898000000.0, "Operating Income": 70898000000.0, "Other Operating Expenses": None, "Interest Expense": -3240000000.0, "Extraordinary Items": None, "Non Recurring": None, "Other Items": None, "Income Tax Expense": 13372000000.0, "Total Revenue": 265595000000.0, "Total Operating Expenses": 194697000000.0, "Cost Of Revenue": 163756000000.0, "Total Other Income Expense Net": 2005000000.0, "Discontinued Operations": None, "Net Income From Continuing Ops": 59531000000.0, "Net Income Applicable To Common Shares": 59531000000.0}]

    #                                             2021-09-25      2020-09-26      2019-09-28      2018-09-29
    # Research Development                     21914000000.0   18752000000.0   16217000000.0   14236000000.0
    # Effect Of Accounting Charges                      None            None            None            None
    # Income Before Tax                       109207000000.0   67091000000.0   65737000000.0   72903000000.0    
    # Minority Interest                                 None            None            None            None
    # Net Income                               94680000000.0   57411000000.0   55256000000.0   59531000000.0
    # Selling General Administrative           21973000000.0   19916000000.0   18245000000.0   16705000000.0
    # Gross Profit                            152836000000.0  104956000000.0   98392000000.0  101839000000.0
    # Ebit                                    108949000000.0   66288000000.0   63930000000.0   70898000000.0
    # Operating Income                        108949000000.0   66288000000.0   63930000000.0   70898000000.0
    # Other Operating Expenses                          None            None            None            None
    # Interest Expense                         -2645000000.0   -2873000000.0   -3576000000.0   -3240000000.0
    # Extraordinary Items                               None            None            None            None
    # Non Recurring                                     None            None            None            None
    # Other Items                                       None            None            None            None
    # Income Tax Expense                       14527000000.0    9680000000.0   10481000000.0   13372000000.0
    # Total Revenue                           365817000000.0  274515000000.0  260174000000.0  265595000000.0
    # Total Operating Expenses                256868000000.0  208227000000.0  196244000000.0  194697000000.0
    # Cost Of Revenue                         212981000000.0  169559000000.0  161782000000.0  163756000000.0
    # Total Other Income Expense Net             258000000.0     803000000.0    1807000000.0    2005000000.0
    # Discontinued Operations                           None            None            None            None
    # Net Income From Continuing Ops           94680000000.0   57411000000.0   55256000000.0   59531000000.0
    # Net Income Applicable To Common Shares   94680000000.0   57411000000.0   55256000000.0   59531000000.0
    
    fin_sql = ("""
        insert or ignore into sat_instrument_financials_year
            (PK_hub, date_load, record_source, date, 
            research_development, effect_of_accounting_charges,
            income_before_tax, net_income, selling_general_administrative,
            gross_profit, ebit, operating_income, other_operating_expenses,
            interest_expense, income_tax_expense,
            total_revenue, total_operating_expenses,
            cost_of_revenue, total_other_income_expense_net,
            net_income_from_continuing_ops, net_income_applicable_to_common_shares)
            values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
        """,
        [(hub_pk, date_load, record_source, a.get("date", None), 
            a.get("Research Development", None), a.get("Effect Of Accounting Charges", None),
            a.get("Income Before Tax", None), a.get("Net Income", None), a.get("Selling General Administrative", None),
            a.get("Gross Profit", None), a.get("Ebit", None), a.get("Operating Income", None), a.get("Other Operating Expenses", None),
            a.get("Interest Expense", None), a.get("Income Tax Expense", None),
            a.get("Total Revenue", None), a.get("Total Operating Expenses", None),
            a.get("Cost Of Revenue", None), a.get("Total Other Income Expense Net", None),
            a.get("Net Income From Continuing Ops", None), a.get("Net Income Applicable To Common Shares", None)) for a in financials])

    yf_db.executemany(fin_sql[0], fin_sql[1]) # many
    yf_db.commit()

def fetch_and_save_financials_quater(ticker, hub_pk, date_load, record_source, yf_db):

    # ------------------------- sat_instrument_financials_quarter

    quarterly_financials = ticker.quarterly_financials
    quarterly_financials = [dict([("date", datetime_to_timestamp_s([c])[0])] + list(zip(quarterly_financials.index, quarterly_financials[c]))) for c in quarterly_financials.columns]

    #                                             2021-09-25     2021-06-26     2021-03-27      2020-12-26
    # Research Development                     5772000000.0   5717000000.0   5262000000.0    5163000000.0
    # Effect Of Accounting Charges                     None           None           None            None
    # Income Before Tax                       23248000000.0  24369000000.0  28011000000.0   33579000000.0
    # Minority Interest                                None           None           None            None
    # Net Income                              20551000000.0  21744000000.0  23630000000.0   28755000000.0
    # Selling General Administrative           5616000000.0   5412000000.0   5314000000.0    5631000000.0
    # Gross Profit                            35174000000.0  35255000000.0  38079000000.0   44328000000.0
    # Ebit                                    23786000000.0  24126000000.0  27503000000.0   33534000000.0
    # Operating Income                        23786000000.0  24126000000.0  27503000000.0   33534000000.0
    # Other Operating Expenses                         None           None           None            None
    # Interest Expense                         -672000000.0   -665000000.0   -670000000.0    -638000000.0
    # Extraordinary Items                              None           None           None            None
    # Non Recurring                                    None           None           None            None
    # Other Items                                      None           None           None            None
    # Income Tax Expense                       2697000000.0   2625000000.0   4381000000.0    4824000000.0
    # Total Revenue                           83360000000.0  81434000000.0  89584000000.0  111439000000.0
    # Total Operating Expenses                59574000000.0  57308000000.0  62081000000.0   77905000000.0
    # Cost Of Revenue                         48186000000.0  46179000000.0  51505000000.0   67111000000.0
    # Total Other Income Expense Net           -538000000.0    243000000.0    508000000.0      45000000.0
    # Discontinued Operations                          None           None           None            None
    # Net Income From Continuing Ops          20551000000.0  21744000000.0  23630000000.0   28755000000.0
    # Net Income Applicable To Common Shares  20551000000.0  21744000000.0  23630000000.0   28755000000.0

    quarter_fin_sql = ("""
        insert or ignore into sat_instrument_financials_quarter
            (PK_hub, date_load, record_source, date, 
            research_development, effect_of_accounting_charges,
            income_before_tax, net_income, selling_general_administrative,
            gross_profit, ebit, operating_income, other_operating_expenses,
            interest_expense, income_tax_expense,
            total_revenue, total_operating_expenses,
            cost_of_revenue, total_other_income_expense_net,
            net_income_from_continuing_ops, net_income_applicable_to_common_shares)
            values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
        """,
        [(hub_pk, date_load, record_source, a.get("date", None), 
            a.get("Research Development", None), a.get("Effect Of Accounting Charges", None),
            a.get("Income Before Tax", None), a.get("Net Income", None), a.get("Selling General Administrative", None),
            a.get("Gross Profit", None), a.get("Ebit", None), a.get("Operating Income", None), a.get("Other Operating Expenses", None),
            a.get("Interest Expense", None), a.get("Income Tax Expense", None),
            a.get("Total Revenue", None), a.get("Total Operating Expenses", None),
            a.get("Cost Of Revenue", None), a.get("Total Other Income Expense Net", None),
            a.get("Net Income From Continuing Ops", None), a.get("Net Income Applicable To Common Shares", None)) for a in quarterly_financials])

    yf_db.executemany(quarter_fin_sql[0], quarter_fin_sql[1]) # many
    yf_db.commit()

def fetch_and_save_major_holders(ticker, hub_pk, date_load, record_source, yf_db):

    # ------------------------- sat_instrument_general_holder_info

    major_holders = ticker.major_holders
    major_holders = [*major_holders.set_axis(major_holders[1])[0].items()]
    major_holders = dict([(k, float(a[:-1]) if a[-1] == "%" else int(a)) for (k, a) in major_holders])

    # {"% of Shares Held by All Insider": "0.07%", "% of Shares Held by Institutions": "58.79%", "% of Float Held by Institutions": "58.83%", "Number of Institutions Holding Shares": "4963"}

    general_holder_sql = ("""
        insert or ignore into sat_instrument_general_holder_info
            (PK_hub, date_load, record_source, 
            percent_shares_held_by_insider, 
            percent_shares_held_institutions, 
            percent_float_held_institutions, 
            number_institutions_holding_shares)
            values (?, ?, ?, ?, ?, ?, ?);
        """,
        (hub_pk, date_load, record_source, 
            major_holders.get("% of Shares Held by All Insider", None),
            major_holders.get("% of Shares Held by Institutions", None),
            major_holders.get("% of Float Held by Institutions", None),
            major_holders.get("Number of Institutions Holding Shares", None)))

    yf_db.execute(general_holder_sql[0], general_holder_sql[1])
    yf_db.commit()

def fetch_and_save_institutional_holders(ticker, hub_pk, date_load, record_source, yf_db):
    
    # ------------------------- sat_instrument_institutional_holder

    institutional_holders = ticker.institutional_holders
    institutional_holders["Date Reported"] = datetime_to_timestamp_s(institutional_holders["Date Reported"])
    institutional_holders = [dict(list(zip(institutional_holders.columns, r.values))) for (i,r) in institutional_holders.iterrows()]

    # [{"Holder": "Vanguard Group, Inc. (The)", "Shares": 1266332667, "Date Reported": 1632873600, "% Out": 0.07719999999999999, "Value": 179186072380}, {"Holder": "Blackrock Inc.", "Shares": 1026223983, "Date Reported": 1632873600, "% Out": 0.0626, "Value": 145210693594}, {"Holder": "Berkshire Hathaway, Inc", "Shares": 887135554, "Date Reported": 1632873600, "% Out": 0.0541, "Value": 125529680891}, {"Holder": "State Street Corporation", "Shares": 622163541, "Date Reported": 1632873600, "% Out": 0.0379, "Value": 88036141051}, {"Holder": "FMR, LLC", "Shares": 350617759, "Date Reported": 1632873600, "% Out": 0.021400000000000002, "Value": 49612412898}, {"Holder": "Geode Capital Management, LLC", "Shares": 259894947, "Date Reported": 1632873600, "% Out": 0.0158, "Value": 36775135000}, {"Holder": "Northern Trust Corporation", "Shares": 195321532, "Date Reported": 1632873600, "% Out": 0.011899999999999999, "Value": 27637996778}, {"Holder": "Price (T.Rowe) Associates Inc", "Shares": 188489966, "Date Reported": 1632873600, "% Out": 0.0115, "Value": 26671330189}, {"Holder": "Norges Bank Investment Management", "Shares": 167580974, "Date Reported": 1609286400, "% Out": 0.0102, "Value": 22236319440}, {"Holder": "Bank Of New York Mellon Corporation", "Shares": 149381117, "Date Reported": 1632873600, "% Out": 0.0091, "Value": 21137428055}]

    #                                 Holder      Shares Date Reported   % Out         Value
    # 0           Vanguard Group, Inc. (The)  1266332667    2021-09-29  0.0772  179186072380
    # 1                       Blackrock Inc.  1026223983    2021-09-29  0.0626  145210693594
    # 2              Berkshire Hathaway, Inc   887135554    2021-09-29  0.0541  125529680891
    # 3             State Street Corporation   622163541    2021-09-29  0.0379   88036141051
    # 4                             FMR, LLC   350617759    2021-09-29  0.0214   49612412898
    # 5        Geode Capital Management, LLC   259894947    2021-09-29  0.0158   36775135000
    # 6           Northern Trust Corporation   195321532    2021-09-29  0.0119   27637996778
    # 7        Price (T.Rowe) Associates Inc   188489966    2021-09-29  0.0115   26671330189
    # 8    Norges Bank Investment Management   167580974    2020-12-30  0.0102   22236319440
    # 9  Bank Of New York Mellon Corporation   149381117    2021-09-29  0.0091   21137428055

    institutional_holder_sql = ("""
        insert or ignore into sat_instrument_institutional_holder
            (PK_hub, date_load, record_source,
            holder, shares, date_reported,
            percent_out, value)
            values (?, ?, ?, ?, ?, ?, ?, ?);
        """,
        [(hub_pk, date_load, record_source, 
            a.get("Holder", None), a.get("Shares", None), a.get("Date Reported", None), 
            a.get("% Out", None), a.get("Value", None)) for a in institutional_holders])

    yf_db.executemany(institutional_holder_sql[0], institutional_holder_sql[1]) # many
    yf_db.commit()

def fetch_and_save_balance_sheets_year(ticker, hub_pk, date_load, record_source, yf_db):

    # ------------------------- sat_instrument_balance_sheet_year

    balance_sheet = ticker.balance_sheet
    balance_sheet = [dict([("date", datetime_to_timestamp_s([c])[0])] + list(zip(balance_sheet.index, balance_sheet[c]))) for c in balance_sheet.columns]

    # [{"date": 1632528000, "Total Liab": 287912000000.0, "Total Stockholder Equity": 63090000000.0, "Other Current Liab": 53577000000.0, "Total Assets": 351002000000.0, "Common Stock": 57365000000.0, "Other Current Assets": 14111000000.0, "Retained Earnings": 5562000000.0, "Other Liab": 43050000000.0, "Treasury Stock": 163000000.0, "Other Assets": 38762000000.0, "Cash": 34940000000.0, "Total Current Liabilities": 125481000000.0, "Short Long Term Debt": 9613000000.0, "Other Stockholder Equity": 163000000.0, "Property Plant Equipment": 49527000000.0, "Total Current Assets": 134836000000.0, "Long Term Investments": 127877000000.0, "Net Tangible Assets": 63090000000.0, "Short Term Investments": 27699000000.0, "Net Receivables": 51506000000.0, "Long Term Debt": 109106000000.0, "Inventory": 6580000000.0, "Accounts Payable": 54763000000.0}, {"date": 1601078400, "Total Liab": 258549000000.0, "Total Stockholder Equity": 65339000000.0, "Other Current Liab": 47867000000.0, "Total Assets": 323888000000.0, "Common Stock": 50779000000.0, "Other Current Assets": 11264000000.0, "Retained Earnings": 14966000000.0, "Other Liab": 46108000000.0, "Treasury Stock": -406000000.0, "Other Assets": 33952000000.0, "Cash": 38016000000.0, "Total Current Liabilities": 105392000000.0, "Short Long Term Debt": 8773000000.0, "Other Stockholder Equity": -406000000.0, "Property Plant Equipment": 45336000000.0, "Total Current Assets": 143713000000.0, "Long Term Investments": 100887000000.0, "Net Tangible Assets": 65339000000.0, "Short Term Investments": 52927000000.0, "Net Receivables": 37445000000.0, "Long Term Debt": 98667000000.0, "Inventory": 4061000000.0, "Accounts Payable": 42296000000.0}, {"date": 1569628800, "Total Liab": 248028000000.0, "Total Stockholder Equity": 90488000000.0, "Other Current Liab": 43242000000.0, "Total Assets": 338516000000.0, "Common Stock": 45174000000.0, "Other Current Assets": 12352000000.0, "Retained Earnings": 45898000000.0, "Other Liab": 50503000000.0, "Treasury Stock": -584000000.0, "Other Assets": 32978000000.0, "Cash": 48844000000.0, "Total Current Liabilities": 105718000000.0, "Short Long Term Debt": 10260000000.0, "Other Stockholder Equity": -584000000.0, "Property Plant Equipment": 37378000000.0, "Total Current Assets": 162819000000.0, "Long Term Investments": 105341000000.0, "Net Tangible Assets": 90488000000.0, "Short Term Investments": 51713000000.0, "Net Receivables": 45804000000.0, "Long Term Debt": 91807000000.0, "Inventory": 4106000000.0, "Accounts Payable": 46236000000.0}, {"date": 1538179200, "Total Liab": 258578000000.0, "Total Stockholder Equity": 107147000000.0, "Other Current Liab": 39293000000.0, "Total Assets": 365725000000.0, "Common Stock": 40201000000.0, "Other Current Assets": 12087000000.0, "Retained Earnings": 70400000000.0, "Other Liab": 48914000000.0, "Treasury Stock": -3454000000.0, "Other Assets": 22283000000.0, "Cash": 25913000000.0, "Total Current Liabilities": 115929000000.0, "Short Long Term Debt": 8784000000.0, "Other Stockholder Equity": -3454000000.0, "Property Plant Equipment": 41304000000.0, 
    # "Total Current Assets": 131339000000.0, "Long Term Investments": 170799000000.0, "Net Tangible Assets": 107147000000.0, "Short Term Investments": 40388000000.0, "Net Receivables": 48995000000.0, "Long Term Debt": 93735000000.0, "Inventory": 3956000000.0, "Accounts Payable": 55888000000.0}]

    #                             2021-09-25    2020-09-26    2019-09-28    2018-09-29
    # Total Liab                 2.879120e+11  2.585490e+11  2.480280e+11  2.585780e+11
    # Total Stockholder Equity   6.309000e+10  6.533900e+10  9.048800e+10  1.071470e+11
    # Other Current Liab         5.357700e+10  4.786700e+10  4.324200e+10  3.929300e+10
    # Total Assets               3.510020e+11  3.238880e+11  3.385160e+11  3.657250e+11
    # Common Stock               5.736500e+10  5.077900e+10  4.517400e+10  4.020100e+10
    # Other Current Assets       1.411100e+10  1.126400e+10  1.235200e+10  1.208700e+10
    # Retained Earnings          5.562000e+09  1.496600e+10  4.589800e+10  7.040000e+10
    # Other Liab                 4.305000e+10  4.610800e+10  5.050300e+10  4.891400e+10
    # Treasury Stock             1.630000e+08 -4.060000e+08 -5.840000e+08 -3.454000e+09
    # Other Assets               3.876200e+10  3.395200e+10  3.297800e+10  2.228300e+10
    # Cash                       3.494000e+10  3.801600e+10  4.884400e+10  2.591300e+10
    # Total Current Liabilities  1.254810e+11  1.053920e+11  1.057180e+11  1.159290e+11
    # Short Long Term Debt       9.613000e+09  8.773000e+09  1.026000e+10  8.784000e+09
    # Other Stockholder Equity   1.630000e+08 -4.060000e+08 -5.840000e+08 -3.454000e+09
    # Property Plant Equipment   4.952700e+10  4.533600e+10  3.737800e+10  4.130400e+10
    # Total Current Assets       1.348360e+11  1.437130e+11  1.628190e+11  1.313390e+11
    # Long Term Investments      1.278770e+11  1.008870e+11  1.053410e+11  1.707990e+11
    # Net Tangible Assets        6.309000e+10  6.533900e+10  9.048800e+10  1.071470e+11
    # Short Term Investments     2.769900e+10  5.292700e+10  5.171300e+10  4.038800e+10
    # Net Receivables            5.150600e+10  3.744500e+10  4.580400e+10  4.899500e+10
    # Long Term Debt             1.091060e+11  9.866700e+10  9.180700e+10  9.373500e+10
    # Inventory                  6.580000e+09  4.061000e+09  4.106000e+09  3.956000e+09
    # Accounts Payable           5.476300e+10  4.229600e+10  4.623600e+10  5.588800e+10

    balance_sheet_sql = ("""
        insert or ignore into sat_instrument_balance_sheet_year
            (PK_hub, date_load, record_source,
            date, total_liab, total_stockholder_equity,
            other_current_liab, total_assets,
            common_stock, other_current_assets,
            retained_earnings, other_liab,
            treasury_stock, other_assets, cash,
            total_current_liabilities,
            short_long_term_debt, other_stockholder_equity,
            property_plant_equipment, total_current_assets,
            long_term_investments, net_tangible_assets,
            short_term_investments, net_receivables,
            long_term_debt, inventory, accounts_payable)
            values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
        """,
        [(hub_pk, date_load, record_source, 
            a.get("date", None), a.get("Total Liab", None), a.get("Total Stockholder Equity", None), 
            a.get("Other Current Liab", None), a.get("Total Assets", None), 
            a.get("Common Stock", None), a.get("Other Current Assets", None), 
            a.get("Retained Earnings", None), a.get("Other Liab", None), 
            a.get("Treasury Stock", None), a.get("Other Assets", None), a.get("Cash", None), 
            a.get("Total Current Liabilities", None), 
            a.get("Short Long Term Debt", None), a.get("Other Stockholder Equity", None), 
            a.get("Property Plant Equipment", None), a.get("Total Current Assets", None), 
            a.get("Long Term Investments", None), a.get("Net Tangible Assets", None),
            a.get("Short Term Investments", None), a.get("Net Receivables", None), 
            a.get("Long Term Debt", None), a.get("Inventory", None), a.get("Accounts Payable", None)) for a in balance_sheet])

    yf_db.executemany(balance_sheet_sql[0], balance_sheet_sql[1]) # many
    yf_db.commit()

def fetch_and_save_balance_sheets_quarter(ticker, hub_pk, date_load, record_source, yf_db):

    # ------------------------- sat_instrument_balance_sheet_quarter

    quarterly_balance_sheet = ticker.quarterly_balance_sheet
    quarterly_balance_sheet = [dict([("date", datetime_to_timestamp_s([c])[0])] + list(zip(quarterly_balance_sheet.index, quarterly_balance_sheet[c]))) for c in quarterly_balance_sheet.columns]

    # [{"date": 1632528000, "Total Liab": 287912000000.0, "Total Stockholder Equity": 63090000000.0, "Other Current Liab": 53577000000.0, "Total Assets": 351002000000.0, "Common Stock": 57365000000.0, "Other Current Assets": 14111000000.0, "Retained Earnings": 5562000000.0, "Other Liab": 43050000000.0, "Treasury Stock": 163000000.0, "Other Assets": 38762000000.0, "Cash": 34940000000.0, "Total Current Liabilities": 125481000000.0, "Short Long Term Debt": 9613000000.0, "Other Stockholder Equity": 163000000.0, "Property Plant Equipment": 49527000000.0, "Total Current Assets": 134836000000.0, "Long Term Investments": 127877000000.0, "Net Tangible Assets": 63090000000.0, "Short Term Investments": 27699000000.0, "Net Receivables": 51506000000.0, "Long Term Debt": 109106000000.0, "Inventory": 6580000000.0, "Accounts Payable": 54763000000.0}, {"date": 1624665600, "Total Liab": 265560000000.0, "Total Stockholder Equity": 64280000000.0, "Other Current Liab": 51306000000.0, "Total Assets": 329840000000.0, "Common Stock": 54989000000.0, "Other Current Assets": 13641000000.0, "Retained Earnings": 9233000000.0, "Other Liab": 38354000000.0, "Treasury Stock": 58000000.0, "Other Assets": 44854000000.0, "Cash": 34050000000.0, "Total Current Liabilities": 107754000000.0, "Short Long Term Debt": 8039000000.0, "Other Stockholder Equity": 58000000.0, "Property Plant Equipment": 38615000000.0, "Total Current Assets": 114423000000.0, "Long Term Investments": 131948000000.0, "Net Tangible Assets": 64280000000.0, "Short Term Investments": 27646000000.0, "Net Receivables": 33908000000.0, "Long Term Debt": 105752000000.0, "Inventory": 5178000000.0, "Accounts Payable": 40409000000.0}, {"date": 1616803200, "Total Liab": 267980000000.0, "Total Stockholder Equity": 69178000000.0, "Other Current Liab": 53255000000.0, "Total Assets": 337158000000.0, "Common Stock": 54203000000.0, "Other Current Assets": 13376000000.0, "Retained Earnings": 15261000000.0, "Other Liab": 39853000000.0, "Treasury Stock": -286000000.0, "Other Assets": 43339000000.0, "Cash": 38466000000.0, "Total Current Liabilities": 106385000000.0, "Short Long Term Debt": 8003000000.0, "Other Stockholder Equity": -286000000.0, "Property Plant Equipment": 37815000000.0, "Total Current Assets": 121465000000.0, "Long Term Investments": 134539000000.0, "Net Tangible Assets": 69178000000.0, "Short Term Investments": 31368000000.0, "Net Receivables": 33036000000.0, "Long Term Debt": 108642000000.0, "Inventory": 5219000000.0, "Accounts Payable": 40127000000.0}, {"date": 1608940800, "Total Liab": 287830000000.0, "Total Stockholder Equity": 66224000000.0, "Other Current Liab": 55899000000.0, "Total Assets": 354054000000.0, "Common Stock": 51744000000.0, "Other Current Assets": 13687000000.0, "Retained Earnings": 14301000000.0, "Other Liab": 43042000000.0, "Treasury Stock": 179000000.0, "Other Assets": 43270000000.0, "Cash": 36010000000.0, "Total Current Liabilities": 132507000000.0, "Short Long Term Debt": 7762000000.0, "Other Stockholder Equity": 179000000.0, "Property Plant Equipment": 37933000000.0, "Total Current Assets": 154106000000.0, "Long Term Investments": 118745000000.0, "Net Tangible Assets": 66224000000.0, "Short Term Investments": 40816000000.0, "Net Receivables": 58620000000.0, "Long Term Debt": 99281000000.0, "Inventory": 4973000000.0, "Accounts Payable": 63846000000.0}]

    #                             2021-09-25    2020-09-26    2019-09-28    2018-09-29
    # Total Liab                 2.879120e+11  2.585490e+11  2.480280e+11  2.585780e+11
    # Total Stockholder Equity   6.309000e+10  6.533900e+10  9.048800e+10  1.071470e+11
    # Other Current Liab         5.357700e+10  4.786700e+10  4.324200e+10  3.929300e+10
    # Total Assets               3.510020e+11  3.238880e+11  3.385160e+11  3.657250e+11
    # Common Stock               5.736500e+10  5.077900e+10  4.517400e+10  4.020100e+10
    # Other Current Assets       1.411100e+10  1.126400e+10  1.235200e+10  1.208700e+10
    # Retained Earnings          5.562000e+09  1.496600e+10  4.589800e+10  7.040000e+10
    # Other Liab                 4.305000e+10  4.610800e+10  5.050300e+10  4.891400e+10
    # Treasury Stock             1.630000e+08 -4.060000e+08 -5.840000e+08 -3.454000e+09
    # Other Assets               3.876200e+10  3.395200e+10  3.297800e+10  2.228300e+10
    # Cash                       3.494000e+10  3.801600e+10  4.884400e+10  2.591300e+10
    # Total Current Liabilities  1.254810e+11  1.053920e+11  1.057180e+11  1.159290e+11
    # Short Long Term Debt       9.613000e+09  8.773000e+09  1.026000e+10  8.784000e+09
    # Other Stockholder Equity   1.630000e+08 -4.060000e+08 -5.840000e+08 -3.454000e+09
    # Property Plant Equipment   4.952700e+10  4.533600e+10  3.737800e+10  4.130400e+10
    # Total Current Assets       1.348360e+11  1.437130e+11  1.628190e+11  1.313390e+11
    # Long Term Investments      1.278770e+11  1.008870e+11  1.053410e+11  1.707990e+11
    # Net Tangible Assets        6.309000e+10  6.533900e+10  9.048800e+10  1.071470e+11
    # Short Term Investments     2.769900e+10  5.292700e+10  5.171300e+10  4.038800e+10
    # Net Receivables            5.150600e+10  3.744500e+10  4.580400e+10  4.899500e+10
    # Long Term Debt             1.091060e+11  9.866700e+10  9.180700e+10  9.373500e+10
    # Inventory                  6.580000e+09  4.061000e+09  4.106000e+09  3.956000e+09
    # Accounts Payable           5.476300e+10  4.229600e+10  4.623600e+10  5.588800e+10
    
    quarter_balance_sheet_sql = ("""
        insert or ignore into sat_instrument_balance_sheet_quarter
            (PK_hub, date_load, record_source,
            date, total_liab, total_stockholder_equity,
            other_current_liab, total_assets,
            common_stock, other_current_assets,
            retained_earnings, other_liab,
            treasury_stock, other_assets, cash,
            total_current_liabilities,
            short_long_term_debt, other_stockholder_equity,
            property_plant_equipment, total_current_assets,
            long_term_investments, net_tangible_assets,
            short_term_investments, net_receivables,
            long_term_debt, inventory, accounts_payable)
            values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
        """,
        [(hub_pk, date_load, record_source, 
            a.get("date", None), a.get("Total Liab", None), a.get("Total Stockholder Equity", None), 
            a.get("Other Current Liab", None), a.get("Total Assets", None), 
            a.get("Common Stock", None), a.get("Other Current Assets", None), 
            a.get("Retained Earnings", None), a.get("Other Liab", None), 
            a.get("Treasury Stock", None), a.get("Other Assets", None), a.get("Cash", None), 
            a.get("Total Current Liabilities", None), 
            a.get("Short Long Term Debt", None), a.get("Other Stockholder Equity", None), 
            a.get("Property Plant Equipment", None), a.get("Total Current Assets", None), 
            a.get("Long Term Investments", None), a.get("Net Tangible Assets", None),
            a.get("Short Term Investments", None), a.get("Net Receivables", None), 
            a.get("Long Term Debt", None), a.get("Inventory", None), a.get("Accounts Payable", None)) for a in quarterly_balance_sheet])

    yf_db.executemany(quarter_balance_sheet_sql[0], quarter_balance_sheet_sql[1]) # many
    yf_db.commit()

def fetch_and_save_cashflow_year(ticker, hub_pk, date_load, record_source, yf_db):

    # ------------------------- sat_instrument_cashflow_year

    cashflow = ticker.cashflow
    cashflow = [dict([("date", datetime_to_timestamp_s([c])[0])] + list(zip(cashflow.index, cashflow[c]))) for c in cashflow.columns]

    # [{"date": 1632528000, "Investments": -2819000000.0, "Change To Liabilities": 14002000000.0, "Total Cashflows From Investing Activities": -14545000000.0, "Net Borrowings": 12665000000.0, "Total Cash From Financing Activities": -93353000000.0, "Change To Operating Activities": -6146000000.0, "Issuance Of Stock": 1105000000.0, "Net Income": 94680000000.0, "Change In Cash": -3860000000.0, "Repurchase Of Stock": -92527000000.0, "Total Cash From Operating Activities": 104038000000.0, "Depreciation": 11284000000.0, "Other Cashflows From Investing 
    # Activities": -608000000.0, "Dividends Paid": -14467000000.0, "Change To Inventory": -2642000000.0, "Change To Account Receivables": -10125000000.0, "Other Cashflows From Financing Activities": -129000000.0, "Change To Netincome": 2985000000.0, "Capital Expenditures": -11085000000.0}, {"date": 1601078400, "Investments": 5335000000.0, "Change To Liabilities": -1981000000.0, "Total Cashflows From Investing Activities": -4289000000.0, "Net Borrowings": 2499000000.0, "Total Cash From Financing Activities": -86820000000.0, "Change To Operating Activities": 881000000.0, "Issuance Of Stock": 880000000.0, "Net Income": 57411000000.0, "Change In Cash": -10435000000.0, "Repurchase Of Stock": -75992000000.0, "Total Cash From Operating Activities": 80674000000.0, "Depreciation": 11056000000.0, "Other Cashflows From Investing Activities": -791000000.0, "Dividends Paid": -14081000000.0, "Change To Inventory": -127000000.0, "Change To Account Receivables": 6917000000.0, "Other Cashflows From Financing Activities": -126000000.0, "Change To Netincome": 6517000000.0, "Capital Expenditures": -7309000000.0}, {"date": 1569628800, "Investments": 58093000000.0, "Change To Liabilities": -2548000000.0, "Total Cashflows From Investing Activities": 45896000000.0, "Net Borrowings": -7819000000.0, "Total Cash From Financing Activities": -90976000000.0, "Change To Operating Activities": -896000000.0, "Issuance Of Stock": 781000000.0, "Net Income": 55256000000.0, "Change In Cash": 24311000000.0, "Repurchase Of Stock": -69714000000.0, "Total Cash From Operating Activities": 69391000000.0, "Depreciation": 12547000000.0, "Other Cashflows From Investing Activities": -1078000000.0, "Dividends Paid": -14119000000.0, "Change To Inventory": -289000000.0, "Change To Account Receivables": 245000000.0, "Other Cashflows From Financing Activities": -105000000.0, "Change To Netincome": 5076000000.0, "Capital Expenditures": 
    # -10495000000.0}, {"date": 1538179200, "Investments": 30845000000.0, "Change To Liabilities": 9172000000.0, "Total Cashflows From Investing Activities": 16066000000.0, "Net Borrowings": 432000000.0, "Total Cash From Financing Activities": -87876000000.0, "Change To Operating Activities": 30016000000.0, "Issuance Of Stock": 669000000.0, "Net Income": 59531000000.0, "Change In Cash": 5624000000.0, "Repurchase Of Stock": -75265000000.0, "Total Cash From Operating Activities": 77434000000.0, "Depreciation": 10903000000.0, "Other Cashflows From Investing Activities": -745000000.0, "Dividends Paid": -13712000000.0, "Change To Inventory": 828000000.0, "Change To Account Receivables": -5322000000.0, "Other Cashflows From Financing Activities": -105000000.0, "Change To Netincome": -27694000000.0, "Capital Expenditures": -13313000000.0}]

    #                                             2021-09-25    2020-09-26    2019-09-28    2018-09-29
    # Investments                               -2.819000e+09  5.335000e+09  5.809300e+10  3.084500e+10
    # Change To Liabilities                      1.400200e+10 -1.981000e+09 -2.548000e+09  9.172000e+09
    # Total Cashflows From Investing Activities -1.454500e+10 -4.289000e+09  4.589600e+10  1.606600e+10
    # Net Borrowings                             1.266500e+10  2.499000e+09 -7.819000e+09  4.320000e+08
    # Total Cash From Financing Activities      -9.335300e+10 -8.682000e+10 -9.097600e+10 -8.787600e+10
    # Change To Operating Activities            -6.146000e+09  8.810000e+08 -8.960000e+08  3.001600e+10
    # Issuance Of Stock                          1.105000e+09  8.800000e+08  7.810000e+08  6.690000e+08
    # Net Income                                 9.468000e+10  5.741100e+10  5.525600e+10  5.953100e+10
    # Change In Cash                            -3.860000e+09 -1.043500e+10  2.431100e+10  5.624000e+09
    # Repurchase Of Stock                       -9.252700e+10 -7.599200e+10 -6.971400e+10 -7.526500e+10
    # Total Cash From Operating Activities       1.040380e+11  8.067400e+10  6.939100e+10  7.743400e+10
    # Depreciation                               1.128400e+10  1.105600e+10  1.254700e+10  1.090300e+10
    # Other Cashflows From Investing Activities -6.080000e+08 -7.910000e+08 -1.078000e+09 -7.450000e+08
    # Dividends Paid                            -1.446700e+10 -1.408100e+10 -1.411900e+10 -1.371200e+10
    # Change To Inventory                       -2.642000e+09 -1.270000e+08 -2.890000e+08  8.280000e+08
    # Change To Account Receivables             -1.012500e+10  6.917000e+09  2.450000e+08 -5.322000e+09
    # Other Cashflows From Financing Activities -1.290000e+08 -1.260000e+08 -1.050000e+08 -1.050000e+08
    # Change To Netincome                        2.985000e+09  6.517000e+09  5.076000e+09 -2.769400e+10
    # Capital Expenditures 


    cashflow_sql = ("""
        insert or ignore into sat_instrument_cashflow_year
            (PK_hub, date_load, record_source,
                date, investments, change_to_liabilities,
                total_cashflows_from_investing_activities,
                net_borrowings, total_cash_from_financing_activities,
                change_to_operating_activities,
                issuance_of_stock, net_income,
                change_in_cash, repurchase_of_stock,
                total_cash_from_operating_activities,
                depreciation, other_cashflows_from_investing_activities,
                dividends_paid, change_to_inventory,
                change_to_account_receivables,
                other_cashflows_from_financing_activities,
                change_to_net_income, capital_expenditures)
            values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
        """,
        [(hub_pk, date_load, record_source, 
            a.get("date", None), a.get("Investments", None), a.get("Change To Liabilities", None), 
            a.get("Total Cashflows From Investing Activities", None), a.get("Net Borrowings", None), 
            a.get("Total Cash From Financing Activities", None), a.get("Change To Operating Activities", None), 
            a.get("Issuance Of Stock", None), a.get("Net Income", None), a.get("Change In Cash", None), a.get("Repurchase Of Stock", None), 
            a.get("Total Cash From Operating Activities", None), a.get("Depreciation", None), a.get("Other Cashflows From Investing Activities", None),
            a.get("Dividends Paid", None), a.get("Change To Inventory", None), a.get("Change To Account Receivables", None), 
            a.get("Other Cashflows From Financing Activities", None), a.get("Change To Netincome", None), 
            a.get("Capital Expenditures", None)) for a in cashflow])


    yf_db.executemany(cashflow_sql[0], cashflow_sql[1]) # many
    yf_db.commit()

def fetch_and_save_cashflow_quarter(ticker, hub_pk, date_load, record_source, yf_db):

    # ------------------------- sat_instrument_cashflow_quarter

    quarterly_cashflow = ticker.quarterly_cashflow
    quarterly_cashflow = [dict([("date", datetime_to_timestamp_s([c])[0])] + list(zip(quarterly_cashflow.index, quarterly_cashflow[c]))) for c in quarterly_cashflow.columns]

    # [{"date": 1632528000, "Investments": -2819000000.0, "Change To Liabilities": 14002000000.0, "Total Cashflows From Investing Activities": -14545000000.0, "Net Borrowings": 12665000000.0, "Total Cash From Financing Activities": -93353000000.0, "Change To Operating Activities": -6146000000.0, "Issuance Of Stock": 1105000000.0, "Net Income": 94680000000.0, "Change In Cash": -3860000000.0, "Repurchase Of Stock": -92527000000.0, "Total Cash From Operating Activities": 104038000000.0, "Depreciation": 11284000000.0, "Other Cashflows From Investing 
    # Activities": -608000000.0, "Dividends Paid": -14467000000.0, "Change To Inventory": -2642000000.0, "Change To Account Receivables": -10125000000.0, "Other Cashflows From Financing Activities": -129000000.0, "Change To Netincome": 2985000000.0, "Capital Expenditures": -11085000000.0}, {"date": 1601078400, "Investments": 5335000000.0, "Change To Liabilities": -1981000000.0, "Total Cashflows From Investing Activities": -4289000000.0, "Net Borrowings": 2499000000.0, "Total Cash From Financing Activities": -86820000000.0, "Change To Operating Activities": 881000000.0, "Issuance Of Stock": 880000000.0, "Net Income": 57411000000.0, "Change In Cash": -10435000000.0, "Repurchase Of Stock": -75992000000.0, "Total Cash From Operating Activities": 80674000000.0, "Depreciation": 11056000000.0, "Other Cashflows From Investing Activities": -791000000.0, "Dividends Paid": -14081000000.0, "Change To Inventory": -127000000.0, "Change To Account Receivables": 6917000000.0, "Other Cashflows From Financing Activities": -126000000.0, "Change To Netincome": 6517000000.0, "Capital Expenditures": -7309000000.0}, {"date": 1569628800, "Investments": 58093000000.0, "Change To Liabilities": -2548000000.0, "Total Cashflows From Investing Activities": 45896000000.0, "Net Borrowings": -7819000000.0, "Total Cash From Financing Activities": -90976000000.0, "Change To Operating Activities": -896000000.0, "Issuance Of Stock": 781000000.0, "Net Income": 55256000000.0, "Change In Cash": 24311000000.0, "Repurchase Of Stock": -69714000000.0, "Total Cash From Operating Activities": 69391000000.0, "Depreciation": 12547000000.0, "Other Cashflows From Investing Activities": -1078000000.0, "Dividends Paid": -14119000000.0, "Change To Inventory": -289000000.0, "Change To Account Receivables": 245000000.0, "Other Cashflows From Financing Activities": -105000000.0, "Change To Netincome": 5076000000.0, "Capital Expenditures": 
    # -10495000000.0}, {"date": 1538179200, "Investments": 30845000000.0, "Change To Liabilities": 9172000000.0, "Total Cashflows From Investing Activities": 16066000000.0, "Net Borrowings": 432000000.0, "Total Cash From Financing Activities": -87876000000.0, "Change To Operating Activities": 30016000000.0, "Issuance Of Stock": 669000000.0, "Net Income": 59531000000.0, "Change In Cash": 5624000000.0, "Repurchase Of Stock": -75265000000.0, "Total Cash From Operating Activities": 77434000000.0, "Depreciation": 10903000000.0, "Other Cashflows From Investing Activities": -745000000.0, "Dividends Paid": -13712000000.0, "Change To Inventory": 828000000.0, "Change To Account Receivables": -5322000000.0, "Other Cashflows From Financing Activities": -105000000.0, "Change To Netincome": -27694000000.0, "Capital Expenditures": -13313000000.0}]

    #                                             2021-09-25    2020-09-26    2019-09-28    2018-09-29
    # Investments                               -2.819000e+09  5.335000e+09  5.809300e+10  3.084500e+10
    # Change To Liabilities                      1.400200e+10 -1.981000e+09 -2.548000e+09  9.172000e+09
    # Total Cashflows From Investing Activities -1.454500e+10 -4.289000e+09  4.589600e+10  1.606600e+10
    # Net Borrowings                             1.266500e+10  2.499000e+09 -7.819000e+09  4.320000e+08
    # Total Cash From Financing Activities      -9.335300e+10 -8.682000e+10 -9.097600e+10 -8.787600e+10
    # Change To Operating Activities            -6.146000e+09  8.810000e+08 -8.960000e+08  3.001600e+10
    # Issuance Of Stock                          1.105000e+09  8.800000e+08  7.810000e+08  6.690000e+08
    # Net Income                                 9.468000e+10  5.741100e+10  5.525600e+10  5.953100e+10
    # Change In Cash                            -3.860000e+09 -1.043500e+10  2.431100e+10  5.624000e+09
    # Repurchase Of Stock                       -9.252700e+10 -7.599200e+10 -6.971400e+10 -7.526500e+10
    # Total Cash From Operating Activities       1.040380e+11  8.067400e+10  6.939100e+10  7.743400e+10
    # Depreciation                               1.128400e+10  1.105600e+10  1.254700e+10  1.090300e+10
    # Other Cashflows From Investing Activities -6.080000e+08 -7.910000e+08 -1.078000e+09 -7.450000e+08
    # Dividends Paid                            -1.446700e+10 -1.408100e+10 -1.411900e+10 -1.371200e+10
    # Change To Inventory                       -2.642000e+09 -1.270000e+08 -2.890000e+08  8.280000e+08
    # Change To Account Receivables             -1.012500e+10  6.917000e+09  2.450000e+08 -5.322000e+09
    # Other Cashflows From Financing Activities -1.290000e+08 -1.260000e+08 -1.050000e+08 -1.050000e+08
    # Change To Netincome                        2.985000e+09  6.517000e+09  5.076000e+09 -2.769400e+10
    # Capital Expenditures 

    quarterly_cashflow_sql = ("""
        insert or ignore into sat_instrument_cashflow_quarter
            (PK_hub, date_load, record_source,
                date, investments, change_to_liabilities,
                total_cashflows_from_investing_activities,
                net_borrowings, total_cash_from_financing_activities,
                change_to_operating_activities,
                issuance_of_stock, net_income,
                change_in_cash, repurchase_of_stock,
                total_cash_from_operating_activities,
                depreciation, other_cashflows_from_investing_activities,
                dividends_paid, change_to_inventory,
                change_to_account_receivables,
                other_cashflows_from_financing_activities,
                change_to_net_income, capital_expenditures)
            values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
        """,
        [(hub_pk, date_load, record_source, 
            a.get("date", None), a.get("Investments", None), a.get("Change To Liabilities", None), 
            a.get("Total Cashflows From Investing Activities", None), a.get("Net Borrowings", None), 
            a.get("Total Cash From Financing Activities", None), a.get("Change To Operating Activities", None), 
            a.get("Issuance Of Stock", None), a.get("Net Income", None), a.get("Change In Cash", None), a.get("Repurchase Of Stock", None), 
            a.get("Total Cash From Operating Activities", None), a.get("Depreciation", None), a.get("Other Cashflows From Investing Activities", None),
            a.get("Dividends Paid", None), a.get("Change To Inventory", None), a.get("Change To Account Receivables", None), 
            a.get("Other Cashflows From Financing Activities", None), a.get("Change To Netincome", None), 
            a.get("Capital Expenditures", None)) for a in quarterly_cashflow])

    yf_db.executemany(quarterly_cashflow_sql[0], quarterly_cashflow_sql[1]) # many
    yf_db.commit()

def fetch_and_save_earnings(ticker, hub_pk, date_load, record_source, yf_db):

    # ------------------------- sat_instrument_earnings_year

    earnings = ticker.earnings
    earnings = [{"year": earnings.index[i], "revenue": a, "earnings": b} for (i, (a,b)) in enumerate(zip(earnings["Revenue"], earnings["Earnings"]))]

    #         Revenue     Earnings
    # Year
    # 2018  265595000000  59531000000
    # 2019  260174000000  55256000000
    # 2020  274515000000  57411000000
    # 2021  365817000000  94680000000

    earning_sql = ("""
        insert or ignore into sat_instrument_earnings_year
            (PK_hub, date_load, record_source, year, revenue, earnings)
            values (?, ?, ?, ?, ?, ?);
        """,
        [(hub_pk, date_load, record_source, a.get("year", None), a.get("revenue", None), a.get("earnings", None)) for a in earnings])

    yf_db.executemany(earning_sql[0], earning_sql[1]) # many
    yf_db.commit()

def fetch_and_save_earnings_quarter(ticker, hub_pk, date_load, record_source, yf_db):

    # ------------------------- sat_instrument_earnings_quarter

    quarterly_earnings = ticker.quarterly_earnings
    quarterly_earnings = [{"quarter": quarterly_earnings.index[i], "revenue": a, "earnings": b} for (i, (a,b)) in enumerate(zip(quarterly_earnings["Revenue"], quarterly_earnings["Earnings"]))]

    # [{"year": 2018, "revenue": 265595000000, "earnings": 59531000000}, {"year": 2019, "revenue": 260174000000, "earnings": 55256000000}, {"year": 2020, "revenue": 274515000000, "earnings": 57411000000}, {"year": 2021, "revenue": 365817000000, "earnings": 94680000000}]

    #             Revenue     Earnings
    # Quarter
    # 4Q2020   111439000000  28755000000
    # 1Q2021    89584000000  23630000000
    # 2Q2021    81434000000  21744000000
    # 3Q2021    83360000000  20551000000
    
    quarterly_earning_sql = ("""
        insert or ignore into sat_instrument_earnings_quarter
            (PK_hub, date_load, record_source, quarter, revenue, earnings)
            values (?, ?, ?, ?, ?, ?);
        """,
        [(hub_pk, date_load, record_source, a.get("quarter", None), a.get("revenue", None), a.get("earnings", None)) for a in quarterly_earnings])

    yf_db.executemany(quarterly_earning_sql[0], quarterly_earning_sql[1]) # many
    yf_db.commit()

def fetch_and_save_calendar(ticker, hub_pk, date_load, record_source, yf_db):

    # ------------------------- sat_instrument_calendar

    calendar = ticker.calendar
    calendar.loc["Earnings Date"] = datetime_to_timestamp_s(calendar.loc["Earnings Date"])
    calendar = [dict(list(zip(calendar.index, calendar[0]))), dict(list(zip(calendar.index, calendar[1])))]

    # [{"Earnings Date", 1643144400), ("Earnings Average", 1.88), ("Earnings Low", 1.75), ("Earnings High", 1.97), ("Revenue Average", 118182000000), ("Revenue Low", 111806000000), ("Revenue High", 121303000000}, {"Earnings Date", 1643662800), ("Earnings Average", 1.88), ("Earnings Low", 1.75), ("Earnings High", 1.97), ("Revenue Average", 118182000000), ("Revenue Low", 111806000000), ("Revenue High", 121303000000}]

    #                                     0                    1
    # Earnings Date     2022-01-25 21:00:00  2022-01-31 21:00:00
    # Earnings Average                 1.88                 1.88
    # Earnings Low                     1.75                 1.75
    # Earnings High                    1.97                 1.97
    # Revenue Average          118182000000         118182000000
    # Revenue Low              111806000000         111806000000
    # Revenue High             121303000000         121303000000

    calendar_sql = ("""
        insert or ignore into sat_instrument_calendar 
            (PK_hub, date_load, record_source, earnings_date, 
            earnings_average, earnings_low, earnings_high, 
            revenue_average, revenue_low, revenue_high)
            values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
        """,
        [(hub_pk, date_load, record_source, a.get("Earnings Date", None), 
            a.get("Earnings Average", None), a.get("Earnings Low", None), a.get("Earnings High", None), 
            a.get("Revenue Average", None), a.get("Revenue Low", None), a.get("Revenue High", None)) for a in calendar])

    yf_db.executemany(calendar_sql[0], calendar_sql[1]) # many
    yf_db.commit()

def fetch_and_save_sustainability(ticker, hub_pk, date_load, record_source, yf_db):

    # ------------------------- sat_instrument_sustainability 

    sustainability = ticker.sustainability
    sustainability = dict(zip(list(sustainability.index), list(sustainability.get("Value", None))))

    # {"palmOil": False, "controversialWeapons": False, "gambling": False, "socialScore": 7.99, "nuclear": False, "furLeather": False, "alcoholic": False, "gmo": False, "catholic": False, "socialPercentile": None, "peerCount": 53, "governanceScore": 8.64, "environmentPercentile": None, "animalTesting": False, "tobacco": False, "totalEsg": 16.92, "highestControversy": 3, "esgPerformance": "UNDER_PERF", "coal": False, "pesticides": False, "adult": False, "percentile": 13.68, "peerGroup": "Technology Hardware", "smallArms": False, "environmentScore": 0.29, "governancePercentile": None, "militaryContract": False}

    #                                     Value
    # 2021-9
    # palmOil                              False
    # controversialWeapons                 False
    # gambling                             False
    # socialScore                           7.99
    # nuclear                              False
    # furLeather                           False
    # alcoholic                            False
    # gmo                                  False
    # catholic                             False
    # socialPercentile                      None
    # peerCount                               53
    # governanceScore                       8.64
    # environmentPercentile                 None
    # animalTesting                        False
    # tobacco                              False
    # totalEsg                             16.92
    # highestControversy                       3
    # esgPerformance                  UNDER_PERF
    # coal                                 False
    # pesticides                           False
    # adult                                False
    # percentile                           13.68
    # peerGroup              Technology Hardware
    # smallArms                            False
    # environmentScore                      0.29
    # governancePercentile                  None
    # militaryContract                     False

    sus_sql = ("""
        insert or ignore into sat_instrument_sustainability 
            (PK_hub, date_load, record_source, social_score, 
            governance_score, environment_score, highest_controversy, 
            total_esg, esg_performance, peer_count, 
            peer_group, palm_oil, controversial_weapons, gambling, 
            nuclear, fur_leather, alcoholic, gmo, catholic, 
            animal_testing, tobacco, coal, pesticides, adult, 
            small_arms, military_contract)
            values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
        """,
        (hub_pk, date_load, record_source, sustainability.get("socialScore", None), 
            sustainability.get("governanceScore", None), sustainability.get("environmentScore", None), sustainability.get("highestControversy", None), 
            sustainability.get("totalEsg", None), sustainability.get("esgPerformance", None), sustainability.get("peerCount", None), 
            sustainability.get("peerGroup", None), sustainability.get("palmOil", None), sustainability.get("controversialWeapons", None), sustainability.get("gambling", None), 
            sustainability.get("nuclear", None), sustainability.get("furLeather", None), sustainability.get("alcoholic", None), sustainability.get("gmo", None), sustainability.get("catholic", None), 
            sustainability.get("animalTesting", None), sustainability.get("tobacco", None), sustainability.get("coal", None), sustainability.get("pesticides", None), sustainability.get("adult", None), 
            sustainability.get("smallArms", None), sustainability.get("militaryContract", None)))

    yf_db.execute(sus_sql[0], sus_sql[1])
    yf_db.commit()

def fetch_and_save_recomendations(ticker, hub_pk, date_load, record_source, yf_db):

    # ------------------------- sat_instrument_recomendation

    recommendations = ticker.recommendations
    recommendations = [{"date": datetime_to_timestamp_s([recommendations.index[i]])[0], "firm": a, "to_grade": b, "From_grade": c, "action": d} for (i, (a,b,c,d)) in enumerate(zip(recommendations["Firm"], recommendations["To Grade"], recommendations["From Grade"], recommendations["Action"]))]
    
    # [{"date": 1328775420, "firm": "Canaccord Genuity", "to_grade": "Buy", "From_grade": "", "action": "main"}, {"date": 1331191980, "firm": "FBN Securities", "to_grade": "Outperform", "From_grade": "", "action": "main"}, {"date": 1331618880, "firm": "Jefferies", "to_grade": "Buy", "From_grade": "", "action": "main"}, {"date": 1331705580, "firm": "Morgan Stanley", "to_grade": "Overweight", "From_grade": "", "action": "main"}, {"date": 1331713260, "firm": "Canaccord Genuity", "to_grade": "Buy", "From_grade": "", "action": "main"}, {"date": 1331738700, "firm": "Oxen Group", "to_grade": "Buy", "From_grade": "", "action": "init"}, {"date": 1331797980, "firm": "PiperJaffray", "to_grade": "Overweight", "From_grade": "", "action": "main"}, {"date": 1331880780, "firm": "Oppenheimer", "to_grade": "Outperform", "From_grade": "", "action": "main"}, {"date": 1331905980, "firm": "UBS", "to_grade": "Buy", "From_grade": "", "action": "main"}, {"date": 1332141840, "firm": "Wedbush", "to_grade": "Outperform", "From_grade": "", "action": "main"}, {"date": 1332150780, "firm": "FBN Securities", "to_grade": "Outperform", "From_grade": "", "action": "main"}, {"date": 1332167100, "firm": "Goldman Sachs", "to_grade": "Buy", "From_grade": "", "action": "main"}, {"date": 1332169140, "firm": "Morgan Keegan", "to_grade": 
    # "Outperform", "From_grade": "", "action": "main"}, {"date": 1332223800, "firm": "Sterne Agee", "to_grade": "Buy", "From_grade": "", "action": "main"}, {"date": 1332226380, "firm": "Citigroup", "to_grade": "Buy", "From_grade": "", "action": "main"}, {"date": 1332319380, "firm": "Scotiabank", "to_grade": "Sector Perform", "From_grade": "", "action": "main"}, {"date": 1332340260, "firm": "Hilliard Lyons", "to_grade": "Buy", "From_grade": "", "action": "main"}, {"date": 1332340380, "firm": "Crowell, Weedon & Co.", "to_grade": "Buy", "From_grade": "", "action": "main"}, {"date": 1333091760, "firm": "BMO Capital", "to_grade": "Outperform", "From_grade": "", "action": "main"}, {"date": 1333436460, "firm": "PiperJaffray", "to_grade": "Overweight", "From_grade": "", "action": "main"}, {"date": 1333436760, "firm": "JP Morgan", "to_grade": "Overweight", "From_grade": "", "action": "main"}, {"date": 1333607100, "firm": "Jefferies", "to_grade": "Buy", "From_grade": "", "action": "main"}, {"date": 1333954800, "firm": "BTIG Research", "to_grade": 
    # "Neutral", "From_grade": "", "action": "down"}, {"date": 1334126520, "firm": "Sterne Agee", "to_grade": "Buy", "From_grade": "", "action": "main"}, {"date": 1334210400, "firm": "Credit Suisse", "to_grade": "Outperform", "From_grade": "", "action": "main"}, {"date": 1334299740, "firm": "Bank oferica", "to_grade": "Buy", "From_grade": "", "action": "main"}, {"date": 1334730180, "firm": "Goldman Sachs", "to_grade": "Buy", "From_grade": "", "action": "main"}, {"date": 1334816100, "firm": "Canaccord Genuity", "to_grade": "Buy", "From_grade": "", "action": "main"}, {"date": 1335333840, "firm": "Deutsche Bank", "to_grade": "Buy", "From_grade": "", "action": "main"}, {"date": 1335336120, "firm": "Goldman Sachs", "to_grade": "Buy", "From_grade": "", "action": "main"}, {"date": 1335336600, "firm": "Citigroup", "to_grade": "Buy", "From_grade": "", "action": "main"}, {"date": 1335337140, "firm": "Wedbush", "to_grade": "Outperform", "From_grade": "", "action": "main"}, {"date": 1335339180, "firm": "FBN Securities", "to_grade": "Outperform", "From_grade": "", "action": "main"}, {"date": 1335339600, "firm": "Canaccord Genuity", "to_grade": "Buy", "From_grade": "", "action": "main"}, {"date": 1335344400, "firm": "Societe Generale", "to_grade": "Buy", "From_grade": "", "action": "up"}, {"date": 1335345540, "firm": "ISI Group", "to_grade": "Buy", "From_grade": "", "action": "main"}, {"date": 1335345720, "firm": "Sterne Agee", "to_grade": "Buy", "From_grade": "", "action": "main"}, {"date": 1335346560, "firm": "Bank oferica", "to_grade": "Buy", "From_grade": "", "action": "main"}, {"date": 1335430260, "firm": "Hilliard Lyons", "to_grade": "Buy", "From_grade": "", "action": "main"}, {"date": 1335431040, "firm": "Scotiabank", "to_grade": "Sector Outperform", "From_grade": "", "action": "up"}, {"date": 1338793740, "firm": "Crowell, Weedon & Co.", "to_grade": "Buy", "From_grade": "", "action": "main"}, {"date": 1341990180, "firm": "UBS", "to_grade": "Buy", "From_grade": "", "action": "init"}, {"date": 1343198760, "firm": "Morgan Stanley", "to_grade": "Overweight", "From_grade": "", 
    # "action": "main"}, {"date": 1343201700, "firm": "Goldman Sachs", "to_grade": "Buy", "From_grade": "", "action": "main"}, {"date": 1343202480, "firm": "Raymond James", "to_grade": "Outperform", "From_grade": "Strong Buy", "action": "down"}, {"date": 1343202780, "firm": "Bank of America", "to_grade": "Buy", "From_grade": "", "action": "main"}, {"date": 1343203500, "firm": "BMO Capital", "to_grade": "Outperform", "From_grade": "", "action": "main"}, {"date": 1344501300, "firm": "Stifel Nicolaus", "to_grade": "Buy", "From_grade": "", "action": "init"}, {"date": 1345187760, "firm": "Jefferies", "to_grade": "Buy", "From_grade": "", "action": "main"}, {"date": 1345549200, "firm": "Oracle Investment Research", "to_grade": "Hold", "From_grade": "Buy", "action": "down"}, {"date": 1345620960, "firm": "Needham", "to_grade": "Buy", "From_grade": "", "action": "main"}, {"date": 1346048700, "firm": "Oppenheimer", "to_grade": "Outperform", "From_grade": "", "action": "main"}, {"date": 1346048820, "firm": "Bank of America", "to_grade": "Buy", "From_grade": "", "action": "main"}, {"date": 1346742240, "firm": "Wedbush", "to_grade": "Outperform", "From_grade": "", "action": "main"}, {"date": 1346828220, "firm": "JP Morgan", "to_grade": "Overweight", "From_grade": "", "action": "main"}, {"date": 1346913420, "firm": "Societe Generale", "to_grade": "Buy", "From_grade": "", "action": "main"}, {"date": 1347275100, "firm": "Monness Crespi Hardt", "to_grade": "Buy", "From_grade": "", "action": "init"}, {"date": 1347436260, "firm": "Hudson Square", "to_grade": "Buy", "From_grade": "", "action": "main"}, {"date": 1347517080, "firm": "Sterne Agee", "to_grade": "Buy", "From_grade": "", "action": "main"}, {"date": 1347517320, "firm": "Goldman Sachs", "to_grade": "Buy", "From_grade": "", "action": "main"}, {"date": 1347533820, "firm": 
    # "UBS", "to_grade": "Buy", "From_grade": "", "action": "main"}, {"date": 1347537360, "firm": "Barclays", "to_grade": "Overweight", "From_grade": "", "action": "main"}, {"date": 1347620820, "firm": "Hilliard Lyons", "to_grade": "Buy", "From_grade": "", "action": "main"}, {"date": 1347873120, "firm": "BMO Capital", "to_grade": "Outperform", "From_grade": "", "action": "main"}, {"date": 1347948300, "firm": "Sterne Agee", "to_grade": "Buy", "From_grade": "", "action": "main"}, {"date": 1347953580, "firm": "Bank of America", "to_grade": "Buy", "From_grade": "", "action": "main"}, {"date": 1348211760, "firm": "Deutsche Bank", "to_grade": "Buy", "From_grade": "", "action": "main"}, {"date": 1349770440, "firm": "Nomura", "to_grade": "Neutral", "From_grade": "", "action": "init"}, {"date": 1351066860, "firm": "Canaccord Genuity", "to_grade": "Buy", "From_grade": "", "action": "main"}, {"date": 1351080660, "firm": "Barclays", "to_grade": "Overweight", "From_grade": "", "action": "main"}, {"date": 1351237140, "firm": "Nomura", "to_grade": "Neutral", "From_grade": "", "action": "main"}, {"date": 1351238340, "firm": "FBN Securities", "to_grade": "Outperform", "From_grade": "", "action": "main"}, {"date": 1351238700, "firm": "Deutsche Bank", "to_grade": "Buy", "From_grade": "", "action": "main"}, {"date": 1351762800, "firm": "Oracle Investment Research", "to_grade": "Buy", "From_grade": "Hold", "action": "up"}, {"date": 1352458680, "firm": "Avondale Partners", "to_grade": "Market Perform", "From_grade": "", "action": "init"}, {"date": 1353323340, "firm": "Bank 
    # of America", "to_grade": "Buy", "From_grade": "", "action": "main"}, {"date": 1353912540, "firm": "Citigroup", "to_grade": "Buy", "From_grade": "", "action": "init"}, {"date": 1354530701, "firm": "ABG Sundal Collier", "to_grade": "", "From_grade": "", "action": 
    # "init"}, {"date": 1354701742, "firm": "Oracle Investment Research", "to_grade": "Strong Buy", "From_grade": "Buy", "action": "up"}, {"date": 1355126415, "firm": "Jefferies", "to_grade": "Buy", "From_grade": "", "action": "main"}, {"date": 1355477534, "firm": "UBS", "to_grade": "Buy", "From_grade": "", "action": "main"}, {"date": 1355723873, "firm": "Citigroup", "to_grade": "Neutral", "From_grade": "Buy", "action": "down"}, {"date": 1355730802, "firm": "BMO Capital", "to_grade": "Outperform", "From_grade": "", "action": "main"}, {"date": 1355732483, "firm": "Pacific Crest", "to_grade": "Outperform", "From_grade": "", "action": "main"}, {"date": 1355740298, "firm": "Mizuho", "to_grade": "Buy", "From_grade": "", "action": "main"}, {"date": 1355748066, "firm": "Canaccord Genuity", "to_grade": "", "From_grade": "", "action": "reit"}, {"date": 1357548669, "firm": "Barclays", "to_grade": "Overweight", "From_grade": "", "action": "main"}, {"date": 1357811853, "firm": "PiperJaffray", "to_grade": "Overweight", "From_grade": "", "action": "main"}, {"date": 1357894526, "firm": "Credit Agricole", "to_grade": "Buy", "From_grade": "", "action": "main"}, {"date": 1358237242, "firm": "Nomura", "to_grade": "Neutral", "From_grade": "", "action": "main"}, {"date": 1358322174, "firm": "Pacific Crest", "to_grade": "Sector Perform", "From_grade": "Outperform", "action": "down"}, {"date": 1358322426, "firm": "Stifel", "to_grade": "Buy", "From_grade": "", "action": "main"}, {"date": 1358322481, "firm": "Bank of America", "to_grade": "Buy", "From_grade": "", "action": "main"}, {"date": 1358325189, "firm": "Stifel", "to_grade": "Buy", "From_grade": "", "action": "main"}, {"date": 1358363360, "firm": "BGC Financial", "to_grade": "Hold", "From_grade": "", "action": "main"}, {"date": 1358411387, "firm": "JP Morgan", "to_grade": "Overweight", "From_grade": "", "action": "main"}, {"date": 1358412447, "firm": "BMO Capital", "to_grade": "Outperform", "From_grade": "", "action": "main"}, {"date": 1358839359, "firm": "UBS", "to_grade": "Buy", "From_grade": "", "action": "main"}, {"date": 1358839467, "firm": "Scotiabank", "to_grade": "Sector Outperform", "From_grade": "", "action": "main"}, {"date": 1359008772, "firm": "Jefferies", "to_grade": "Hold", "From_grade": "Buy", "action": "down"}, {"date": 1359010412, "firm": "Oracle Investment Research", "to_grade": "Strong Buy", "From_grade": "", "action": "main"}, {"date": 1359011717, "firm": "Credit Suisse", "to_grade": "Outperform", "From_grade": "", "action": "main"}, {"date": 1359012629, "firm": "Deutsche Bank", "to_grade": "Buy", "From_grade": "", "action": "main"}, {"date": 1359013331, "firm": "BMO Capital", "to_grade": "Outperform", "From_gr

    #                                     Firm    To Grade From Grade Action
    # Date
    # 2012-02-09 08:17:00   Canaccord Genuity         Buy              main
    # 2012-03-08 07:33:00      FBN Securities  Outperform              main
    # 2012-03-13 06:08:00           Jefferies         Buy              main
    # 2012-03-14 06:13:00      Morgan Stanley  Overweight              main
    # 2012-03-14 08:21:00   Canaccord Genuity         Buy              main
    # ...                                 ...         ...        ...    ...
    # 2021-12-06 11:05:33             Keybanc  Overweight              init
    # 2021-12-07 13:04:51      Morgan Stanley  Overweight              main
    # 2021-12-13 14:36:22           JP Morgan  Overweight              main
    # 2021-12-14 11:26:08   B of A Securities         Buy    Neutral     up
    # 2021-12-14 13:13:36  Evercore ISI Group  Outperform              main

    recommendations_sql = ("""
        insert or ignore into sat_instrument_recomendation 
            (PK_hub, date_load, record_source, 
            date, firm, from_grade, to_grade, action)
            values (?, ?, ?, ?, ?, ?, ?, ?);
        """,
        [(hub_pk, date_load, record_source, a.get("date", None), a.get("firm", None), a.get("From_grade", None), a.get("to_grade", None), a.get("action", None)) for a in recommendations])

    yf_db.executemany(recommendations_sql[0], recommendations_sql[1]) # many
    yf_db.commit()

def fetch_and_save_daily_ticker(ticker, hub_pk, date_load, record_source, yf_db):

    # ------------------------- sat_instrument_symbol_1d
    
    d_hist = ticker.history(period="max", interval="1d", auto_adjust = True, prepost = True)
    d_timestamp = datetime_to_timestamp_s(d_hist.index)
    d_hist["date"] = d_timestamp
    d_hist = [dict(list(zip(d_hist.columns, r.values))) for (i,r) in d_hist.iterrows()]

    day_sql = ("""
        insert or ignore into sat_instrument_symbol_1d 
            (PK_hub, date_load, record_source, date,
                open, high, low, close, volume, dividend, stock_split)
            values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
        """,
        [(hub_pk, date_load, record_source, a.get("date", None), 
            a.get("Open", None), a.get("High", None), a.get("Low", None), a.get("Close", None), a.get("Volume", None), 
            a.get("Dividends", None), a.get("Stock Splits", None)) for a in d_hist])

    yf_db.executemany(day_sql[0], day_sql[1]) # many
    yf_db.commit()

def fetch_and_save_hourely_ticker(ticker, hub_pk, date_load, record_source, start_date, today, yf_db):

    # ------------------------- sat_instrument_symbol_1h
    
    h_hist = ticker.history(start = start_date, end = today, interval="1h", auto_adjust = True, prepost = True)
    h_timestamp = datetime_to_timestamp_s(h_hist.index.strftime("%Y-%m-%d %X"))
    h_hist["date"] = h_timestamp
    h_hist = [dict(list(zip(h_hist.columns, r.values))) for (i,r) in h_hist.iterrows()]

    hour_sql = ("""
        insert or ignore into sat_instrument_symbol_1h 
            (PK_hub, date_load, record_source, date,
                open, high, low, close, volume, dividend, stock_split)
            values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
        """,
        [(hub_pk, date_load, record_source, a.get("date", None), 
            a.get("Open", None), a.get("High", None), a.get("Low", None), a.get("Close", None), a.get("Volume", None), 
            a.get("Dividends", None), a.get("Stock Splits", None)) for a in h_hist])

    yf_db.executemany(hour_sql[0], hour_sql[1]) # many
    yf_db.commit()

def fetch_and_save_minute_ticker(ticker, hub_pk, date_load, record_source, start_date, today, yf_db):

    # ------------------------- sat_instrument_symbol_5m
    
    m_hist = ticker.history(start = start_date, end = today, interval = "5m", auto_adjust = True, prepost = True)
    m_timestamp = datetime_to_timestamp_s(m_hist.index.strftime("%Y-%m-%d %X"))
    m_hist["date"] = m_timestamp
    m_hist = [dict(list(zip(m_hist.columns, r.values))) for (i,r) in m_hist.iterrows()]

    min_sql = ("""
        insert or ignore into sat_instrument_symbol_5m 
            (PK_hub, date_load, record_source, date,
                open, high, low, close, volume, dividend, stock_split)
            values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
        """,
        [(hub_pk, date_load, record_source, a.get("date", None), 
            a.get("Open", None), a.get("High", None), a.get("Low", None), a.get("Close", None), a.get("Volume", None), 
            a.get("Dividends", None), a.get("Stock Splits", None)) for a in m_hist])

    yf_db.executemany(min_sql[0], min_sql[1]) # many
    yf_db.commit()

def fetch_all_aux_infos(symbol, yf_db):
    
    ticker = yf.Ticker(symbol)

    # option_chain = ticker.option_chain(today)
    # calls = option_chain.calls
    # puts = option_chain.puts

    hub_pk = md5(symbol.encode("utf-8")).hexdigest()
    date_load = datetime_to_timestamp_s([to_datetime(time.time(), unit="s").date()])[0]
    record_source = "yahoo finance"

    
    dt = time.time()
    wrap_tc(fetch_and_save_hub, ticker, hub_pk, date_load, record_source, symbol, yf_db)
    logging.info("fetch_and_save_hub time: " + str(dt - time.time()))
    
    dt = time.time()
    wrap_tc(fetch_and_save_meta_infos, ticker, hub_pk, date_load, record_source, yf_db)
    logging.info("fetch_and_save_meta_infos time: " + str(dt - time.time()))
    
    dt = time.time()
    wrap_tc(fetch_and_save_technical_infos, ticker, hub_pk, date_load, record_source, yf_db)
    logging.info("fetch_and_save_technical_infos time: " + str(dt - time.time()))
    
    dt = time.time()
    wrap_tc(fetch_and_save_metric_infos, ticker, hub_pk, date_load, record_source, yf_db)
    logging.info("fetch_and_save_metric_infos time: " + str(dt - time.time()))
    
    dt = time.time()
    wrap_tc(fetch_and_save_dividends, ticker, hub_pk, date_load, record_source, yf_db)
    logging.info("fetch_and_save_dividends time: " + str(dt - time.time()))
    
    dt = time.time()
    wrap_tc(fetch_and_save_splits, ticker, hub_pk, date_load, record_source, yf_db)
    logging.info("fetch_and_save_splits time: " + str(dt - time.time()))
    
    dt = time.time()
    wrap_tc(fetch_and_save_financials_year, ticker, hub_pk, date_load, record_source, yf_db)
    logging.info("fetch_and_save_financials_year time: " + str(dt - time.time()))
    
    dt = time.time()
    wrap_tc(fetch_and_save_financials_quater, ticker, hub_pk, date_load, record_source, yf_db)
    logging.info("fetch_and_save_financials_quater time: " + str(dt - time.time()))
    
    dt = time.time()
    wrap_tc(fetch_and_save_major_holders, ticker, hub_pk, date_load, record_source, yf_db)
    logging.info("fetch_and_save_major_holders time: " + str(dt - time.time()))
    
    dt = time.time()
    wrap_tc(fetch_and_save_institutional_holders, ticker, hub_pk, date_load, record_source, yf_db)
    logging.info("fetch_and_save_institutional_holders time: " + str(dt - time.time()))
    
    dt = time.time()
    wrap_tc(fetch_and_save_balance_sheets_year, ticker, hub_pk, date_load, record_source, yf_db)
    logging.info("fetch_and_save_balance_sheets_year time: " + str(dt - time.time()))
    
    dt = time.time()
    wrap_tc(fetch_and_save_balance_sheets_quarter, ticker, hub_pk, date_load, record_source, yf_db)
    logging.info("fetch_and_save_balance_sheets_quarter time: " + str(dt - time.time()))
    
    dt = time.time()
    wrap_tc(fetch_and_save_cashflow_year, ticker, hub_pk, date_load, record_source, yf_db)
    logging.info("fetch_and_save_cashflow_year time: " + str(dt - time.time()))
    
    dt = time.time()
    wrap_tc(fetch_and_save_cashflow_quarter, ticker, hub_pk, date_load, record_source, yf_db)
    logging.info("fetch_and_save_cashflow_quarter time: " + str(dt - time.time()))
    
    dt = time.time()
    wrap_tc(fetch_and_save_earnings, ticker, hub_pk, date_load, record_source, yf_db)
    logging.info("fetch_and_save_earnings time: " + str(dt - time.time()))
    
    dt = time.time()
    wrap_tc(fetch_and_save_earnings_quarter, ticker, hub_pk, date_load, record_source, yf_db)
    logging.info("fetch_and_save_earnings_quarter time: " + str(dt - time.time()))
    
    dt = time.time()
    wrap_tc(fetch_and_save_calendar, ticker, hub_pk, date_load, record_source, yf_db)
    logging.info("fetch_and_save_calendar time: " + str(dt - time.time()))
    
    dt = time.time()
    wrap_tc(fetch_and_save_sustainability, ticker, hub_pk, date_load, record_source, yf_db)
    logging.info("fetch_and_save_sustainability time: " + str(dt - time.time()))
    
    dt = time.time()
    wrap_tc(fetch_and_save_recomendations, ticker, hub_pk, date_load, record_source, yf_db)
    logging.info("fetch_and_save_recomendations time: " + str(dt - time.time()))

# ------------------------------------------------------------

if __name__ == "__main__":

    logging.basicConfig(level=logging.INFO)

    symbol_list = pd.read_csv(str(Path(__file__).absolute().parent) + "\\..\\..\\data\\american_stocks.csv")
    yf_db = sqlite3.connect(str(Path(__file__).absolute().parent) + "\\..\\..\\data\\yahoo_finance.db")

    
    logging.info("fetch meta data for: " + str(symbol_list))


    syms = [s["Symbol"] for (_, s) in symbol_list.iterrows()]

    for (i, sym) in enumerate(syms):
        
        dt = time.time()
        logging.info("fetching:     \t" + str(i) + " / " + str(len(symbol_list)) + "\t::\t" + str(sym + "\t:\t"))
        fetch_all_aux_infos(sym, yf_db)
        logging.info("done:         \t")
        logging.info("elapsed time: \t" + str(((time.time() - dt) * (len(symbol_list) - i)) / 60))
