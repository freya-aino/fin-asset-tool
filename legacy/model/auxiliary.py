import pandas as pd
from datetime import datetime


def timestamp_s_to_datetime(timestamp):
    return datetime.fromtimestamp(int(timestamp)).strftime("%y-%m-%d %H:%M:%S")

def datetime_to_timestamp_s(datetime):
    return pd.to_numeric(pd.to_datetime(datetime)) / pow(10, 9)

# date = pd.to_datetime(timestamp, unit="s", origin="unix")
# hist["weekday"]  = date.weekday
# hist["monthday"] = date.day
# hist["month"]    = date.month