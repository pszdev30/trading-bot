import btalib
import pandas as pd
import json
import requests
import pipeline
from secrets import *
from globals import *


def calc_indicators(ticker):
    df = pd.read_csv('bta-data/{}.txt'.format(ticker),
                     parse_dates=True, index_col='Date')

    return two_hundred_ema(df), two_hundred_sma(df)


def two_hundred_sma(df):
    sma = btalib.sma(df, period=200).df
    return sma


def two_hundred_ema(df):
    ema = btalib.ema(df, period=200).df
    return ema


def macd(ticker):
    pass


z = calc_indicators('MSFT')
# g = calc_indicators('MSFT')

print(z)
