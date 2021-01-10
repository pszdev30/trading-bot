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

    # return two_hundred_sma(df), fifty_sma(df)
    return macd(df)


def fifty_sma(df):
    sma = btalib.sma(df, period=50).df
    return sma


def two_hundred_sma(df):
    sma = btalib.sma(df, period=200).df
    return sma


def macd(df):
    macd = btalib.macd(df).df
    return macd
    # pass


z = calc_indicators('MSFT')
# g = calc_indicators('MSFT')

print(z)
