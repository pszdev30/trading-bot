import btalib
import pandas as pd
import json
import requests
import pipeline
from secrets import *
from globals import *


def ingest_stream(minute_bar):
    print(minute_bar)


def calc_indicators(ticker):
    df = pd.read_csv('bta-data/{}.txt'.format(ticker),
                     parse_dates=True, index_col='Date')

    return macd(df), rsi(df)


def macd(df):
    macd = btalib.macd(df).df
    return macd


def rsi(df):
    return btalib.rsi(df).df


# def dx(df):
#     return btalib.dx(df).df


# def adx(df):
#     return btalib.adx(df).df

# def fifty_sma(df):
#     sma = btalib.sma(df, period=50).df
#     return sma


# def two_hundred_sma(df):
#     sma = btalib.sma(df, period=200).df
#     return sma


z = calc_indicators('MSFT')
# g = calc_indicators('MSFT')

print(z)
