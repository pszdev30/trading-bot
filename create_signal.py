import pipeline
import secrets
import globals
import btalib
import json
import requests
import pandas as pd
from datetime import datetime
from datetime import timedelta
from direct_redis import DirectRedis
from alpaca_trade_api.rest import REST

api = REST(secrets.APCA_API_KEY_ID, secrets.APCA_API_SECRET_KEY,
           secrets.APCA_API_PORTFOLIO_BASE_URL)
cache = DirectRedis(host='localhost', port=6379, db=0)
# cache.set('name', 'ashton')
# expire_datetime = datetime.now() + timedelta(seconds=60)
# cache.expireat('name', expire_datetime)


def ingest_stream(minute_bar):
    determine_signal(minute_bar)


def determine_signal(minute_bar):
    ticker = minute_bar.symbol
    price = minute_bar.close
    positions = api.list_positions()
    long_ema_weight = 0
    rsi_weight = 0
    macd = 0
    mfi = 0

    signal = None
    if (cache.exists('{ticker}_df'.format(ticker=ticker))):
        long_ema, rsi, macd_line, macd_signal, mfi = retrieve_cache(ticker)
        """
        trading strategy:
            1) check if ticker in long-term upward trend (thru long_ema)
            2) if yes, check if rsi: if <= 30 and macd crosses signal line, move on
            3) calculate pivot points for resistance prices
            4) if price crosses first resistance, use mfi to confirm breakout
        """
        if (long_ema > price):
            return signal
        else:
            pass
    else:
        fill_cache(ticker)

    return


def fill_cache(ticker):
    cache.set('{ticker}_df'.format(ticker=ticker), pd.read_csv(
        'bta-data/{ticker}.txt'.format(ticker=ticker), parse_dates=True, index_col='Date'))
    df = cache.get('{ticker}_df'.format(ticker=ticker))
    cache.set('{ticker}_long_ema'.format(ticker=ticker), ema(df=df))
    cache.set('{ticker}_rsi'.format(ticker=ticker), rsi(df=df))
    cache.set('{ticker}_mfi'.format(ticker=ticker), mfi(df=df))
    cache.lpush('{ticker}_macd'.format(ticker=ticker), macd(df=df))
    cache.rpush('{ticker}_macd'.format(ticker=ticker), macd_signal(df=df))
    expire_datetime = datetime.now() + timedelta(hours=8)
    # expire_timestamp = datetime.timestamp(expire_datetime)
    cache.expireat('{ticker}_macd', expire_datetime)


def retrieve_cache(ticker):
    return cache.get('{ticker}_long_ema'.format(ticker=ticker)), cache.get('{ticker}_rsi'.format(ticker=ticker)),
    cache.get('{ticker}_macd'.format(ticker=ticker)), cache.get(
        '{ticker}_mfi'.format(ticker=ticker))


def trade(ticker, signal):
    requests.post(secrets.AWS_REST_API + signal, data={'ticker': ticker})
    return


def ema(df):
    return btalib.ema(df, period=200).df.ema.iat[-1]


def rsi(df):
    return btalib.rsi(df).df.rsi.iat[-1]


def macd(df):
    return str(btalib.macd(df).df.macd.iat[-1])


def macd_signal(df):
    return str(btalib.macd(df).df.signal.iat[-1])


def mfi(df):
    return btalib.mfi(df, period=7).df.mfi.iat[-1]


df = pd.read_csv('bta-data/MSFT.txt',
                 parse_dates=True, index_col='Date')

# pd.set_option('display.max_rows', None)
print(mfi(df=df))

# print(api.list_positions())


# print(determine_signal())
