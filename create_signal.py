from datetime import datetime
from datetime import timedelta
from direct_redis import DirectRedis
from alpaca_trade_api.rest import REST
from scipy.interpolate import make_interp_spline, BSpline
import btalib
import json
import requests
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
import sys

apca_secrets = aws_secrets.get_secrets('APCA-SECRETS')
aws_services_secrets = aws_secrets.get_secrets(
    'TRADING-BOT-AWS-SERVICES-SECRETS')
APCA_API_KEY_ID = apca_secrets["APCA-API-KEY-ID"]
APCA_API_SECRET_KEY = apca_secrets["APCA-API-SECRET-KEY"]
APCA_DATA_BARS_URL = apca_secrets['APCA-DATA-BARS-V1-URL']
HEADERS = json.loads(apca_secrets["HEADERS"])
HEADERS['APCA-API-KEY-ID'] = APCA_API_KEY_ID
HEADERS['APCA-API-SECRET-KEY'] = APCA_API_SECRET_KEY
REDIS_ELASTICACHE_CLUSTER_URL = aws_services_secrets[
    'REDIS-ELASTICACHE-CLUSTER-URL']


def start(minute_bar):
    try:
        minute_bar = sys.argv[0]
    except:
        sys.exit()

    api = REST(secrets.APCA_API_KEY_ID, secrets.APCA_API_SECRET_KEY,
               secrets.APCA_API_PORTFOLIO_BASE_URL)
    cache = DirectRedis.from_url(REDIS_ELASTICACHE_CLUSTER_URL)
    ingest_stream(minute_bar)
    # cache.set('name', 'ashton')
    # expire_datetime = datetime.now() + timedelta(seconds=60)
    # cache.expireat('name', expire_datetime)


def ingest_stream(minute_bar):
    json_minute_bar = json.loads(minute_bar)[0]
    determine_signal(json_minute_bar)


def determine_signal(minute_bar):
    ticker = minute_bar['S']
    price = minute_bar['c']
    # print(ticker, price)
    positions = api.list_positions()
    long_ema_weight = 0
    rsi_weight = 0
    macd = 0
    mfi = 0
    signal = None
    # if (cache.exists('{ticker}_df'.format(ticker=ticker))):
    #     long_ema, rsi, macd_line, macd_signal, mfi = retrieve_cache(ticker)
    #     """
    #     trading strategy:
    #         1) check if ticker in long-term upward trend (thru long_ema) and macd + signal lines are at least 20
    #         2) if yes, check if rsi: if <= 30
    #         3) calculate pivot points for resistance prices
    #         4) if price crosses first resistance, use mfi to confirm breakout
    #     """
    #     # if (long_ema > price):
    #     #     return signal
    #     # else:
    #     #     if (macd_line > 20 and macd_signal > 20 and rsi <= 30):
    #     #         r1, s1 = calculate_pivot(ticker)
    #     #         if (price > r1):
    #     #             pass
    # else:
    #     fill_cache(ticker)

    # return


def calculate_pivot(ticker):
    # PP = (HIGHprev + LOWprev + CLOSEprev) / 3
    # R1 = PP * 2 - LOWprev
    # S1 = PP * 2 - HIGHprev
    # R2 = PP + (HIGHprev - LOWprev)
    df = cache.get('{ticker}_df'.format(ticker=ticker))
    prev_high = df['High']
    prev_low = df['Low']
    prev_close = df['Close']
    pivot_point = (prev_high + prev_low + prev_close) / 3
    r1 = pivot_point * 2 - prev_low
    s1 = pivot_point * 2 - prev_high
    return r1, s1


def fill_cache(ticker):
    cache.set(
        '{ticker}_df'.format(ticker=ticker),
        pd.read_csv('bta-data/{ticker}.txt'.format(ticker=ticker),
                    parse_dates=True,
                    index_col='Date'))
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
    return cache.get('{ticker}_long_ema'.format(ticker=ticker)), cache.get(
        '{ticker}_rsi'.format(ticker=ticker)),
    cache.get('{ticker}_macd'.format(ticker=ticker)), cache.get(
        '{ticker}_mfi'.format(ticker=ticker))


def trade(ticker, signal):
    # needs replaced
    requests.post(secrets.AWS_REST_API + signal,
                  data={
                      'ticker': ticker,
                      'stop_loss': 12
                  })
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
    return btalib.mfi(df, period=7).df.mfi


stock = 'EA'
df = pd.read_csv('bta-data/{}.txt'.format(stock),
                 parse_dates=True,
                 index_col='Date')

pd.set_option('display.max_rows', None)
# print(mfi(df=df))
# res = mfi(df=df)
# print(res)
# df['date'] = df.index

# x = [date for date in df['date'].tail(7)]
# y = [mfi for mfi in res.tail(7)]
# plt.plot(x, y)
# plt.xlabel('Day')
# plt.ylabel('MFI')
# plt.title('MFI Slope Test: {}'.format(stock))

# adjustedX = [1, 2, 3, 4, 5, 6, 7]
# numpyX = np.array(adjustedX)
# numpyY = np.array(y)
# m, b = np.polyfit(numpyX, numpyY, 1)

# newX = np.linspace(numpyX.min(), numpyX.max(), 300)
# spl = make_interp_spline(numpyX, numpyY, k=3)
# power_smooth = spl(newX)

# plt.plot(newX, power_smooth)

# abline_values = [m * i + b for i in numpyX]
# plt.plot(numpyX, abline_values, 'b')

# plt.plot(numpyX, numpyY, 'o')
# plt.plot(numpyX, m*numpyX + b)

# plt.show()

# print(api.list_positions())

# print(determine_signal())

# start()
