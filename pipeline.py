# import alpaca_trade_api as alpaca
from alpaca_trade_api.rest import REST, TimeFrame
from secrets import *
from datetime import datetime, timedelta
import globals
import pandas as pd
import requests
import json
import redis


api = REST(APCA_API_KEY_ID, APCA_API_SECRET_KEY,
           APCA_API_PORTFOLIO_BASE_URL)


def get_screened_ticker_data():
    two_hundred_days_ago = (datetime.now() - timedelta(days=200)).date()
    start = pd.Timestamp(two_hundred_days_ago, tz=globals.NY).isoformat()
    end = 'now'
    timeframe = '1D'

    symbols = ','.join(globals.screened_tickers)
    limit = 200

    DAY_BAR_URL = APCA_DATA_BARS_URL + \
        '/{}?symbols={}&limit={}'.format(timeframe, symbols, limit)

    r = requests.get(DAY_BAR_URL, headers=HEADERS)
    data = r.json()

    return data


def transform():
    data = get_screened_ticker_data()
    for ticker in data:
        file = 'bta-data/{}.txt'.format(ticker)
        f = open(file, 'w+')
        f.write('Date,Open,High,Low,Close,Volume,OpenInterest\n')

        for bar in data[ticker]:
            time = datetime.fromtimestamp(bar['t'])
            date = time.strftime('%Y-%m-%d')
            day_entry = '{},{},{},{},{},{},{}\n'.format(
                date, bar['o'], bar['h'], bar['l'], bar['c'], bar['v'], 0.00)
            f.write(day_entry)


transform()


# with open('output.txt', 'w') as output:
#     output.write(data)
# print(json.dumps(data['MSFT']['t'], indent=4))
