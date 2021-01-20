import alpaca_trade_api as alpaca
import requests
from datetime import datetime
import json
from globals import *
from secrets import *


api = alpaca.REST(APCA_API_KEY_ID, APCA_API_SECRET_KEY,
                  APCA_API_PORTFOLIO_BASE_URL)


def get_screened_ticker_data():
    timeframe = '1D'
    symbols = ','.join(screened_tickers)
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
