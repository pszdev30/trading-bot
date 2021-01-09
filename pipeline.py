import requests
import json
from globals import *
from secrets import *


def get_screened_ticker_data():
    timeframe = '1D'
    symbols = ','.join(screened_tickers)
    limit = 200

    DAY_BAR_URL = APCA_DATA_BARS_URL + \
        '/{}?symbols={}&limit={}'.format(timeframe, symbols, limit)

    r = requests.get(DAY_BAR_URL, headers=HEADERS)

    data = json.dumps(r.json(), indent=4)

    transform(data)


def transform():
    pass

    # with open('output.txt', 'w') as output:
    #     output.write(data)
