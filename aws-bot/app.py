from chalice import Chalice
import alpaca_trade_api as alpaca
import pandas as pd
import json
from secrets import *


app = Chalice(app_name='aws-bot')

api = alpaca.REST(APCA_API_KEY_ID, APCA_API_SECRET_KEY,
                  APCA_API_PORTFOLIO_BASE_URL)


@app.route('/')
def index():
    return api.list_positions()


@app.route('/buy', methods=['POST'])
def buy():
    request = app.current_request
    ticker = request.json_body['ticker']

    api.submit_order(
        symbol=ticker,
        side='buy',
        qty='100',
        type='market',
        time_in_force='day',
        order_class='simple',
    )

    return


@app.route('/sell', methods=['POST'])
def sell():
    request = app.current_request
    ticker = request.json_body['ticker']

    api.close_position(symbol=ticker, qty='100')

    return


# The view function above will return {"hello": "world"}
# whenever you make an HTTP GET request to '/'.
#
# Here are a few more examples:
#
# @app.route('/hello/{name}')
# def hello_name(name):
#    # '/hello/james' -> {"hello": "james"}
#    return {'hello': name}
#
# @app.route('/users', methods=['POST'])
# def create_user():
#     # This is the JSON body the user sent in their POST request.
#     user_as_json = app.current_request.json_body
#     # We'll echo the json body back to the user in a 'user' key.
#     return {'user': user_as_json}
#
# See the README documentation for more examples.
#
