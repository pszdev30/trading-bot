import alpaca_trade_api as alpaca
import websocket
import json
import pandas as pd
from secrets import *
import ta


# Alpaca API config stuff

AUTHENTICATE = {
    "action": "authenticate",
    "data": {
        "key_id": APCA_API_KEY,
        "secret_key": APCA_SECRET_KEY
    }
}

tickers = ['AM.MSFT', 'AM.FB', 'AM.CMCSA', 'AM.KO']

listen = {
    'action': 'listen', 'data': {'streams': ['alpacadatav1/AM.MSFT', 'alpacadatav1/AM.FB']}
}


def on_open(ws):
    ws.send(json.dumps(AUTHENTICATE))
    ws.send(json.dumps(listen))


def on_message(ws, minute_bar):

    ta.ingest_stream(minute_bar)


def on_close(ws):
    print('closed connection')


socket = 'wss://data.alpaca.markets/stream'

ws = websocket.WebSocketApp(socket, on_open=on_open,
                            on_message=on_message, on_close=on_close)
ws.run_forever()
