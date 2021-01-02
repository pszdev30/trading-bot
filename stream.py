import alpaca_trade_api as alpaca
import websocket
import json
import pandas as pd


# Alpaca API config stuff

APCA_API_BASE_URL = 'https://paper-api.alpaca.markets'
APCA_API_KEY = 'PKAYAWQZMFDA9J1XTBXH'
APCA_SECRET_KEY = 'KHml00XOyP9ar8TiFtr6ZwBgn5KCRK1hk0Ed7L6i'

# api = alpaca.REST(APCA_API_KEY, APCA_SECRET_KEY, APCA_API_BASE_URL)


AUTHENTICATE = {
    "action": "authenticate",
    "data": {
        "key_id": APCA_API_KEY,
        "secret_key": APCA_SECRET_KEY
    }
}

listen = {
    'action': 'listen', 'data': {'streams': ['AM.MSFT', 'AM.FB', 'AM.CMCSA', 'AM.COKE']}
}


def on_open(ws):
    ws.send(json.dumps(AUTHENTICATE))
    ws.send(json.dumps(listen))


def on_message(ws, message):
    print(message)


def on_close(ws):
    pass


socket = 'wss://data.alpaca.markets/stream'

ws = websocket.WebSocketApp(socket, on_open=on_open,
                            on_message=on_message, on_close=on_close)
ws.run_forever()
