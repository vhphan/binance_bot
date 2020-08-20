##
from pprint import pprint

from binance.client import Client
from sqlalchemy import create_engine

from private.keys import binance_paper_keys, connection_strings
import pandas as pd
import os
from binance.websockets import BinanceSocketManager
from twisted.internet import reactor

##
binance_client = Client(binance_paper_keys.get('api'), binance_paper_keys.get('secret_key'))

##
binance_client.API_URL = 'https://testnet.binance.vision/api'

##
# get balances for all assets & some account information
pprint(binance_client.get_account())

# %%
# get balance for a specific asset only (BTC)
pprint(binance_client.get_asset_balance(asset='BTC'))

# %%
# get latest price from Binance API
btc_price = binance_client.get_symbol_ticker(symbol="BTCUSDT")
# print full output (dictionary)
print(btc_price)

# %%
hist = binance_client.get_historical_klines('BTCUSDT',
                                            binance_client.KLINE_INTERVAL_5MINUTE,
                                            start_str='2020-07-01',
                                            end_str='2020-07-02',
                                            limit=10)
pprint(hist)

# %%
btc_price = dict(error=False)


def btc_trade_history(msg):
    """ define how to process incoming WebSocket messages """
    if msg['e'] != 'error':
        print(msg['c'])
        btc_price['last'] = msg['c']
        btc_price['bid'] = msg['b']
        btc_price['last'] = msg['a']
    else:
        btc_price['error'] = True

    pprint(btc_price)


##
def print_resp(msg):
    pprint(msg)


##
# init and start the WebSocket
bsm = BinanceSocketManager(binance_client)
# conn_key = bsm.start_symbol_ticker_socket('BTCUSDT', btc_trade_history)
conn_key = bsm.start_kline_socket('BTCUSDT', print_resp, interval=binance_client.KLINE_INTERVAL_1MINUTE)
bsm.start()

##
# # stop websocket
bsm.stop_socket(conn_key)
#
# # properly terminate WebSocket
reactor.stop()
