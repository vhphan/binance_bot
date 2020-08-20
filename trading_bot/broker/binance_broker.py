import math
import pathlib
import time
from datetime import datetime

from binance.exceptions import BinanceAPIException, BinanceOrderException
# from binance.helpers import date_to_milliseconds
from binance.helpers import date_to_milliseconds

from trading_bot.broker.broker import Broker
from pprint import pprint

import pendulum
from binance.client import Client
from sqlalchemy import create_engine

from private.keys import binance_paper_keys, connection_strings, binance_keys
import pandas as pd
import os
from binance.websockets import BinanceSocketManager
from twisted.internet import reactor

import pendulum

bin_minutes = {"1m": 1, "5m": 5, "1h": 60, "4h": 240, "1d": 1440}


class StepSizeNotFound(Exception):
    """Raised when the stepSize not found"""
    pass


class DataNotInFile(Exception):
    pass


class BinanceBroker:
    PRACTICE_API_HOST = 'https://testnet.binance.vision/api'

    def __init__(self, is_live=False):

        # need true client for accurate historical data regardless of is_live
        self.true_client = Client(binance_keys.get('api'), binance_keys.get('secret_key'))
        self.test_client = Client(binance_paper_keys.get('api'), binance_paper_keys.get('secret_key'))

        if is_live:
            self.client = self.true_client
        else:
            self.client = self.test_client
            self.client.API_URL = 'https://testnet.binance.vision/api'
        self.bsm = BinanceSocketManager(self.client)
        self.conn_key = []
        self.all_symbols = []

        self.__price_event_handler = None
        self.__order_event_handler = None
        self.__position_event_handler = None

        self.stream_params = None

    @property
    def on_price_event(self):
        """
        Listeners will receive candles
        """
        return self.__price_event_handler

    @on_price_event.setter
    def on_price_event(self, event_handler):
        self.__price_event_handler = event_handler

    @property
    def on_order_event(self):
        """
        Listeners will receive:
        transaction_id
        """
        return self.__order_event_handler

    @on_order_event.setter
    def on_order_event(self, event_handler):
        self.__order_event_handler = event_handler

    @property
    def on_position_event(self):
        """
        Listeners will receive:
        symbol, is_long, units, unrealized_pnl, pnl
        """
        return self.__position_event_handler

    @on_position_event.setter
    def on_position_event(self, event_handler):
        self.__position_event_handler = event_handler

    def get_symbols(self):
        self.all_symbols = self.client.get_all_tickers()

    def get_account_balance(self):
        return self.client.get_account()

    def get_historical_data(self, symbol, params):

        filename = f'binance_{symbol}'
        # for key, value in params.items():
        #     filename += '_' + str(value)
        filename += '_' + params.get('interval')
        filename += '.csv'

        current_file_path = pathlib.Path(__file__).parent.absolute()
        output_file_path = current_file_path.joinpath('data', filename)

        try:

            df = pd.read_csv(output_file_path)
            max_data_dt = df['datetime'].max()
            min_data_dt = df['datetime'].min()
            start_dt = params.get('start_str')
            end_dt = params.get('end_str')
            if not (min_data_dt <= start_dt <= end_dt <= max_data_dt):
                raise DataNotInFile
            # check if required data in csv

        except (FileNotFoundError, DataNotInFile) as e:
            dict_crypto = self.true_client.get_historical_klines(symbol=symbol, **params)
            columns = ['datetime', 'open', 'high', 'low',
                       'close', 'volume', 'close time', 'quote asset volume',
                       'number of trades', 'taker buy base asset volume',
                       'taker buy quote asset volume', 'ignore']

            df = pd.DataFrame(dict_crypto, columns=columns)

            if isinstance(e, DataNotInFile):
                # merge into existing csv
                df_file = pd.read_csv(output_file_path)
                final_df = pd.concat([df_file, df])
                final_df.drop_duplicates(['datetime'], inplace=True)
            else:
                final_df = df
            final_df.to_csv(output_file_path, index=False)

        df['Asset'] = symbol
        df['datetime'] = pd.to_datetime(df['datetime'], unit='ms')
        df[['open', 'high', 'low', 'close', 'volume']] = df[['open', 'high', 'low', 'close', 'volume']].apply(
            pd.to_numeric,
            errors='coerce')
        df.dropna(inplace=True)
        df.set_index('datetime', inplace=True)
        df['symbol'] = symbol
        df['interval'] = params.get('interval')

        return df[['open', 'high', 'low', 'close', 'volume', 'symbol', 'interval']]

    def previous_candles(self, symbol, min_candles=200, interval=None):

        if interval is None:
            interval = self.client.KLINE_INTERVAL_4HOUR
        now = pendulum.now('UTC')
        start = now.subtract(minutes=min_candles * bin_minutes.get(interval))
        end = now.add(minutes=10 * bin_minutes.get(interval))  # add some buffer to end date
        df = self.get_historical_data(symbol,
                                      dict(interval=interval,
                                           start_str=start.int_timestamp * 1000,
                                           end_str=end.int_timestamp * 1000,
                                           ))
        return df

    def process_price(self, price):
        #
        # {
        #   "e": "kline",     // Event type
        #   "E": 123456789,   // Event time
        #   "s": "BNBBTC",    // Symbol
        #   "k": {
        #
        #     "t": 123400000, // Kline start time
        #     "T": 123460000, // Kline close time
        #     "s": "BNBBTC",  // Symbol
        #     "i": "1m",      // Interval
        #     "f": 100,       // First trade ID
        #     "L": 200,       // Last trade ID
        #     "o": "0.0010",  // Open price
        #     "c": "0.0020",  // Close price
        #     "h": "0.0025",  // High price
        #     "l": "0.0015",  // Low price
        #     "v": "1000",    // Base asset volume
        #     "n": 100,       // Number of trades
        #     "x": false,     // Is this kline closed?
        #     "q": "1.0000",  // Quote asset volume
        #     "V": "500",     // Taker buy base asset volume
        #     "Q": "0.500",   // Taker buy quote asset volume
        #     "B": "123456"   // Ignore
        #   }
        # }

        # if msg['e'] == 'error':
        #     # close and restart the socket
        #     else:
        #     # process message normally

        print(price)

        if price.get('e') == 'error':
            # close and restart socker
            self.stop_stream()
            self.stream_prices()
            return

        if price.get('k').get('x'):
            kline = price.get('k')
            df = pd.DataFrame.from_dict(kline, orient='index').T
            df = df[['t', 'o', 'h', 'l', 'c', 'v', 's', 'i']]
            df.columns = ['datetime', 'open', 'high', 'low', 'close', 'volume', 'symbol', 'interval']
            df['datetime'] = pd.to_datetime(df['datetime'], unit='ms')
            df.set_index('datetime', inplace=True)
            self.on_price_event(df)

    def stream_prices(self, **params):
        # symbols, interval
        if params is None and self.stream_params is not None:
            params = self.stream_params
        elif params is not None:
            # save the streaming parameters
            self.stream_params = params
        else:
            print('error... invalid streaming parameters')
            self.__exit__()

        symbols = params.get('symbols')
        interval = params.get('interval')
        for symbol in symbols:
            self.conn_key.append(self.bsm.start_kline_socket(symbol, self.process_price, interval=interval))
        self.bsm.start()

    def stop_stream(self):
        self.bsm.stop_socket(self.conn_key)

    def __exit__(self, exc_type, exc_val, exc_tb):
        # stop websocket
        self.bsm.stop_socket(self.conn_key)
        # properly terminate WebSocket
        reactor.stop()

    def send_market_order(self, symbol, quantity, is_buy, buy_type='MARKET', price=None, time_in_force='GTC'):
        try:
            if is_buy:
                params = dict(symbol=symbol,
                              side='BUY',
                              type=buy_type,
                              quantity=quantity)

                if buy_type == 'LIMIT':
                    params['price'] = price
                    params['timeInForce'] = time_in_force
                # client.create_order(symbol=symbol, side=side, type=order_type, quantity=quantity)
                buy_order = self.client.create_order(
                    **params
                )
                pprint(buy_order)
                return buy_order

        except BinanceAPIException as e:
            # error handling goes here
            print(e)
        except BinanceOrderException as e:
            # error handling goes here
            print(e)

    def get_positions(self):
        # [{'clientOrderId': 'CDECpePXJi04rf2yfvBWIe',
        #   'cummulativeQuoteQty': '0.00020000',
        #   'executedQty': '0.10000000',
        #   'icebergQty': '0.00000000',
        #   'isWorking': True,
        #   'orderId': 224,
        #   'orderListId': -1,
        #   'origQty': '0.10000000',
        #   'origQuoteOrderQty': '0.00000000',
        #   'price': '0.00000000',
        #   'side': 'BUY',
        #   'status': 'FILLED',
        #   'stopPrice': '0.00000000',
        #   'symbol': 'BNBBTC',
        #   'time': 1596848973611,
        #   'timeInForce': 'GTC',
        #   'type': 'MARKET',
        #   'updateTime': 1596848973611}]
        return self.client.get_all_orders(symbol=symbol)

    def process_historical_price(self, df):
        pass

    def position_sizing(self):
        pass

    def get_lot(self, symbol, price, initial_quantity, base='BTC'):
        symbol_info = self.client.get_symbol_info(symbol)
        filters = symbol_info.get('filters')
        # search filter=LOT_SIZE
        step_size = None
        min_notional = None
        for filter in filters:
            if filter.get('filterType') == 'LOT_SIZE':
                step_size = float(filter.get('stepSize'))
                min_quantity = float(filter.get('minQty'))
                max_quantity = float(filter.get('maxQty'))
            if filter.get('filterType') == 'MIN_NOTIONAL':
                min_notional = float(filter.get('minNotional'))

        if step_size is None:
            raise StepSizeNotFound

        balance = self.client.get_asset_balance(asset=base)
        temp = min(initial_quantity, float(balance['free']))
        q2 = temp / float(price) * 0.9995

        precision = int(round(-math.log(step_size, 10), 0))

        final_quantity = float(self.roundup(q2, precision))
        if final_quantity < min_notional:
            return None
        return final_quantity

    @staticmethod
    def roundup(num, precision):
        return math.ceil(num * 10 ** precision) / 100.0 * precision


if __name__ == '__main__':
    symbol = 'ETHUSDT'

    bb = BinanceBroker(is_live=False)
    pprint(bb.client.get_symbol_info(symbol))

    balances = bb.get_account_balance().get('balances')

    # bb.get_symbols()
    # pprint(bb.all_symbols)
    #

    ticker = bb.client.get_symbol_ticker(symbol=symbol)
    ticker_true = bb.true_client.get_symbol_ticker(symbol=symbol)
    price = ticker.get('price')
    pprint(price)
    # adjusted_quantity = bb.get_lot(symbol, price, 0.0001, base='BTC')
    # order = bb.send_market_order(symbol=symbol, quantity=adjusted_quantity, is_buy=True)
    #
    # positions = bb.get_positions()
    # print(positions)
    #
    # open_orders = bb.client.get_open_orders(symbol=symbol)
    # my_trades = bb.client.get_my_trades(symbol=symbol)
    #
    # symbol_info = bb.client.get_symbol_info('BNBBTC')
