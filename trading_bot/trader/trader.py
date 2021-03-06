import datetime as dt
import inspect

import pandas as pd
import talib as ta
import btalib as bta
from binance.enums import ORDER_STATUS_FILLED

from lib.error_decorator import handle_exception
from trading_bot.broker.binance_broker import BinanceBroker

MIN_CANDLES = 100

pd.options.display.max_columns = None


class Trader(object):
    def __init__(
            self, broker, interval, symbol, units
    ):
        """
        A trading platform.

        :param broker: Broker object
        :param symbol: A str object recognized by the broker for trading
        :param units: Number of units to trade in base asset, eg. ETHUSDT = unit of ETHER
        :param resample_interval:
            Frequency for resampling price time series

        """
        self.broker = self.setup_broker(broker)
        self.symbol = symbol

        # self.df_prices = pd.DataFrame(
        #     columns=['datetime',
        #              'open',
        #              'high',
        #              'low',
        #              'close',
        #              'volume',
        #              'symbol',
        #              'interval'])
        self.df_prices = self.broker.previous_candles(symbol, interval=interval)
        self.pnl, self.upnl = {symbol: 0}, {symbol: 0}
        self.position = {symbol: 0}
        self.is_order_pending = False
        self.is_next_signal_cycle = True
        self.interval = interval
        self.price_event_count = 0
        self.units = units
        self.base = self.broker.client.get_symbol_info(symbol).get('quoteAsset')

        self.RSI_OVERBOUGHT = 60
        self.RSI_OVERSOLD = 50

    class Decorators:
        @staticmethod
        def safe_run(func):
            def wrapper(this, *args, **kwargs):
                try:
                    return func(this, *args, **kwargs)
                except Exception as e:
                    handle_exception(e)

            return wrapper

    def test_exception(self):
        raise Exception('test exception')

    def setup_broker(self, broker):
        broker.on_price_event = self.on_price_event
        broker.on_order_event = self.on_order_event
        broker.on_position_event = self.on_position_event
        return broker

    def on_price_event(self, df):
        print(f'{inspect.currentframe().f_code.co_name} triggered')
        self.df_prices = self.df_prices.append(df)

        if self.price_event_count == 0:
            # remove duplicates in case 1st candle from wss is duplicate with historical
            self.df_prices = self.df_prices.reset_index().drop_duplicates(subset='datetime',
                                                                          keep='last').set_index('datetime')

        print(self.df_prices)
        self.price_event_count += 1
        # self.get_positions()
        self.generate_signals_and_think()

    def get_positions(self):
        print(f'{inspect.currentframe().f_code.co_name} triggered')
        try:
            self.broker.get_positions()
        except Exception as ex:
            print('get_positions error:', ex)

    def on_order_event(self, symbol, quantity, is_buy, transaction_id, status):
        print(f'{inspect.currentframe().f_code.co_name} triggered')
        print(
            dt.datetime.now(), '[ORDER]',
            'transaction_id:', transaction_id,
            'status:', status,
            'symbol:', symbol,
            'quantity:', quantity,
            'is_buy:', is_buy,
        )
        if status == 'FILLED':
            self.is_order_pending = False
            self.is_next_signal_cycle = False

    def on_position_event(self, symbol, is_long, units, upnl, pnl):
        print(f'{inspect.currentframe().f_code.co_name} triggered')
        if symbol == self.symbol:
            self.position[symbol] = abs(units) * (1 if is_long else -1)
            self.pnl[symbol] = pnl
            self.upnl[symbol] = upnl

    def generate_signals_and_think(self):
        print(f'{inspect.currentframe().f_code.co_name} triggered')

        print('thinking...')
        # Strategy goes here generate signals.
        is_signal_buy, is_signal_sell = False, False

        # 1. Check if sufficient samples
        if len(self.df_prices) < MIN_CANDLES:
            print(f'insufficient candles...{len(self.df_prices)} candles... waiting for more...')
            return

        # simple rsi strategy
        rsi = ta.RSI(self.df_prices['close'], 14)
        print("all rsis calculated so far")
        print(rsi)
        last_rsi = rsi[-1]

        if last_rsi > self.RSI_OVERBOUGHT:
            if self.position.get(self.symbol) > 0:
                print("Overbought! Sell!")
                # put binance sell logic here
                is_signal_sell = True
            else:
                print("It is overbought, but we don't own any. Nothing to do.")

        if last_rsi < self.RSI_OVERSOLD:
            if self.position.get(self.symbol) > 0:
                print("It is oversold, but you already own it, nothing to do.")
            else:
                print("Oversold! Buy!")
                # put binance buy order logic here
                is_signal_buy = True

        self.think(is_signal_buy, is_signal_sell)

    def think(self, is_signal_buy, is_signal_sell):
        print(f'{inspect.currentframe().f_code.co_name} triggered')

        if self.is_order_pending:
            return

        if is_signal_buy:
            # order = self.send_market_order(self.symbol, self.units, is_buy=True)
            # order_status = order.get('status')
            # print(f'order status = {order_status}')
            # if order_status == ORDER_STATUS_FILLED:
            #     self.position = 1
            self.position[self.symbol] = 1
            print('bought some candy')

        if is_signal_sell and self.position[self.symbol] == 1:
            # order = self.send_market_order(self.symbol, self.units, is_buy=False)
            # order_status = order.get('status')
            # print(f'order status = {order_status}')
            # if order_status == 'FILLED':
            #     self.position[self.symbol] = 0
            self.position[self.symbol] = 0
            print('sold some goodies')

    def send_market_order(self, symbol, quantity, is_buy):
        # price = self.broker.client.get_symbol_ticker(symbol=symbol).get('price')
        # adjusted_quantity = self.broker.get_lot(symbol, price, self.units, base=self.base)
        order = self.broker.send_market_order(symbol, quantity, is_buy)
        return order

    def run(self):
        self.broker.stream_prices(symbols=[self.symbol], interval=self.interval)


if __name__ == '__main__':
    broker = BinanceBroker(is_live=False)
    trader = Trader(broker=broker, interval='1m', symbol='ETHUSDT', units=0.05)
    trader.run()
