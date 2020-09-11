import datetime as dt
import pandas as pd
import talib as ta
import btalib as bta
from trading_bot.broker.binance_broker import BinanceBroker

pd.options.display.max_columns = None


class TraderAll(object):
    def __init__(
            self, broker, interval, units
    ):
        """
        A trading platform.

        :param broker: Broker object
        :param units: Number of units to trade in quote, eg. ETHUSDT = unit of ETHER
        :param resample_interval:
            Frequency for resampling price time series

        """
        self.broker = self.setup_broker(broker)
        self.all_symbols = self.broker.get_symbols()

        # self.df_prices = pd.DataFrame(
        #     columns=['datetime',
        #              'open',
        #              'high',
        #              'low',
        #              'close',
        #              'volume',
        #              'symbol',
        #              'interval'])
        self.df_prices = {self.broker.previous_candles(symbol, interval=interval)
                          for symbol in self.symbols}
        self.pnl, self.upnl = 0, 0
        self.position = 0
        self.is_order_pending = False
        self.is_next_signal_cycle = True
        self.interval = interval
        self.price_event_count = 0
        self.units = units
        self.symbols = [symbol for symbol in self.symbols
                        if self.broker.client.get_symbol_info(symbol).get('quoteAsset') == 'USDT']

        self.RSI_OVERBOUGHT = 80
        self.RSI_OVERSOLD = 50

    def setup_broker(self, broker):
        broker.on_price_event = self.on_price_event
        broker.on_order_event = self.on_order_event
        broker.on_position_event = self.on_position_event
        return broker

    def on_price_event(self, df):
        self.df_prices = self.df_prices.append(df)

        if self.price_event_count == 0:
            # remove duplicates in case 1st candle from wss is duplicate with historical
            self.df_prices = self.df_prices \
                .reset_index() \
                .drop_duplicates(subset='datetime',
                                 keep='last') \
                .set_index('datetime')

        print(self.df_prices)
        self.price_event_count += 1
        # self.get_positions()
        self.generate_signals_and_think()

    def get_positions(self):
        try:
            self.broker.get_positions()
        except Exception as ex:
            print('get_positions error:', ex)

    def on_order_event(self, symbol, quantity, is_buy, transaction_id, status):
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
        if symbol == self.symbol:
            self.position = abs(units) * (1 if is_long else -1)
            self.pnl = pnl
            self.upnl = upnl
            self.print_state()

    def print_state(self):
        print(
            dt.datetime.now(), self.symbol, self.position_state,
            abs(self.position), 'upnl:', self.upnl, 'pnl:', self.pnl
        )

    @property
    def position_state(self):
        if self.position == 0:
            return 'FLAT'
        if self.position > 0:
            return 'LONG'
        if self.position < 0:
            return 'SHORT'

    def generate_signals_and_think(self):

        print('thinking...')
        # Strategy goes here generate signals.
        is_signal_buy, is_signal_sell = False, False

        # 1. Check if sufficient samples
        if len(self.df_prices) < 100:
            print(f'insufficient candles...{len(self.df_prices)} candles... waiting for more...')
            return

        # simple rsi strategy
        rsi = ta.RSI(self.df_prices['close'], 14)
        print("all rsis calculated so far")
        print(rsi)
        last_rsi = rsi[-1]

        if last_rsi > self.RSI_OVERBOUGHT:
            if self.position > 0:
                print("Overbought! Sell!")
                # put binance sell logic here
                is_signal_sell = True
            else:
                print("It is overbought, but we don't own any. Nothing to do.")

        if last_rsi < self.RSI_OVERSOLD:
            if self.position > 0:
                print("It is oversold, but you already own it, nothing to do.")
            else:
                print("Oversold! Buy!")
                # put binance buy order logic here
                is_signal_buy = True

        self.think(is_signal_buy, is_signal_sell)

    def think(self, is_signal_buy, is_signal_sell):
        if self.is_order_pending:
            return

        if is_signal_buy:
            order = self.send_market_order(self.symbol, self.units, is_buy=True)
            order_status = order.get('status')
            print(f'order status = {order_status}')
            if order_status == 'FILLED':
                self.position = 1

        if is_signal_sell:
            order = self.send_market_order(self.symbol, self.units, is_buy=False)
            order_status = order.get('status')
            print(f'order status = {order_status}')
            if order_status == 'FILLED':
                self.position = 0

    def send_market_order(self, symbol, quantity, is_buy):
        # price = self.broker.client.get_symbol_ticker(symbol=symbol).get('price')
        # adjusted_quantity = self.broker.get_lot(symbol, price, self.units, base=self.base)
        order = self.broker.send_market_order(symbol, quantity, is_buy)
        return order

    def run(self):
        self.broker.stream_prices(symbols=[self.symbol], interval=self.interval)


if __name__ == '__main__':
    broker = BinanceBroker(is_live=False)
    trader = TraderAll(broker=broker, interval='1m', symbol='ETHUSDT', units=0.05)
    trader.run()
