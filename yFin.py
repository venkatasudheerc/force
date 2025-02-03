from datetime import datetime, timedelta
import logging

import pandas as pd
import yfinance as yf
from ta.momentum import RSIIndicator
from ta.trend import EMAIndicator
from ta.trend import ADXIndicator
from ta.trend import MACD
from ta.volume import VolumeWeightedAveragePrice
from ta.volatility import AverageTrueRange


class YFinance:
    def __init__(self, ticker="^NSEI", period="1d", interval="1d", data_location=None, country="India"):
        self.data = pd.DataFrame()
        self.indexData = pd.DataFrame()
        self.ticker = ticker
        self.period = period
        self.interval = "1d"
        self.country = country
        self.file_name = data_location + ticker.upper() + ".csv"
        self.magic = 0

        '''
        if ticker[0] == '^':
            self.file_name = data_location + ticker[1:].upper() + ".csv"
        else:
            self.file_name = data_location + ticker.upper() + ".csv"
        '''

    def fetch_data(self):
        now = datetime.now() + timedelta(days=1)
        end_date = now.strftime("%Y-%m-%d")
        # print(end_date)
        if self.interval == "1d":
            if self.ticker == "SPY" or self.ticker == "^NSEI":
                if self.magic:
                    self.data = yf.download(tickers=self.ticker, period="60d", interval="90m")
                else:
                    self.data = yf.download(tickers=self.ticker, period=self.period, interval=self.interval,
                                            start="2023-09-01", end=end_date)
            else:
                self.data = yf.download(tickers=self.ticker, period=self.period, interval=self.interval,
                                        start="2023-09-01", end=end_date)
        else:
            # self.data = yf.download(tickers=self.ticker, period="60d", interval="90m")
            self.data = yf.download(tickers=self.ticker, period=self.period, interval=self.interval, start="2023-01-01",
                                    end=end_date)
        return self.data

    def relative_strength(self, window=14):
        if self.ticker != "SPY" or self.ticker == "^NSEI":
            self.indexData = pd.read_csv("./stock_data/SPY.csv")

        # pandas series begin with 0 to length -1.
        rs = (self.data['Close'] / self.data['Close'].shift(window - 1)) / \
             (self.indexData['Close'] / self.indexData['Close'].shift(window - 1)) - 1
        return rs

    def load_data(self):
        logging.info("Data Fetch Started")
        self.data = self.fetch_data()
        self.data = self.data.round(decimals=2)
        self.data.to_csv(self.file_name)
        # print(self.data.head())
        logging.info("Data Fetch Completed")

        try:
            # Data enrichment
            self.data = pd.read_csv(self.file_name)

            # self.data['55_Close'] = self.data['Close'].shift(54)

            if self.ticker != "SPY" or self.ticker == "^NSEI":
                self.data['rs5'] = self.relative_strength(window=5)
                self.data['rs13'] = self.relative_strength(window=13)
                self.data['rs34'] = self.relative_strength(window=34)
                self.data['rs55'] = self.relative_strength(window=55)
                self.data['rs252'] = self.relative_strength(window=252)
                self.data['cumRS'] = self.data['rs5'] * 0.2 \
                                     + self.data['rs13'] * 0.3 \
                                     + self.data['rs34'] * 0.3 \
                                     + self.data['rs55'] * 0.2 \
                                     + self.data['rs252'] * 0.0
            indicator_rsi = RSIIndicator(self.data['Close'], window=14)
            self.data['rsi'] = indicator_rsi.rsi()
            indicator_rsi = RSIIndicator(self.data['Close'], window=5)
            self.data['rsi5'] = indicator_rsi.rsi()
            self.data['rsi5_sma'] = self.data['rsi5'].rolling(8).mean()
            indicator_adx = ADXIndicator(self.data['High'], self.data['Low'], self.data['Close'], window=5)
            self.data['pdi'] = indicator_adx.adx_pos()
            self.data['mdi'] = indicator_adx.adx_neg()
            self.data['adx_diff'] = abs(abs((indicator_adx.adx_pos() - indicator_adx.adx_neg())).diff())
            self.data['spike_exists'] = self.data['adx_diff'].gt(20)
            self.data['spike5'] = self.data['spike_exists'].rolling(5).mean().gt(0)

            '''
            atr = AverageTrueRange(self.data['High'], self.data['Low'], self.data['Close'], window=5)
            self.data['atr_slow'] = atr.average_true_range()
            atr = AverageTrueRange(self.data['High'], self.data['Low'], self.data['Close'], window=21)
            self.data['atr_fast'] = atr.average_true_range()
            # Add RSI
            indicator_rsi = RSIIndicator(self.data['Close'], window=14)
            indicator_rsi5 = RSIIndicator(self.data['Close'], window=5)
            # Add ADX/DMI
            indicator_adx = ADXIndicator(self.data['High'], self.data['Low'], self.data['Close'], window=14)
            indicator_adx5 = ADXIndicator(self.data['High'], self.data['Low'], self.data['Close'], window=5)
            
            # Add SMA 20
            # indicator_sma = SMAIndicator(self.data['Close'], window=20)
            # indicator_sma2 = SMAIndicator(self.data['Close'], window=13)
            # Add EMA 20
            indicator_ema = EMAIndicator(self.data['Close'], window=21)
            indicator_ema2 = EMAIndicator(self.data['Close'], window=8)
            indicator_ema3 = EMAIndicator(self.data['Close'], window=13)
            # add MACD
            indicator_macd = MACD(self.data['Close'])
            vwap = VolumeWeightedAveragePrice(self.data['High'], self.data['Low'],
                                              self.data['Close'], self.data['Volume'],
                                              window=14)

            self.data['vwap'] = vwap.volume_weighted_average_price()
            # Calculate RDX
            self.data['rdx'] = indicator_rsi.rsi() + indicator_adx.adx_pos() - indicator_adx.adx_neg()
            self.data['rdx5'] = indicator_rsi5.rsi() + indicator_adx5.adx_pos() - indicator_adx5.adx_neg()
            self.data['pdi'] = indicator_adx.adx_pos()
            self.data['mdi'] = indicator_adx.adx_neg()
            self.data['adx'] = indicator_adx.adx()
            self.data['rsi'] = indicator_rsi.rsi()
            self.data['rsi5'] = indicator_rsi5.rsi()
            self.data['macdv'] = indicator_macd.macd_diff() * 100 / atr.average_true_range()
            # self.data['sma20'] = indicator_sma.sma_indicator()
            # self.data['sma13'] = indicator_sma2.sma_indicator()
            self.data['ema21'] = indicator_ema.ema_indicator()
            self.data['ema8'] = indicator_ema2.ema_indicator()
            self.data['ema13'] = indicator_ema3.ema_indicator()
            self.data['macd_diff'] = indicator_macd.macd_diff()
            self.data['adx_diff'] = abs(abs((indicator_adx.adx_pos() - indicator_adx.adx_neg())).diff())
            self.data['spike_exists'] = self.data['adx_diff'].gt(20)
            self.data['spike14'] = self.data['spike_exists'].rolling(14).mean().gt(0)
            self.data['bullish'] = self.data.apply(lambda x: x.ema8 > x.ema13 and x.rdx > 50, axis=1)
            self.data['bearish'] = self.data.apply(lambda x: x.ema8 < x.ema13 and x.rdx < 50, axis=1)
            self.data['bull_signal'] = self.data['bullish'] & self.data['bullish'].rolling(2).sum().eq(1)
            self.data['bear_signal'] = self.data['bearish'] & self.data['bearish'].rolling(2).sum().eq(1)
            
            if self.interval != "1d" or (self.magic and (self.ticker == "SPY" or self.ticker == "^NSEI")):
                self.data['date1'] = pd.to_datetime(self.data['Datetime']).dt.date
                self.data = self.data.drop_duplicates(subset='date1', keep="last")
                self.data = self.data.rename(columns={"Datetime": "Date"})
            '''

            logging.info("Custom data added")

            self.data = self.data.round(decimals=4)
            self.data.to_csv(self.file_name, index=False)
            logging.info("data written to data.csv file")

        except Exception as ex:
            print("Exception occurred.", ex)

        return self.data
