import logging
import pandas as pd
import yFin
import os
import shutil


class RankData:
    def __init__(self, target="US", symbol_count="200", interval="1d"):
        self.symbols = None
        self.data_interval = interval
        self.data_location = "./stock_data/"
        self.rank_location = "./rank_data/"
        self.indices = pd.Series(['SPY', '^NSEI'])
        self.symbol_count = symbol_count
        if target == "US":
            self.target_symbols = "US_Symbols.csv"
            # self.target_symbols = "USETFs.csv"
            self.country = "United States"
        else:
            self.target_symbols = "IND_Symbols.csv"
            self.data_location = "./istock_data/"
            self.rank_location = "./irank_data/"
            self.country = "India"

    def load_data(self):
        self.clean_dir()
        indices = pd.Series(['SPY', '^NSEI'])

        # print(type(indices))
        # df = pd.read_csv(self.target_symbols, nrows=self.symbol_count+1)
        df = pd.read_csv(self.target_symbols)
        self.symbols = df['SYMBOL']
        # print(len(self.symbols))
        # print((self.symbols.append(indices, ignore_index=True)))
        i = 0
        try:
            for stock in self.indices._append(self.symbols, ignore_index=True)[i:]:
                print(stock)
                yf = yFin.YFinance(ticker=stock, interval=self.data_interval, data_location=self.data_location,
                                   country=self.country)
                # print(yf.tail(1))
                df = yf.load_data()
                # print(df.tail(1))
        except ValueError as value:
            print("value error: ", value)
        except ArithmeticError as ex:
            print("Exception : ArithmeticError occurred.")

    def rank_data(self):
        df = pd.read_csv(self.target_symbols)
        self.symbols = df['SYMBOL']
        try:
            start = 1
            end = 1001

            while start < end:
                rows_list = []
                i = 0
                for stock in self.symbols[i:]:
                    # print("started with : ", stock)
                    if stock in self.indices.values:
                        continue
                    df = pd.read_csv(self.data_location + stock + ".csv")
                    # print(df)
                    if end == 1001:
                        print("end calculated based on stock: ", stock)
                        end = len(df)
                    d = df.iloc[start].Date[:10]
                    # print(d)
                    d1 = str(d).replace("-", "")

                    dict1 = {}
                    dict1.update(df.iloc[start])
                    rows_list.append(dict1)
                    # print("done with: ", stock)
                    # print(rows_list)

                df = pd.DataFrame(rows_list,
                                  columns=['Open', 'Close', 'rsi', 'rsi_sma', 'rsi5', 'rsi5_sma', 'rs5', 'rs13', 'rs34',
                                           'rs55', 'cumRS', 'pdi', 'mdi', 'spike5', 'spike14'])
                # print(df.tail(1))
                df['Ticker'] = self.symbols
                df = df[['Ticker', 'Open', 'Close', 'rsi', 'rsi_sma', 'rsi5', 'rsi5_sma', 'rs5', 'rs13', 'rs34', 'rs55',
                         'cumRS', 'pdi', 'mdi', 'spike5', 'spike14']]
                df['cRS'] = df['rs5'] * 0.4 + df['rs13'] * 0.4 + df['rs34'] * 0.2
                df['cRank'] = df['cRS'].rank(pct=True) * 100

                '''
                Commenting below smoothing logic
                
                df['cRS_smoothed'] = df['rs5'].rolling(5).mean() * 0.4 + \
                                     df['rs13'].rolling(13).mean() * 0.4 + \
                                     df['rs34'].rolling(34).mean() * 0.2
                df['cRank_smoothed'] = df['cRS_smoothed'].rank(pct=True) * 100
                '''

                # df['cRS1'] = df['rs5'] * 0.4 + df['rs13'] * 0.3 + df['rs34'] * 0.3
                # df['cRank1'] = df['cRS1'].rank(pct=True) * 100
                # df['rs13Rank'] = df['rs13'].rank(pct=True) * 100
                # df['rs34Rank'] = df['rs34'].rank(pct=True) * 100
                df['cumRank'] = df['cumRS'].rank(pct=True) * 100
                # df['cumRS1'] = df['rs5'] * 0.2 + df['rs13'] * 0.2 + df['rs34'] * 0.2 + \
                #                  df['rs55'] * 0.2 + df['rs252'] * 0.2
                # df['cumRank1'] = df['cumRS1'].rank(pct=True) * 100
                # df['cumRS2'] = df['rs13'] * 0.2 + df['rs34'] * 0.2 + df['rs55'] * 0.3 + df['rs252'] * 0.3
                # df['cumRank2'] = df['cumRS2'].rank(pct=True) * 100
                # df['cumRS3'] = df['rs5'] * 0.2 + df['rs13'] * 0.2 + df['rs55'] * 0.3 + df['rs252'] * 0.3
                # df['cumRank3'] = df['cumRS3'].rank(pct=True) * 100

                df = df.sort_values(by=['cRank'], ascending=False)
                df = df.round(decimals=4)
                df.to_csv(self.rank_location + "rank_data_" + d1 + ".csv", index=False)
                print("completed rank: ", start)
                start = start + 1
            # print(df)

        except ValueError as value:
            logging.error("value error.", value)
            raise
        except Exception as ex:
            print(stock)
            logging.info("Exception occurred.", ex)
            raise

    def clean_dir(self):
        folders = [self.data_location, self.rank_location]
        for folder in folders:
            for filename in os.listdir(folder):
                file_path = os.path.join(folder, filename)
                try:
                    if os.path.isfile(file_path) or os.path.islink(file_path):
                        os.unlink(file_path)
                    elif os.path.isdir(file_path):
                        shutil.rmtree(file_path)
                except Exception as e:
                    print('Failed to delete %s. Reason: %s' % (file_path, e))
