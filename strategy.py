import glob
import os.path
import pandas as pd


class Strategy:
    def __init__(self, target="US", capital="100000", max_positions="20"):
        self.rank_data_location = "./rank_data/"
        self.stock_data_location = "./stock_data/"
        self.long_portfolio = pd.DataFrame()
        self.short_portfolio = pd.DataFrame()
        self.closed_pos = pd.DataFrame()
        self.long_short_df = pd.DataFrame()
        self.is_long_only = True
        self.index_df = pd.DataFrame()
        self.max_positions_long = int(max_positions)
        self.max_positions_short = int(max_positions)
        self.capital = int(capital)
        self.investment_per_stock = self.capital / max_positions

        if target == 'US':
            self.index_file_name = self.stock_data_location + "SPY" + ".csv"
        else:
            self.rank_data_location = "./irank_data/"
            self.stock_data_location = "./istock_data/"
            self.index_file_name = self.stock_data_location + "^NSEI" + ".csv"

    def load_index(self):
        df = pd.read_csv(self.index_file_name)
        self.index_df = pd.DataFrame(columns=['Date', 'signal'])

        # Start with a long signal
        signal = "LONG"
        row_list = []
        for index, row in df.iterrows():
            dict1 = {}

            if row.ema8 > row.ema13 and row.rdx > 50:
                signal = "LONG"
            elif row.ema8 < row.ema13 and row.rdx < 50:
                signal = "SHORT"

            dict1.update({
                'Date': str(row.Date[:10]).replace("-", ""),
                'signal': signal
            })
            row_list.append(dict1)

        self.index_df = pd.DataFrame(row_list)
        self.index_df.to_csv("signal.csv", index=False)

    def load_index1(self):
        df = pd.read_csv(self.index_file_name)
        self.index_df = pd.DataFrame(columns=['Datetime', 'signal'])

        # Start with a long signal
        signal = "LONG"
        row_list = []
        for index, row in df.iterrows():
            if "15:30:00" not in row.Datetime:
                continue

            dict1 = {}
            if row.ema8 > row.ema13 and row.rdx > 50:
                signal = "LONG"
            elif row.ema8 < row.ema13 and row.rdx < 50:
                signal = "SHORT"

            dict1.update({
                'Date': str(row.Datetime[:10]).replace("-", ""),
                'signal': signal
            })
            row_list.append(dict1)

        self.index_df = pd.DataFrame(row_list)
        self.index_df.to_csv("signal.csv", index=False)

    def check_for_stoploss(self, df, d):
        temp = self.long_portfolio
        rows_list = []
        # print("Checking SL for ", d)
        for index, row in self.long_portfolio.iterrows():
            # print(index, " : ", row.Ticker)
            dict1 = {}
            if row.Ticker in df.values:
                df_row = df[df['Ticker'] == row.Ticker]
                if df_row.iloc[0]['cRank'] < 95 \
                        or df_row.iloc[0]['rsi5'] < 55 \
                        or df_row.iloc[0]['Open'] < row.Entry_Price * 0.9:
                    temp = temp[temp['Ticker'] != row.Ticker]
                    dict1.update({
                        'Entry_Date': row.Entry_Date,
                        'Signal': row.Signal,
                        'Ticker': row.Ticker,
                        'Entry_Price': row.Entry_Price,
                        'Qty': row.Qty,
                        'Exit_Date': d,
                        'Exit_Price': df_row.iloc[0]['Open'],
                        'Gain': (df_row.iloc[0]['Open'] / row.Entry_Price - 1) * 100,
                        'Gain_in_Dollars': (df_row.iloc[0]['Open'] - row.Entry_Price) * row.Qty
                    })
                    rows_list.append(dict1)

        # print("SL triggered for ", rows_list.__len__(), " symbols")
        if len(self.closed_pos.index) == 0:
            self.closed_pos = pd.DataFrame(rows_list)
        else:
            self.closed_pos = pd.concat([self.closed_pos, pd.DataFrame(rows_list)])

        self.closed_pos = self.closed_pos.round(decimals=2)
        self.closed_pos.to_csv("closed_positions.csv", index=False)
        self.long_portfolio = temp
        # print(len(self.portfolio.index))

    def check_for_short_stoploss(self, df, d):
        temp = self.short_portfolio
        rows_list = []
        # print("Checking SL for ", d)
        for index, row in self.short_portfolio.iterrows():
            # print(index, " : ", row.Ticker)
            dict1 = {}
            if row.Ticker in df.values:
                df_row = df[df['Ticker'] == row.Ticker]
                if df_row.iloc[0]['cRank'] > 5 \
                        or df_row.iloc[0]['rsi5'] > 35 \
                        or df_row.iloc[0]['Open'] > row.Entry_Price * 1.1:
                    temp = temp[temp['Ticker'] != row.Ticker]
                    dict1.update({
                        'Entry_Date': row.Entry_Date,
                        'Signal': row.Signal,
                        'Ticker': row.Ticker,
                        'Entry_Price': row.Entry_Price,
                        'Qty': row.Qty,
                        'Exit_Date': d,
                        'Exit_Price': df_row.iloc[0]['Open'],
                        'Gain': (row.Entry_Price / df_row.iloc[0]['Open'] - 1) * 100,
                        'Gain_in_Dollars': (row.Entry_Price - df_row.iloc[0]['Open']) * row.Qty
                    })
                    rows_list.append(dict1)

        # print("SL triggered for ", rows_list.__len__(), " symbols")
        if len(self.closed_pos.index) == 0:
            self.closed_pos = pd.DataFrame(rows_list)
        else:
            self.closed_pos = pd.concat([self.closed_pos, pd.DataFrame(rows_list)])

        self.closed_pos = self.closed_pos.round(decimals=2)
        self.closed_pos.to_csv("closed_positions.csv", index=False)
        self.short_portfolio = temp
        # print(len(self.portfolio.index))

    def evaluate(self, start_date=""):
        ranked_files = glob.glob(self.rank_data_location + "*_*.csv")
        ranked_files.sort()
        i = 0
        max_positions_long = self.max_positions_long
        max_positions_short = self.max_positions_short
        is_long = True
        is_short = True

        long_short_dict = {}
        long_short_list = []
        ma_long_short_dict = {}
        ma_long_short_list = []
        for file in ranked_files[i:]:
            # Need just the basename to extract date from filename
            file_name = os.path.basename(file)
            # print(file_name)
            d = file_name[10:18]
            # print(d)
            df = pd.read_csv(file)
            # df = df[df['spike5'] == 0]

            long_df = df[df['rsi5'] > 55]
            # long_df = long_df[long_df['pdi'] > 20]
            long_df = long_df[long_df['cRank'] > 94]
            # long_df = long_df[long_df['rs5'] > long_df['rs13']]
            # long_df = long_df[long_df['rs13Rank'] > long_df['rs34Rank']]

            short_df = df[df['rsi5'] < 35]
            # short_df = short_df[short_df['mdi'] > 20]
            short_df = short_df[short_df['cRank'] < 6]
            # short_df = short_df[short_df['rs5'] < short_df['rs13']]
            # short_df = short_df[short_df['rs13Rank'] < short_df['rs34Rank']]

            # ma_long_df = df[df['bull_signal'] == True]
            # ma_short_df = df[df['bear_signal'] > 0]
            long_df = long_df.sort_values(by=['cRank'], ascending=False)
            short_df = short_df.sort_values(by=['cRank'], ascending=True)
            # print(d, ",", len(long_df), ",", len(short_df))

            '''
            long_short_dict = {
                'Date': d,
                'Long_count': len(long_df),
                'Short_count': len(short_df)
            }
            long_short_list.append(long_short_dict)

            ma_long_short_dict = {
                'Date': d,
                'Long_count': len(ma_long_df),
                'Short_count': len(ma_short_df)
            }
            ma_long_short_list.append(ma_long_short_dict)
            '''

            if d < start_date:
                continue

            # print(long_df)

            if is_long:
                if len(self.long_portfolio.index) > 0:
                    self.check_for_stoploss(df, d)

                remaining_space = max_positions_long - len(self.long_portfolio.index)
                rows_list = []
                # long_df = long_df[long_df['spike14'] == 0]
                if remaining_space > 0:
                    for row in long_df.iterrows():
                        # print(type(row[1]))
                        # print(row[1])

                        if row[1].Ticker in self.long_portfolio.values:
                            continue
                        dict1 = {
                            'Entry_Date': d,
                            'Signal': "Long",
                            'Ticker': row[1].Ticker,
                            'Entry_Price': row[1].Open,
                            'Qty': round(self.investment_per_stock / row[1].Open),
                            'Exit_Date': '',
                            'Exit_Price': '',
                            'Gain': '',
                            'Gain_in_Dollars': ''
                        }
                        rows_list.append(dict1)
                        if rows_list.__len__() > remaining_space - 1:
                            break

                    if len(self.long_portfolio.index) == 0:
                        self.long_portfolio = pd.DataFrame(rows_list)
                    else:
                        self.long_portfolio = pd.concat([self.long_portfolio, pd.DataFrame(rows_list)])
                    self.long_portfolio = self.long_portfolio.round(decimals=2)
                    self.long_portfolio.to_csv("long_open_positions.csv", index=False)
            if is_short:
                if len(self.short_portfolio.index) > 0:
                    self.check_for_short_stoploss(df, d)

                remaining_space = max_positions_short - len(self.short_portfolio.index)
                # print(self.portfolio)
                rows_list = []
                # short_df = short_df[short_df['spike14'] == 0]
                if remaining_space > 0:
                    for row in short_df.iterrows():
                        # print(type(row[1]))
                        # print(row[1])

                        if row[1].Ticker in self.short_portfolio.values:
                            continue
                        dict1 = {
                            'Entry_Date': d,
                            'Signal': "SHORT",
                            'Ticker': row[1].Ticker,
                            'Entry_Price': row[1].Open,
                            'Qty': round(self.investment_per_stock / row[1].Open),
                            'Exit_Date': '',
                            'Exit_Price': '',
                            'Gain': '',
                            'Gain_in_Dollars': ''
                        }
                        rows_list.append(dict1)
                        if rows_list.__len__() > remaining_space - 1:
                            break

                    if len(self.short_portfolio.index) == 0:
                        self.short_portfolio = pd.DataFrame(rows_list)
                    else:
                        self.short_portfolio = pd.concat([self.short_portfolio, pd.DataFrame(rows_list)])
                    # print(self.portfolio)
                self.short_portfolio = self.short_portfolio.round(decimals=2)
                self.short_portfolio.to_csv("short_open_positions.csv", index=False)
