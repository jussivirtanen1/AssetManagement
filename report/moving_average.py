import pandas as pd
import numpy as np
import yfinance as yf
import os
import datetime
import matplotlib.pyplot as plt
import utils.db_utils as dbu
pd.set_option('display.max_rows', None)

def ma_plots_all_stock():
    # config_df = dbu.getConfigurationsData('config/user_config.json')
    stock_config_df = dbu.fetchDataFromDB(dbu.getAssets(filter = 20) \
                        ,conn = dbu.getDBConnection(env = 'prod' \
                        ,user_file_name='config/user_config.json' \
                        ,user_index=0))
    df_list = list()

    for ticker in stock_config_df['yahoo_ticker']:
        print(ticker)
        asset_mav = yf.download(tickers= ticker, start='2023-01-01')
        asset_mav['SMA_200'] = asset_mav['Close'] \
                                .rolling(window=200, min_periods=1) \
                                .mean()
        asset_mav['SMA_50'] = asset_mav['Close'] \
                                .rolling(window=50, min_periods=1) \
                                .mean()
        asset_mav['EMA_200'] = asset_mav['Close'].ewm(span=200).mean()
        asset_mav['EMA_50'] = asset_mav['Close'].ewm(span=50).mean()
        asset_mav = asset_mav[['Close', 'SMA_200', 'SMA_50', 'EMA_200', 'EMA_50']] \
                        .assign(SMA_Signal=lambda x: x.SMA_50 > x.SMA_200) \
                        .assign(EMA_Signal=lambda x: x.EMA_50 > x.EMA_200)
        asset_mav['SMA_Signal'] = asset_mav['SMA_Signal'] \
                            .replace({True: 1, False: 0}) \
                            .diff() \
                            .replace({1.0: 'Buy', -1.0: 'Sell', 0.0: '', np.nan: ''})
        asset_mav['EMA_Signal'] = asset_mav['EMA_Signal'] \
                            .replace({True: 1, False: 0}) \
                            .diff() \
                            .replace({1.0: 'Buy', -1.0: 'Sell', 0.0: '', np.nan: ''})
        df_list.append(asset_mav)
        df_list[0]
        # f, ax_1 = plt.subplots()
        plt.figure(figsize=(10,7.5))
        # plt.plot(asset_mav[['Close', 'SMA_200', 'SMA_50', 'EMA_200', 'EMA_50']])
        plt.plot(asset_mav[['Close']], color = 'steelblue')
        plt.plot(asset_mav[['SMA_200']], color = 'red')
        plt.plot(asset_mav[['SMA_50']], color = 'orange')
        plt.plot(asset_mav[['EMA_200']], color = 'purple')
        plt.plot(asset_mav[['EMA_50']], color = 'blue')
        plt.legend(asset_mav[['Close', 'SMA_200', 'SMA_50', 'EMA_200', 'EMA_50']]
                   .columns
                   , loc='best')
        title_stock_name = stock_config_df[stock_config_df \
                            ['yahoo_ticker'] == ticker] \
                            ['name'].values[0]
        plt.title(f"Moving averages of 50 and 200 days for {title_stock_name}")
        plt.xticks(rotation=45)
        stock_name = stock_config_df[stock_config_df['yahoo_ticker'] == ticker] \
                                    ['name'].values[0]
        plt.savefig(os.path.join(os.path.expanduser("~")
        , "Desktop/Plots/"
        , f"{stock_name}_{datetime.datetime.today().strftime('%Y-%m-%d')}.pdf"))
        # plt.show()

if __name__ == "__main__":
    ma_plots_all_stock()