import pandas as pd
import numpy as np
import os
import utils.date_utils as du
import utils.db_utils as dbu
import utils.am_utils as amu
pd.set_option('display.max_columns', None)

def asset_management():
    assets_df = dbu.fetchDataFromDB(dbu.getAssets(filter = 10) \
            ,conn = dbu.getDBConnection(env = 'prod' \
                                        ,user_file_name='config/user_config.json' \
                                        ,user_index=0))
    # Fetch full data from asset_management_db table
    postgresql_table = dbu.fetchDataFromDB(dbu.getDBQuery(filter = 10) \
            , conn = dbu.getDBConnection(env = 'prod' \
                                        ,user_file_name='config/user_config.json' \
                                        ,user_index=0))
    # Get assets list as yahoo tickers
    assets_list = amu.getAssetsList(assets_df)
    # Fetch usable dates and non-null yahoo finance data
    # First transaction is around the beginning of February 2018
    data = du.getDataFromYahoo(assets_list, start_date = "2018-02-01")

    data_cleaned = du.getUsableDataForAssets(data)

    usable_dates_list = du.getUsableDatesList(data_cleaned, freq = 'M')

    yf_data = du.getUsableDatesForAssets(data_cleaned, usable_dates_list)

    # Get asset portfolio by usable dates and asset positions on that date
    asset_portfolio = amu.assetPortfolioOverTime(assets_df
                                                ,postgresql_table
                                                ,yf_data)

    # Calculate asset portfolio proportions of total portfolio return
    asset_contr = amu.calculateProportionOfReturn(asset_portfolio)

    # Calculate asset portfolio proportions
    proportions_df = amu.assetProportions(asset_portfolio)

    # Calculate portfolio returns times proportions
    return_times_proportion_df = proportions_df.mul(asset_portfolio.pct_change()
                                                    .fillna(0)
                                                    .replace([np.inf, -np.inf], 0)
                                                    , axis = 'columns')
    
    vol_df = amu.volatilityStatistics(return_times_proportion_df)

    # Calculate beta of the portfolio against two indices (benchmarks):
    # S&P500 and Stoxx Europe 600
    # S&P500 benchmark ticker in yahoo ^GSPC
    # Stoxx Europe 600 benchmark ticker in yahoo ^STOXX
    betaOnSP500 = amu.betaOfPortfolio(assets_df
                        , proportions_df.rename(columns=dict(zip(assets_df.name
                        , assets_df.yahoo_ticker)))
                        , '^GSPC', start_date = '2020-01-01')
    betaOnStoxxEurope600 = amu.betaOfPortfolio(assets_df
                        , proportions_df.rename(columns=dict(zip(assets_df.name
                        , assets_df.yahoo_ticker)))
                        , '^STOXX', start_date = '2020-01-01')
    beta_df = pd.DataFrame(data = [(betaOnSP500, betaOnStoxxEurope600)]
                        , columns=['Beta on S&P500', 'Beta on Stoxx Europe 600']
                        , index = ['beta'])
    
    # Define different asset types to get proportions for each of them individually
    etf_assets_df = dbu.getConfigByInstrument(assets_df, 'ETF')
    mutual_fund_assets_df = dbu.getConfigByInstrument(assets_df, 'Mutual fund')
    stock_assets_df = dbu.getConfigByInstrument(assets_df, 'Stock')

    # Round values for Excel
    etf_assets_proportions = proportions_df[etf_assets_df['name']].round(2)
    fund_assets_proportions = proportions_df[mutual_fund_assets_df['name']].round(2)
    stock_assets_proportions = proportions_df[stock_assets_df['name']].round(2)
    vol_df = vol_df.round(2)

    with pd.ExcelWriter(os.path.join(os.path.expanduser("~")
                                     ,"Desktop"
                                     ,"Analysis_October.xlsx")) as writer:
        etf_assets_proportions.to_excel(writer, sheet_name='ETF', index=True)
        fund_assets_proportions \
                              .to_excel(writer, sheet_name='Mutual fund', index=True)
        stock_assets_proportions.to_excel(writer, sheet_name='Stock', index=True)
        asset_contr.to_excel(writer, sheet_name='Asset contribution', index=True)
        vol_df.to_excel(writer, sheet_name='Volatility', index=True)
        beta_df.to_excel(writer, sheet_name='Beta', index=True)

if __name__ == "__main__":
    asset_management()