import pandas as pd
import utils.date_utils as du
import utils.db_utils as dbu
import utils.am_utils as amu
import matplotlib.pyplot as plt
pd.set_option('display.max_columns', None)

def asset_management():
    assets_df = dbu.fetchDataFromDB(dbu.getAssets(filter = 10)
                        ,conn = dbu.getDBConnection(env = 'prod'
                        ,user_file_name='config/user_config.json'
                        ,user_index=0))
    # Fetch full data from asset_management_db table
    postgresql_table = dbu.fetchDataFromDB(dbu.getDBQuery(filter = 10) \
                        ,conn = dbu.getDBConnection(env = 'prod' \
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
    asset_portfolio = amu.assetPortfolioOverTime(assets_df \
                                               , postgresql_table \
                                               , yf_data)
    
    # if asset has been nad its proportion in portfolio is zero then exclude from it portfolio.
    assets_df = assets_df[assets_df['name'].isin(asset_portfolio.columns)]
    
    # Optionally, calculate asset portfolio proportions
    asset_portfolio_proportions = amu.assetProportions(asset_portfolio)

    # Define different configurations to get porportions for each of them individually
    etf_assets_df = dbu.getConfigByInstrument(assets_df \
                                        , 'ETF')
    mutual_fund_assets_df = dbu.getConfigByInstrument(assets_df \
                                        , 'Mutual fund')
    stock_assets_df = dbu.getConfigByInstrument(assets_df \
                                        , 'Stock')

    # Plot industry proportion development by each configuration
    plt.plot(asset_portfolio_proportions[etf_assets_df['name']])
    plt.legend(asset_portfolio_proportions \
            .columns \
            .intersection(list(etf_assets_df['name'])) \
            , loc="upper left")
    plt.xticks(rotation=45)
    plt.grid()
    plt.show()

    plt.plot(asset_portfolio_proportions[mutual_fund_assets_df['name']])
    plt.legend(asset_portfolio_proportions \
            .columns \
            .intersection(list(mutual_fund_assets_df['name'])) \
            , loc="upper left")
    plt.xticks(rotation=45)
    plt.grid()
    plt.show()

    plt.plot(asset_portfolio_proportions[stock_assets_df['name']])
    plt.legend(asset_portfolio_proportions \
            .columns \
            .intersection(list(stock_assets_df['name'])) \
            , loc="upper left")
    plt.xticks(rotation=45)
    plt.grid()
    plt.show()

if __name__ == "__main__":
    asset_management()