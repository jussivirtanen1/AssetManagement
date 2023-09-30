import pandas as pd
import numpy as np
import utils.date_utils as du
import utils.db_utils as dbu
import utils.am_utils as amu

config_df = dbu.getConfigurationsData('config/user_config.json')
assets_config_df = dbu.fetchDataFromDB(dbu.getAssets('c') \
                    ,conn = dbu.getDBConnection(env = 'prod' \
                    ,user_file_name='config/user_config.json' \
                    ,user_index=0))
# Fetch full data from asset_management_db table
postgresql_table = dbu.fetchDataFromDB(dbu.getDBQuery_c()
                    ,conn = dbu.getDBConnection(env = 'prod' \
                    ,user_file_name='config/user_config.json' \
                    ,user_index=0))
# Get assets list as yahoo tickers
assets_list = dbu.getAssetsList(assets_config_df)
# Fetch usable dates and non-null yahoo finance data

data = du.getDataFromYahoo(assets_list, start_date = "2022-09-01")

data_cleaned = du.getUsableDataForAssets(data)

usable_dates_list = du.getUsableDatesList(data_cleaned, freq = 'M')

yf_data = du.getUsableDatesForAssets(data_cleaned, usable_dates_list)

# Get asset portfolio by usable dates and asset positions on that date
asset_portfolio = amu.assetPortfolioOverTime(assets_config_df
                                                  , postgresql_table
                                                  , yf_data)
asset_portfolio_proportions = amu.assetProportions(asset_portfolio)

# Calculate portfolio returns times proportions
return_times_proportion_df = asset_portfolio_proportions \
                            .mul(asset_portfolio
                                 .pct_change()
                                 , axis = 'columns')
# Traditional volatility for portfolio
volatitility_portfolio = np.sqrt(return_times_proportion_df.var(axis = 1))
volatitility_portfolio_df = pd.DataFrame(volatitility_portfolio
                                         , columns = ['volatility']
                                         , index = volatitility_portfolio.index)
# Calculate mean absolute deviation for portfolio
mad_portfolio = (return_times_proportion_df
                 .sub(return_times_proportion_df
                 .mean(axis=1), axis=0)) \
                 .abs().mean(axis = 1)
mad_portfolio_df = pd.DataFrame(mad_portfolio
                                , columns = ['mad']
                                , index = mad_portfolio.index)
# Combine two measures
vol_df = pd.concat([volatitility_portfolio_df, mad_portfolio_df], axis = 1).round(2)
