import pandas as pd
import numpy as np
import yfinance as yf
import datetime

def createIndustryDict(config_df: pd.core.frame.DataFrame):
    x = list(config_df['industry'].values)
    industry_dict = {}
    for i in x:
        industry_dict[i] = config_df[config_df['industry'] == i]['yahoo_ticker'].values
    return industry_dict

def createAssetNameDict(config_df: pd.core.frame.DataFrame):
    x = list(config_df['name'].values)
    asset_name_dict = {}
    for i in x:
        asset_name_dict[i] = config_df[config_df['name'] == i]['yahoo_ticker'].values
    return asset_name_dict

# Obsolete function due to new tables in database!

# def createMergedBigDF(sql_df: pd.core.frame.DataFrame
#                       , config_df: pd.core.frame.DataFrame):
#     sql_df['date'] = sql_df['date'].astype(str)
#     merged_big_df = pd.merge(sql_df
#                              ,config_df
#                              ,left_on = 'asset_id'
#                              ,right_on = 'asset_id'
#                              ,how = 'left')
#     return merged_big_df

def getAssetQuantitiesTillDate(merged_big_df, date):
    return merged_big_df[merged_big_df['date'] <= date] \
                        .groupby(['name'])['quantity'].sum()

def calculateQuantitiesForEachAsset(merged_big_df, usable_dates_list):
    df_list = []
    for date in usable_dates_list:
        df_list.append(getAssetQuantitiesTillDate(merged_big_df, date)
                       .to_frame()
                       .rename(columns = {'quantity' : date})
                       .transpose())
    return pd.concat(df_list).replace(np.nan, 0)

def getAssetsList(assets_config_df):
    assets_list = list(np.concatenate([assets_config_df['yahoo_fx_ticker']
                                       .unique()
                                       ,assets_config_df['yahoo_ticker']
                                       .unique()]))
    return assets_list

    # Get only fx columns from yf_data
def assetPortfolioOverTime(assets_config_df, postgresql_table, yf_data, usable_dates_list):
    fx_list = list(assets_config_df['yahoo_fx_ticker'].unique())
    # Multiply asset columns by fx columns
    df_list = []
    for fx in fx_list:
        df_1 = yf_data[list(assets_config_df[assets_config_df['yahoo_fx_ticker'] == fx]
                            ['yahoo_ticker'])] \
                            .multiply(1/yf_data[fx_list][fx], axis="index")
        # Idea is to create a new dataframe with all assets and their values in USD, 
        # same is done for each currency separately,
        # resulting in len(currencies) dataframes which can be merged together
        # and portfolio value calculated by summing all dataframes
        # together. In future function could take argument

        df_1 = df_1.rename(columns=dict(
                                        zip(assets_config_df.yahoo_ticker
                                            ,assets_config_df.name)))
        # With above plan, for loops in functions are avoided
        #  and functions are easier to separate and test.
        
        merged_big_df = postgresql_table
                            # pd.merge(postgresql_table \
                            #  ,assets_config_df \
                            #  ,left_on = 'asset_id' \
                            #  ,right_on = 'asset_id' \
                            #  ,how = 'left')
        # Get asset quantities till each each usable date
        #  and multiply them by asset value in that date.

        # Get usable dates from yf_data dataframe
        # usable_dates_list = yf_data.index.tolist()



        df_2 = calculateQuantitiesForEachAsset(merged_big_df
                                               ,usable_dates_list) \
                                               [df_1.columns]
        df_1.index = list(df_2.index)

        # df_3 has asset as column and usable_dates_list as index.
        # Each value is the value of investment at that time point.
        # df_3 columns are filtered by assets which are noted in
        # some currency defined in row 35, hardcoded for now.
        df_3 = pd.DataFrame() 
        for colname in df_1.columns:
            df_3[colname] = df_1[colname].multiply(df_2[colname])
        df_list.append(df_3)
    asset_portfolio = pd.concat(df_list, axis=1, ignore_index=False)
    asset_portfolio = asset_portfolio.loc[:, (asset_portfolio.iloc[-1] != 0)]
    return asset_portfolio

def assetProportions(asset_portfolio):
    asset_portfolio_proportions = 100 * asset_portfolio \
                                        .div(asset_portfolio \
                                        .sum(axis=1), axis=0)
    return asset_portfolio_proportions

def calculateProportionOfReturn(asset_portfolio):
    df = asset_portfolio
    weights = df.div(df.sum(axis=1), axis=0)
    df_return = df.pct_change().fillna(0).replace([np.inf, -np.inf], 0)

    df_contr = df_return.mul(weights, axis=1)
    df_contr['Portfolio'] = df_contr.sum(axis=1)
    df_contr = df_contr * 100
    return df_contr.round(2)

def betaOfPortfolio(assets_config_df
                    , proportions_df
                    , benchmark_ticker
                    , start_date = '2023-01-01'):
    beta_list = []
    print('Using ticker' + benchmark_ticker + ' as benchmark')
    benchmark = yf.download(tickers = benchmark_ticker
                        ,start=start_date
                        ,end = datetime.datetime.strftime((proportions_df.index.max()
                        + datetime.timedelta(days=1)), "%Y-%m-%d"))
    for ticker in assets_config_df['yahoo_ticker']:
        print('Download daily price data for ' + ticker)
        asset_price = yf.download(tickers = ticker
                        ,start=start_date
                        ,end = datetime.datetime.strftime((proportions_df.index.max()
                        + datetime.timedelta(days=1)), "%Y-%m-%d"))
        benchmark_df = pd.DataFrame(benchmark['Close']) \
                            .rename(columns={'Close':'benchmark_price'})
        asset_price_df = pd.DataFrame(asset_price['Close']) \
                            .rename(columns={'Close':'asset_price'})
        # Join two price DataFrame to ensure that both have
        #  the same dates and therefore same date range
        benchmark_filled = asset_price_df \
                            .join(benchmark_df, how='outer') \
                            .ffill()['benchmark_price']
        asset_price_filled = asset_price_df \
                             .join(benchmark_df, how='outer') \
                             .ffill()['asset_price']
        # Compute (daily) returns
        benchmark_return = benchmark_filled.ffill().pct_change()
        asset_price_return = asset_price_filled.ffill().pct_change()
        # Old way of calculating covariance manually
        # Calculate covariane of asset and benchmark
        covar = asset_price_return.cov(benchmark_return)
        # Old way of calculating variance manually
        # Calculate variance of benchmark
        benchmark_var = benchmark_return.var()
        # Append to beta list created earlier.
        # Divide by 100 to get percentage as decimal
        # and then multiply by proportion of asset in portfolio
        beta_list.append((covar/benchmark_var) *
                        (proportions_df.tail(1)/100)
                        [ticker].iloc[0])
    # Return portfolio weighted beta
    beta_list = np.nan_to_num(beta_list, nan=0)
    beta = sum(beta_list).round(4)
    return beta


def volatilityStatistics(df):
    # Calculate volatility for each asset
    volatitility = np.sqrt(df.var(axis = 1))
    volatitility_df = pd.DataFrame(volatitility
                                    , columns = ['volatility']
                                    , index = volatitility.index)
    # Calculate mean absolute deviation for each asset
    mad = (df.sub(df.mean(axis=0), axis=1)).abs().mean(axis = 1)
    mad_df = pd.DataFrame(mad
                            , columns = ['mad']
                            , index = mad.index)
    # Combine two measures
    vol_df = pd.concat([volatitility_df, mad_df], axis = 1)
    return vol_df