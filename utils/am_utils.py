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

def createMergedBigDF(sql_df: pd.core.frame.DataFrame
                      , config_df: pd.core.frame.DataFrame):
    sql_df['date'] = sql_df['date'].astype(str)
    merged_big_df = pd.merge(sql_df[['date', 'name', 'quantity']]
                             , config_df
                             , left_on = 'name'
                             , right_on = 'name'
                             , how = 'left')
    return merged_big_df

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
                                       , assets_config_df['yahoo_ticker']
                                       .unique()]))
    return assets_list

    # Get only fx columns from yf_data
def assetPortfolioOverTime(assets_config_df, postgresql_table, yf_data):
    fx_list = list(assets_config_df['yahoo_fx_ticker'].unique())
    # Multiply asset columns by fx columns
    df_list = []
    for fx in fx_list:
        df_1 = yf_data[list(assets_config_df[assets_config_df['yahoo_fx_ticker'] == fx]
                            ['yahoo_ticker'])] \
                            .multiply(1/yf_data[fx_list][fx], axis="index")
        # Idea is to create a new dataframe with all assets and their values in USD, 
        # ame is done for each currency separately,
        # resulting in len(currencies) dataframes which can be merged together
        # and portfolio value calculated by summing all dataframes
        # together. In future function could take argument

        df_1 = df_1.rename(columns=dict(
                                        zip(assets_config_df.yahoo_ticker
                                            , assets_config_df.name)))
        # With above plan, for loops in functions are avoided
        #  and functions are easier to separate and test.
        
        merged_big_df = createMergedBigDF(postgresql_table, assets_config_df)
        # Get asset quantities till each each usable date
        #  and multiply them by asset value in that date.

        # Get usable dates from yf_data dataframe
        usable_dates_list = yf_data.index.strftime('%Y-%m-%d').tolist()

        df_2 = calculateQuantitiesForEachAsset(merged_big_df
                                               , usable_dates_list) \
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
    return asset_portfolio

def assetProportions(asset_portfolio):
    asset_portfolio_proportions = 100 * asset_portfolio \
                                        .div(asset_portfolio \
                                        .sum(axis=1), axis=0)
    return asset_portfolio_proportions

def calculateProportionOfReturn(asset_portfolio):
    asset_prop_of_return = asset_portfolio \
                            .diff() \
                            .div(asset_portfolio.sum(axis=1).diff(), axis=0)
    # Below commented line is used to get top 5 assets
    #  with highest proportion of return during latest time period
    # asset_prop_of_return[-1:].apply(pd.Series.nlargest, axis=1, n=5)
    # TODO: This function needs more work and practicality how it should be used
    return asset_prop_of_return

def betaOfPortfolio(assets_config_df
                    , proportions_df
                    , benchmark_ticker
                    , start_date = '2023-01-01'):
    beta_list = []
    print('Using ticker' + benchmark_ticker + ' as benchmark')
    benchmark = yf.download(tickers = benchmark_ticker
                            , start=start_date
                            , end = datetime.datetime
                            .strftime(datetime.datetime
                            .strptime(proportions_df.index.max()
                            , "%Y-%m-%d")
                            + datetime.timedelta(days=1)
                            , "%Y-%m-%d"))
    for ticker in assets_config_df['yahoo_ticker']:
        print('Download daily price data for ' + ticker)
        asset_price = yf.download(tickers = ticker
                                , start=start_date
                                , end = datetime.datetime
                                .strftime(datetime.datetime
                                .strptime(proportions_df.index.max()
                                , "%Y-%m-%d") 
                                + datetime.timedelta(days=1)
                                , "%Y-%m-%d"))
        benchmark_df = pd.DataFrame(benchmark['Close']) \
                            .rename(columns={'Close':'benchmark_price'})
        asset_price_df = pd.DataFrame(asset_price['Close']) \
                            .rename(columns={'Close':'asset_price'})
        # Join two price DataFrame to ensure that both have
        #  the same dates and therefore same date range
        benchmark_filled = asset_price_df \
                            .join(benchmark_df, how='outer') \
                            .fillna(method='ffill')['benchmark_price']
        asset_price_filled = asset_price_df \
                             .join(benchmark_df, how='outer') \
                             .fillna(method='ffill')['asset_price']
        # Compute (daily) returns
        benchmark_return = benchmark_filled.pct_change()
        asset_price_return = asset_price_filled.pct_change()
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
                         (proportions_df.tail(1)/100)[ticker][0])
    # Return portfolio weighted beta
    return sum(beta_list).round(2)
