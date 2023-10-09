import pandas as pd
import yfinance as yf

def getDataFromYahoo(assets_list: list, start_date = "2020-01-01"):
    # Download data for all stocks
    # Get closing data for assets and FX
    # Get column levels from yahoo downloaded data
    # set(data.columns.get_level_values(0))
    data = yf.download(assets_list, start = start_date)['Close']
    return data

def getUsableDataForAssets(data):
    # Set euro to euro exchange rate to 1.0
    data_2 = data.copy()
    data_2['EUREUR=X'] = 1.0
    # data.loc[:, 'EUREUR=X'] = 1.0
    # Forward fill and backfill any NaN values. Some assets have no data for some dates.
    data_2 = data_2.ffill().bfill()
    # Remove any rows with NaN values
    data_2 = data_2[~data_2.isnull().any(axis=1)]
    return data_2

def getUsableDatesList(data, freq: str):
    data = getUsableDataForAssets(data)
    # Functionality to find usable dates for the data
    datetime_df = pd.DataFrame(index=pd.to_datetime(data.index).strftime('%Y-%m-%d'))
    datetime_df.index = pd.to_datetime(datetime_df.index)
    # set the dataframe index with your date
    # group by month / alternatively use MS for Month Start
    #  / referencing the previously created object
    datag = datetime_df.groupby(pd.Grouper(freq=freq))
    datetime_df['datetime_col'] = datetime_df.index
    if (freq == 'D'):
        usable_dates_list = []
        for i in range(len(datetime_df)):
            usable_dates_list.append(datetime_df['datetime_col'].tolist()[i].strftime('%Y-%m-%d'))
    else:
        # Finally, find the max date in each month
        datetime_df = datag['datetime_col'].max()
        # To specifically coerce the results of the groupby to a list:
        usable_dates_list = []
        for i in range(len(datetime_df)):
            usable_dates_list.append(datag.agg({'datetime_col': 'max'})
                                     ['datetime_col'].tolist()[i].strftime('%Y-%m-%d'))
    return usable_dates_list

def getUsableDatesForAssets(data, usable_dates_list: list):
    return data[data.index.isin(usable_dates_list)]