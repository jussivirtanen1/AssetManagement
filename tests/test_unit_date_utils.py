import utils.date_utils as du
                        # getUsableDataForAssets, \
                        # getUsableDatesList, \
                        # getUsableDatesForAssets
import pandas as pd
import numpy as np

# Test dataframes for assets config
test_assets_config_data = {'name': ['Nordea Bank Oyj', 'Sampo Oyj', 'USA Indeksirahasto'
                                    , 'Tanska Indeksirahasto', 'Salesforce'],
                          'yahoo_ticker': ['NDA-FI.HE', 'SAMPO.HE', '0P0001K6NM.F'
                                           , '0P000134KA.CO', 'CRM'],
                          'yahoo_fx_ticker': ['EUREUR=X', 'EUREUR=X', 'EUREUR=X'
                                              , 'EURDKK=X', 'EURUSD=X'],
                          'currency': ['EUR', 'EUR', 'EUR', 'DKK', 'USD'],
                          'instrument': ['Stock', 'Stock', 'Mutual fund'
                                         , 'Mutual fund', 'Stock'],
                          'region': ['Finland', 'Finland', 'North America'
                                     , 'Denmark', 'North America'],
                          'bank': ['Nordea', 'Nordea', 'Nordnet', 'Nordnet', 'Nordea'],
                          'industry': ['Finance', 'Insurance', 'General index'
                                       , 'General index', 'IT and consulting']}
test_assets_config_df = pd.DataFrame(data=test_assets_config_data)

test_yahoo_data = {'0P000134KA.CO': [255.0, np.nan, 257.76, 257.54, np.nan, np.nan],
                    '0P0001K6NM.F': [155.0, 157.64, 159.36, 159.68, np.nan, np.nan],
                    'CRM': [205.50, 210.43, 211.26, 211.65, np.nan, 213.7],
                    'EURDKK=X': [6.0, 7.45, 7.45, 7.45, 7.45, 7.45],
                    'EUREUR=X': [1.0, np.nan, 1.0, 1.0, np.nan, 1.0],
                    'EURUSD=X': [1.02, 1.09, 1.09, 1.09, 1.09, 1.09],
                    'NDA-FI.HE': [9.5, 9.8, 9.97, 10.06, 9.97, 9.92],
                    'SAMPO.HE': [42.1, 40.9, 41.12, 41.13, 41.05, 40.49]}

test_yahoo_df = pd.DataFrame(data=test_yahoo_data
                             , index = ['2023-06-28', '2023-06-29', '2023-06-30'
                                        , '2023-07-03', '2023-07-04', '2023-07-05'])

def test_getDataFromYahoo():
    yahoo_df = du.getDataFromYahoo(assets_list = ['NDA-FI.HE', 'SAMPO.HE'
                                            ,'0P0001K6NM.F'
                                            ,'0P000134KA.CO', 'CRM']
                               , start_date = "2023-09-01")
    
    assert yahoo_df.shape[0] > 0
    assert yahoo_df.shape[1] > 0
    assert yahoo_df.index[0].strftime('%Y-%m-%d') == '2023-09-01'


def test_getUsableDataForAssets():
    data = du.getUsableDataForAssets(test_yahoo_df)
    assert data.shape[0] > 0
    assert len(data) == 3

def test_getUsableDatesList():
    dates_M = du.getUsableDatesList(data = test_yahoo_df, freq = 'M')
    dates_D = du.getUsableDatesList(data = test_yahoo_df, freq = 'D')
    assert len(dates_M) == 2
    assert dates_M[0] == '2023-06-30'
    assert dates_M[1] == '2023-07-03'
    assert len(dates_D) == 3
    assert dates_D[0] == '2023-06-28'
    assert dates_D[1] == '2023-06-30'
    assert dates_D[2] == '2023-07-03'

def test_getUsableDatesForAssets():
    assert test_yahoo_df.isnull().values.any()
    test_data_init = du.getUsableDataForAssets(test_yahoo_df)
    usable_dates_list = du.getUsableDatesList(data = test_data_init, freq = 'M')
    usable_dates_data_df = du.getUsableDatesForAssets(data = test_data_init
                                                , usable_dates_list = usable_dates_list)
    assert not usable_dates_data_df.isnull().values.any()
    assert usable_dates_data_df.shape[0] == 2
    assert usable_dates_data_df.index[0] == '2023-06-30'
    assert usable_dates_data_df.index[1] == '2023-07-03'