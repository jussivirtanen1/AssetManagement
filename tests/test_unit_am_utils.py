import utils.am_utils as amu
import utils.db_utils as dbu
from sqlalchemy import text
import pandas as pd
import numpy as np
import datetime


# get test db connection
test_db_engine = dbu.getDBConnection(env = 'test' \
                        ,user_file_name='tests/mock_user_config.json' \
                        ,user_index=0)
test_conn = test_db_engine.connect()

test_conn.execute(text("TRUNCATE TABLE asset_management_test.asset_ids;"))

test_conn.execute(text("TRUNCATE TABLE asset_management_test.asset_info;"))

test_conn.execute(text("TRUNCATE TABLE asset_management_test.asset_owner;"))

test_conn.execute(text("TRUNCATE TABLE asset_management_test.asset_transactions;"))

test_conn.execute(text("COMMIT;"))

# test_conn.execute(text("ROLLBACK;"))


# asset_ids_table:
test_asset_ids_data = {'name': ['Nordea Bank Oyj', 'Sampo Oyj', 'USA Indeksirahasto'
                                    , 'Tanska Indeksirahasto', 'Salesforce'],
                          'asset_id': [2008, 1012, 1007, 1008, 1013],
                          'yahoo_ticker': ['NDA-FI.HE', 'SAMPO.HE', '0P0001K6NM.F'
                                    , '0P000134KA.CO', 'CRM'],
                          'yahoo_fx_ticker': ['EUREUR=X', 'EUREUR=X'
                                    , 'EUREUR=X', 'EURDKK=X', 'EURUSD=X'],
                          'isin': ['FI4000297767', 'FI0009003305', 'IE00BMTD2W97'
                                    , 'SE0005993078', 'US79466L3024']
}
test_asset_ids_df = pd.DataFrame(data=test_asset_ids_data)

# Insert test asset_ids data to test database
test_asset_ids_df.to_sql(name = 'asset_ids' \
                    ,schema = 'asset_management_test' \
                    ,con=test_db_engine \
                    ,if_exists="append" \
                    ,index=False)

# asset_info_table:
test_asset_info_data ={'name': ['Nordea Bank Oyj', 'Sampo Oyj', 'USA Indeksirahasto'
                                    , 'Tanska Indeksirahasto', 'Salesforce'],
                        'asset_id': [2008, 1012, 1007, 1008, 1013],
                        'currency': ['EUR', 'EUR', 'EUR', 'DKK', 'USD'],
                        'instrument': ['Stock', 'Stock', 'Mutual fund',
                                        'Mutual fund', 'Stock'],
                        'geographical_area': ['Finland', 'Finland', 'North America',
                                                'Denmark', 'North America'],
                        'industry': ['Finance', 'Insurance', 'General index',
                                        'General index', 'IT and consulting']
}
test_asset_info_df = pd.DataFrame(data=test_asset_info_data)

# Insert test asset_info data to test database
test_asset_info_df.to_sql(name = 'asset_info' \
                    ,schema = 'asset_management_test' \
                    ,con=test_db_engine \
                    ,if_exists="append" \
                    ,index=False)

# asset_owner_table:
test_asset_owner_data = {'name': ['Nordea Bank Oyj', 'Sampo Oyj', 'USA Indeksirahasto',
                                    'Tanska Indeksirahasto', 'Salesforce'],
                        'asset_id': [2008, 1012, 1007, 1008, 1013],
                        'owner_id': [10, 10, 10, 10, 10],
                        'bank': ['Nordea', 'Nordea', 'Nordnet', 'Nordnet', 'Nordea'],
                        'account': ['Osakesäästötili', 'Osakesäästötili', 'Arvo-osuustili',
                                        'Arvo-osuustili', 'Osakesäästötili']
}
test_asset_owner_df = pd.DataFrame(data=test_asset_owner_data)

# Insert test asset_info data to test database
test_asset_owner_df.to_sql(name = 'asset_owner' \
                    ,schema = 'asset_management_test' \
                    ,con=test_db_engine \
                    ,if_exists="append" \
                    ,index=False)

# asset_transactions_table:
test_asset_transactions_data = {'event_type': ['Osto', 'Osto', 'Osto'
                                                ,'Osto', 'Merkintä', 'Merkintä'],
                                'asset_id': [1013, 1013, 1012, 2008, 1008, 1007],
                                'owner_id': [10, 10, 10, 10, 10, 10],
                                'name': ['Salesforce', 'Salesforce', 'Sampo Oyj'
                                        , 'Nordea Bank Oyj', 'Tanska Indeksirahasto'
                                        , 'USA Indeksirahasto'],
                                'date': [datetime.date(2023, 9, 29)
                                        ,datetime.date(2023, 4, 14)
                                        ,datetime.date(2023, 5, 31)
                                        ,datetime.date(2023, 6, 30)
                                        ,datetime.date(2023, 6, 22)
                                        ,datetime.date(2023, 7, 3)],
                                'quantity': [2.0, 3.0, 3.0, 1.0, 7.27, 3.77],
                                'price_fx': [158.55, 162.43, 43.4, 9.86, 344.4, 159.24],
                                'price_eur': [150.2, 158.0, 43.4, 9.86, 49.2, 159.24],
                                'amount': [300.4, 474.0, 130.2, 9.86, 357.68, 600.33],
}
test_asset_transactions_df = pd.DataFrame(data=test_asset_transactions_data)

# Insert test asset_transactions data to test database
test_asset_transactions_df.to_sql(name = 'asset_transactions' \
                    ,schema = 'asset_management_test' \
                    ,con=test_db_engine \
                    ,if_exists="append" \
                    ,index=False)

asset_config_query = """SELECT ids.*
                    ,info.currency
                    ,info.instrument
                    ,info.geographical_area
                    ,info.industry
            FROM asset_management_test.asset_ids ids
            JOIN asset_management_test.asset_info info ON ids.asset_id = info.asset_id;
                     """

test_assets_config_df = dbu.fetchDataFromDB(asset_config_query, conn = test_db_engine)

assets_postgresql_query = """SELECT trans.*
                                    ,ids.yahoo_ticker
                                    ,ids.yahoo_fx_ticker
                                    ,ids.isin
                                    ,info.currency
                                    ,info.instrument
                                    ,info.geographical_area
                                    ,info.industry
                            FROM asset_management_test.asset_transactions trans
                                LEFT JOIN asset_management_test.asset_info info ON trans.asset_id = info.asset_id
                                LEFT JOIN asset_management_test.asset_ids ids ON ids.asset_id = trans.asset_id
                                WHERE trans.owner_id = 10
                                ORDER BY trans.date DESC;
                          """

test_postgresql_df = dbu.fetchDataFromDB(assets_postgresql_query, conn = test_db_engine)
test_postgresql_df['date'][0]
# # Test dataframes for assets config and postgresql table
# test_assets_config_data = {'name': ['Nordea Bank Oyj', 'Sampo Oyj', 'USA Indeksirahasto'
#                                     , 'Tanska Indeksirahasto', 'Salesforce'],
#                           'yahoo_ticker': ['NDA-FI.HE', 'SAMPO.HE', '0P0001K6NM.F'
#                                     , '0P000134KA.CO', 'CRM'],
#                           'yahoo_fx_ticker': ['EUREUR=X', 'EUREUR=X'
#                                     , 'EUREUR=X', 'EURDKK=X', 'EURUSD=X'],
#                           'currency': ['EUR', 'EUR', 'EUR'
#                                     , 'DKK', 'USD'],
#                           'instrument': ['Stock', 'Stock', 'Mutual fund'
#                                     , 'Mutual fund', 'Stock'],
#                           'region': ['Finland', 'Finland', 'North America'
#                                      , 'Denmark', 'North America'],
#                           'bank': ['Nordea', 'Nordea', 'Nordnet'
#                                    , 'Nordnet', 'Nordea'],
#                           'industry': ['Finance', 'Insurance', 'General index'
#                                     , 'General index', 'IT and consulting']}
# test_assets_config_df = pd.DataFrame(data=test_assets_config_data)
# test_postgresql_data = {'event_type': ['Osto', 'Osto', 'Osto'
#                                        , 'Osto', 'Merkintä', 'Merkintä'],
#      'name': ['Salesforce', 'Salesforce', 'Sampo Oyj'
#               , 'Nordea Bank Oyj', 'Tanska Indeksirahasto'
#               , 'USA Indeksirahasto'],
#      'isin': [None, None, None, None, None, None],
#      'ticker': [None, None, None, None, None, None],
#      'region': ['USA', 'USA', 'Suomi', 'Suomi', 'Tanska', 'USA'],
#      'exchange': ['XNYS', 'XNYS', 'OMX Helsinki', 'OMX Helsinki', None, None],
#      'date': [datetime.date(2023, 9, 29)
#               , datetime.date(2023, 4, 14)
#               , datetime.date(2023, 5, 31)
#               , datetime.date(2023, 6, 30)
#               , datetime.date(2023, 6, 22)
#               , datetime.date(2023, 7, 3)],
#      'return_type': ['Osake', 'Osake', 'Osake'
#                      , 'Osake', 'Osakerahasto', 'Osakerahasto'],
#      'industry': ['IT', 'IT', 'Vakuutus', 'Rahoitus'
#                   , 'Yleisindeksi', 'Yleisindeksi'],
#      'instrument': ['Osake', 'Osake', 'Osake', 'Osake'
#                     , 'Osakerahasto', 'Osakerahasto'],
#      'quantity': [2.0, 3.0, 3.0, 1.0, 7.27, 3.77],
#      'amount_no_fee': [123.69, 523.78, 128.85, 9.86, 250.0, 600.0],
#      'fee': [8.0, 1.0, 1.29, 0.1, np.nan, np.nan],
#      'currency': ['EUR', 'EUR', 'EUR', 'EUR', 'EUR', 'EUR'],
#      'amount': [131.69, 524.78, 130.14, 9.96, 250.0, 600.0],
#      'bank': ['Nordea', 'Nordea', 'Nordea'
#               , 'Nordea', 'Nordnet', 'Nordnet'],
#      'usage': [None, None, None, None, None, None],
#      'account': ['Osakesäästötili'
#                  , 'Osakesäästötili'
#                  , 'Osakesäästötili'
#                  , 'Osakesäästötili'
#                  , 'Arvo-osuustili'
#                  , 'Arvo-osuustili']}
# test_postgresql_df = pd.DataFrame(data=test_postgresql_data)

am_test_yahoo_data = {'0P000134KA.CO': [257.76, 257.54],
                   '0P0001K6NM.F': [159.36, 159.68],
                   'CRM': [211.26, 211.65],
                   'EURDKK=X': [7.45, 7.45],
                   'EUREUR=X': [1.0, 1.0],
                   'EURUSD=X': [1.09, 1.09],
                   'NDA-FI.HE': [9.97, 10.06],
                   'SAMPO.HE': [41.12, 41.13]}

am_test_yahoo_df = pd.DataFrame(data=am_test_yahoo_data)
am_test_yahoo_df['Date'] = pd.to_datetime(['2023-06-30', '2023-07-03']) 
am_test_yahoo_df = am_test_yahoo_df.set_index('Date')


def test_createIndustryDict():
    # Test that function creates an dictionary from configuration dataframe
    # Test that the dictionary has the same number
    #  of keys as the configuration dataframe
    industry_dict = amu.createIndustryDict(test_assets_config_df)
    assert isinstance(industry_dict, dict)
    assert len(industry_dict.keys()) == len(set(test_assets_config_df['industry']))

def test_createAssetNameDict():
    # Test that function creates an dictionary from configuration dataframe
    # Test that the dictionary has the same number
    #  of keys as the configuration dataframe
    assets_dict = amu.createAssetNameDict(test_assets_config_df)
    assert isinstance(assets_dict, dict)
    assert len(assets_dict.keys()) == len(test_assets_config_df['name'].values)

# createMergedBigDF() is obsolete due to new tables in database!
# def test_createMergedBigDF():
#     # Test that all needed column are in the new table
#     # Test that the new table has the same number of rows as the original table
#     merged_big_df = amu.createMergedBigDF(test_postgresql_df, test_assets_config_df)
#     assert len(merged_big_df) == len(test_postgresql_df)
#     assert set(merged_big_df.columns) == set(['currency' \
#                     , 'yahoo_ticker', 'date', 'geogra' \
#                     , 'quantity', 'instrument', 'yahoo_fx_ticker' \
#                     , 'name', 'bank', 'industry'])

def test_getAssetQuantitiesTillDate():
    asset_quantities_20230530_df = amu.getAssetQuantitiesTillDate(\
                                        test_postgresql_df \
                                        ,datetime.date(2023, 5, 30))
    asset_quantities_20230630_df = amu.getAssetQuantitiesTillDate(\
                                        test_postgresql_df \
                                        ,datetime.date(2023, 6, 30))
    asset_quantities_20230930_df = amu.getAssetQuantitiesTillDate(\
                                        test_postgresql_df \
                                        ,datetime.date(2023, 9, 30))
    

    assert set(asset_quantities_20230530_df.index.values) == set(['Salesforce'])
    assert asset_quantities_20230530_df.loc['Salesforce'] == 3.0
    assert set(asset_quantities_20230630_df.index.values) == set(['Salesforce' \
                                    , 'Sampo Oyj' \
                                    , 'Nordea Bank Oyj' \
                                    , 'Tanska Indeksirahasto'])
    assert asset_quantities_20230930_df.loc['Salesforce'] == 5.0
    assert set(asset_quantities_20230930_df.index.values) == set(['Salesforce' \
                                    , 'Sampo Oyj' \
                                    , 'Nordea Bank Oyj' \
                                    , 'Tanska Indeksirahasto' \
                                    , 'USA Indeksirahasto'])

def test_calculateQuantitiesForEachAsset():
    assets_q_df = amu.calculateQuantitiesForEachAsset(\
                            test_postgresql_df \
                            , [datetime.date(2023, 5, 30), datetime.date(2023, 5, 30), datetime.date(2023, 9, 30)])
    assert set(assets_q_df.index) == set([datetime.date(2023, 5, 30) \
                                        , datetime.date(2023, 6, 30) \
                                        , datetime.date(2023, 9, 30)])
    assert set(assets_q_df.columns) == set(['Salesforce' \
                                            , 'Sampo Oyj' \
                                            , 'Nordea Bank Oyj' \
                                            , 'Tanska Indeksirahasto' \
                                            , 'USA Indeksirahasto'])
    assert assets_q_df.loc['2023-05-30', 'Salesforce'] == 3.0
    assert assets_q_df.loc['2023-09-30', 'Salesforce'] == 5.0
    assert assets_q_df.loc['2023-05-30', 'Sampo Oyj'] == 0.0
    assert assets_q_df.loc['2023-05-30', 'Nordea Bank Oyj'] == 0.0
    assert assets_q_df.loc['2023-06-30', 'Sampo Oyj'] == 3.0
    assert assets_q_df.loc['2023-09-30', 'USA Indeksirahasto'] == 3.77
    assert assets_q_df.loc['2023-06-30', 'Tanska Indeksirahasto'] == 7.27
    assert assets_q_df.loc['2023-05-30', 'Tanska Indeksirahasto'] == 0.00
    
def test_getAssetsList():
    # Test that function returns a list of assets
    assets_list = amu.getAssetsList(test_assets_config_df)
    assert isinstance(assets_list, list)
    assert set(assets_list) == set(['EUREUR=X'
                            , 'EURDKK=X'
                            , 'EURUSD=X'
                            , 'NDA-FI.HE'
                            , 'SAMPO.HE'
                            , '0P0001K6NM.F'
                            , '0P000134KA.CO'
                            , 'CRM'])

def test_assetPortfoliOverTime():
    assetPortfolioOverTime_df = amu.assetPortfolioOverTime(test_assets_config_df
                                , test_postgresql_df
                                , am_test_yahoo_df)
    assert not assetPortfolioOverTime_df.isnull().values.any()
    assert set(assetPortfolioOverTime_df.index) == set([datetime.date(2023, 6, 30), datetime.date(2023, 7, 3)])
    assert set(assetPortfolioOverTime_df.columns) == set(['Nordea Bank Oyj'
                                            , 'Sampo Oyj'
                                            , 'USA Indeksirahasto'
                                            , 'Tanska Indeksirahasto'
                                            , 'Salesforce'])
    assert len(assetPortfolioOverTime_df) == 2
    assert assetPortfolioOverTime_df.loc['2023-07-03', 'Nordea Bank Oyj'] == 10.06
    # TODO: This needs more tests

def test_assetProportions():
    assetPortfolioOverTime_df = amu.assetPortfolioOverTime(test_assets_config_df
                                                            ,test_postgresql_df
                                                            ,am_test_yahoo_df)
    assert amu.assetProportions(assetPortfolioOverTime_df).sum(axis=1).iloc[0] == 100.0
    assert amu.assetProportions(assetPortfolioOverTime_df).sum(axis=1).iloc[1] == 100.0

# def test_calculateProportionOfReturn():
    # assetPortfolioOverTime_df = amu.assetPortfolioOverTime(test_assets_config_df
    #                                                         ,test_postgresql_df
    #                                                         ,am_test_yahoo_df)
    # testReturnDF = amu.calculateProportionOfReturn(assetPortfolioOverTime_df)
    # TODO: mock data has too little cahnges in values to make this test reasonable.
    # NOTE TO SELF: Changing test data will affect other tests. REMEMBER THAT TESTS
    #  SHOULD BE INDEPENDENT OF EACH OTHER.
    # assert testReturnDF.sum(axis=1).iloc[1] == 0.13
    # assert testReturnDF['Nordea Bank Oyj'].iloc[1].round(6) == 0.000149
    # assert testReturnDF['USA Indeksirahasto'].iloc[1].round(6) == 0.998377
    # assert testReturnDF['Salesforce'].iloc[1].round(5) == 0.00178

def test_betaOfPortfolio():
  assetPortfolioOverTime_df = amu.assetPortfolioOverTime(test_assets_config_df
                                                      , test_postgresql_df
                                                      , am_test_yahoo_df)
  beta_proportions_df = amu.assetProportions(assetPortfolioOverTime_df)
  assert amu.betaOfPortfolio(test_assets_config_df
                          ,beta_proportions_df.rename(columns=dict(zip(test_assets_config_df.name
                          ,test_assets_config_df.yahoo_ticker)))
                          ,'^GSPC'
                          ,start_date = '2023-01-01') == 0.8269

  assert amu.betaOfPortfolio(test_assets_config_df
                          ,beta_proportions_df.rename(columns=dict(zip(test_assets_config_df.name
                          ,test_assets_config_df.yahoo_ticker)))
                          ,'^STOXX'
                          ,start_date = '2023-01-01') == 0.5689
