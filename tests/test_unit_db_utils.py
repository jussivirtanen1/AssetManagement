import utils.db_utils as dbu
import pandas as pd
from sqlalchemy import text

# get test db connection
test_db_engine = dbu.getDBConnection(env = 'test' \
                        ,user_file_name='tests/mock_user_config.json' \
                        ,user_index=0)
test_conn = test_db_engine.connect()

test_conn.execute(text("TRUNCATE TABLE asset_management_test.asset_ids;"))

test_conn.execute(text("TRUNCATE TABLE asset_management_test.asset_info;"))

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

asset_config_query = """SELECT ids.*
                    ,info.currency
                    ,info.instrument
                    ,info.geographical_area
                    ,info.industry
            FROM asset_management_test.asset_ids ids
            JOIN asset_management_test.asset_info info ON ids.asset_id = info.asset_id;"""

test_assets_config_df = dbu.fetchDataFromDB(asset_config_query, conn = test_db_engine)


# # Test dataframes for assets config
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

def test_getConfigByInstrument():
# Test with valid instrument
    mutual_fund_config_df = dbu.getConfigByInstrument(test_assets_config_df \
                                                    ,'Mutual fund')
    stock_config_df = dbu.getConfigByInstrument(test_assets_config_df \
                                                ,'Stock')
    assert len(mutual_fund_config_df) == 2
    assert len(stock_config_df) == 3

def test_getConfigurationsData():
    user_config_df = dbu.getConfigurationsData('tests/mock_user_config.json')
    assert len(user_config_df) == 2
    assert user_config_df['username'][0] == 'testuser'
    assert user_config_df['host_name'][0] == 'localhost'
    assert user_config_df['database'][0] == 'am_db_test'
    assert user_config_df['port'][0] == '5432'
    assert user_config_df['engine'][0] == 'postgresql+psycopg2://'

def test_getDBConnection():
    db_conn_0 = dbu.getDBConnection(env = 'test' \
                                    ,user_file_name='tests/mock_user_config.json' \
                                    ,user_index=0)
    assert db_conn_0.url.username == 'testuser'
    assert db_conn_0.url.host == 'localhost'
    assert db_conn_0.url.database == 'am_db_test'
    assert db_conn_0.url.port == 5432
    assert db_conn_0.url.drivername == 'postgresql+psycopg2'

    db_conn_1 = dbu.getDBConnection(env = 'test' \
                                    ,user_file_name='tests/mock_user_config.json' \
                                    ,user_index=1)
    assert db_conn_1.url.username == 'produser'
    assert db_conn_1.url.host == 'example.com'
    assert db_conn_1.url.database == 'proddb'
    assert db_conn_1.url.port == 5432
    assert db_conn_1.url.drivername == 'postgresql+psycopg2'

# Modify below query to match current schema
# def test_DBQuery():
#     db_query = dbu.getDBQuery(type = 'test')
#     assert db_query == 'SELECT * FROM asset_management_test.transactions'

# Modify below query to match current schema
# def test_getAssets():
#     db_query = dbu.getAssets(type = 'test')
#     assert db_query == 'SELECT * FROM asset_management_test.assets'