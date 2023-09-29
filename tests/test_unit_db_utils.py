import utils.db_utils as dbu
import pandas as pd

# Test dataframes for assets config
test_assets_config_data = {'name': ['Nordea Bank Oyj', 'Sampo Oyj', 'USA Indeksirahasto'
                                    , 'Tanska Indeksirahasto', 'Salesforce'],
                          'yahoo_ticker': ['NDA-FI.HE', 'SAMPO.HE', '0P0001K6NM.F'
                                    , '0P000134KA.CO', 'CRM'],
                          'yahoo_fx_ticker': ['EUREUR=X', 'EUREUR=X'
                                    , 'EUREUR=X', 'EURDKK=X', 'EURUSD=X'],
                          'currency': ['EUR', 'EUR', 'EUR'
                                    , 'DKK', 'USD'],
                          'instrument': ['Stock', 'Stock', 'Mutual fund'
                                    , 'Mutual fund', 'Stock'],
                          'region': ['Finland', 'Finland', 'North America'
                                     , 'Denmark', 'North America'],
                          'bank': ['Nordea', 'Nordea', 'Nordnet'
                                   , 'Nordnet', 'Nordea'],
                          'industry': ['Finance', 'Insurance', 'General index'
                                    , 'General index', 'IT and consulting']}

test_assets_config_df = pd.DataFrame(data=test_assets_config_data)

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
    assert user_config_df['user_name'][0] == 'testuser'
    assert user_config_df['host_name'][0] == 'localhost'
    assert user_config_df['database'][0] == 'testdb'
    assert user_config_df['port'][0] == '5432'
    assert user_config_df['engine'][0] == 'postgresql+psycopg2://'

def test_getDBConnection():
    db_conn_0 = dbu.getDBConnection(env = 'test' \
                                    ,user_file_name='tests/mock_user_config.json' \
                                    ,user_index=0)
    assert db_conn_0.url.username == 'testuser'
    assert db_conn_0.url.host == 'localhost'
    assert db_conn_0.url.database == 'testdb'
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


