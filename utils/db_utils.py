import pandas as pd
import json
from sqlalchemy import create_engine
import configparser
config = configparser.ConfigParser()
pd.set_option('display.max_columns', None)

def getConfigByInstrument(assets_config_df, instrument):
    return assets_config_df[assets_config_df['instrument'] == instrument]

def getConfigurationsData(user_file_name):
    # Define user configuration file locally in your repo
    with open(user_file_name,'r') as f:
        user_config_data = json.loads(f.read())
        config_df = pd.json_normalize(user_config_data, record_path =['config'])
    return config_df

def fetchDataFromDB(query, conn):
    postgresql_table = pd.read_sql(query, conn)
    #postgresql_table['date'] = postgresql_table['date'].astype(str)
    return postgresql_table

def getDBConnection(env: str, user_file_name: str, user_index = 0):
    with open(user_file_name, 'r') as f:
        user_config_data = json.loads(f.read())
        config_df = pd.json_normalize(user_config_data, record_path =['config'])
    if env == 'prod':
        config.read('config/passwords.config')
    else:
        config.read('tests/passwords_tests.config')
    # Specify for database connection
    engine_string = config_df['engine'][user_index] \
                    + config_df['user_name'][user_index] + ':' \
                    + config.get('PASSWORDS', 'password') \
                    + '@' + config_df['host_name'][user_index] \
                    + ':' + config_df['port'][user_index] \
                    + '/' + config_df['database'][user_index]
    conn = create_engine(engine_string)
    return conn

def getDBQuery(filter: int):
    query = f"""SELECT event_type
                      ,asset_id
                      ,owner_id
                      ,name
                      ,date
                      ,quantity
                      ,price_fx
                      ,price_eur
                      ,amount
                      FROM asset_management_prod.asset_transactions
                      WHERE owner_id = {filter}"""
    return query

def getAssets(filter: int):
    query = f"""SELECT id.name
                      ,id.asset_id
                      ,id.yahoo_ticker
                      ,id.yahoo_fx_ticker
                      ,info.instrument
                FROM asset_management_prod.asset_ids AS id
                LEFT JOIN asset_management_prod.asset_owner AS own ON id.asset_id = own.asset_id
                LEFT JOIN asset_management_prod.asset_info AS info ON id.asset_id = info.asset_id
                WHERE owner_id = {filter}
                """
    return query


# def getAssets(type: str):
#     query = f"""SELECT * FROM asset_management_{type}.assets"""
#     return query


def insertToDBFromFile(schema, table, key_columns: list, schema_attr: 'p'):
    query = f"""SELECT * FROM {schema}.{table}"""
    config_df = getConfigurationsData('config/user_config.json')
    db_conn = getDBConnection(user_file_name='config/user_config.json' \
                    ,user=config_df['username'][0] \
                    ,host_name=config_df['host_name'][0] \
                    ,db=config_df['database'][0], user_index=0)
    db_df = fetchDataFromDB(query, conn = db_conn)

    # Get data to be inserted from csv, ignore commented example rows
    insert_df = pd.read_csv(f'db_insert/{table}_insert_{schema_attr}.csv'
                            , comment="#"
                            , quotechar='"')
    insert_file_columns = insert_df.columns
    # Convert date column to correct datetime format
    insert_df['date'] = pd.to_datetime(insert_df['date']
                                       ,format = "%Y-%m-%d") \
                                       .dt.date
    # Convert insert dataframe's other column datatypes to same as in SQL table
    insert_df = insert_df.astype(db_df.dtypes.to_dict())
    # Check that datatypes match between dataframes, should be True
    if not ((insert_df.dtypes == db_df.dtypes).all()):
        raise ValueError('Data types between db and insert dataframes do not match!')
    
    # Check that column names between insert df and database df match
    if not (set(db_df.columns) == set(insert_df.columns)):
        raise ValueError('Columns between insert file dataframe \
                          and database dataframe do not match!')
    
    # Check that any to be inserted rows exist in database table, i.e. return
    #  value from merge should be empty, i.e. below empty function returns True
    if (insert_df.merge(db_df, on = key_columns, how = 'inner').empty) is not True:
        raise ValueError('Insert file has rows existing in the database already!')
    # Create unique UUID for values to be inserted. TODO: apply this later
    # import uuid
    # insert_df['uuid'] = [uuid.uuid4() for _ in range(len(insert_df.index))]
    # Check that UUID between db and insertion table are unique, should be True
    # insert_df.merge(db_df, on=['uuid'], how='inner').empty
    
    # Insert data to database
    insert_df.to_sql(name = table
                     ,schema = schema
                     ,con=db_conn
                     ,if_exists="append"
                     ,index=False)
    # Empty (overwrite) existing insert file
    write_file_name = f'db_insert/{table}_insert_{schema_attr}.csv'
    print("succesfully loaded data in to database!")
    pd.DataFrame(data=[], columns = insert_file_columns) \
                        .to_csv(write_file_name, index = False)
    print("Succesfully emptied insert table!")