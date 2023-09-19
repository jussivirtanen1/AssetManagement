import pandas as pd
import json
from sqlalchemy import create_engine
import configparser
import getpass
config = configparser.ConfigParser()
config.read('config/passwords.config')
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

def getDBConnection(user: str, host_name: str, db: str):
    with open('config/user_config.json','r') as f:
        user_config_data = json.loads(f.read())
        config_df = pd.json_normalize(user_config_data, record_path =['config'])
    # Specify for database connection
    port = config_df['port'][0]
    engine_string = config_df['engine'][0] \
                    + user + ':' \
                    + config.get('PASSWORDS', 'password_' + getpass.getuser()) \
                    + '@' + host_name + ':' + port + '/' + db
    conn = create_engine(engine_string)
    return conn

def getDBQuery_p():
    query = """SELECT * FROM asset_management_p.transactions"""
    return query

def getDBQuery_c():
    query = """SELECT * FROM asset_management_c.transactions"""
    return query

def getAssets(type: str):
    query = f"""SELECT * FROM asset_management_{type}.assets"""
    return query


def insertToDBFromFile(schema, table, key_columns: list):
    query = f"""SELECT * FROM {schema}.{table}"""
    config_df = getConfigurationsData('config/user_config.json')
    db_conn = getDBConnection(user=config_df['username'][0] \
                    , host_name=config_df['host_name'][0] \
                    , db=config_df['database'][0])
    db_df = fetchDataFromDB(query, conn = db_conn)

    # Get data to be inserted from csv, ignore commented example rows
    insert_df = pd.read_csv(f'db_insert/{table}_insert.csv', comment="#", quotechar='"')
    insert_file_columns = insert_df.columns
    # Convert date column to correct datetime format
    insert_df['date'] = pd.to_datetime(insert_df['date']
                                       ,format = "%Y-%m-%d") \
                                       .dt.date
    # Convert insert dataframe's other column datatypes to same as in SQL table
    insert_df = insert_df.astype(db_df.dtypes.to_dict())
    # Check that datatypes match between dataframes, should be True
    if ((insert_df.dtypes == db_df.dtypes).all()) != True:
        raise ValueError('Data types between db and insert dataframes do not match!')
    
    # Check that column names between insert df and database df match
    if (set(db_df.columns) == set(insert_df.columns)) != True:
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
    write_file_name = f'db_insert/{table}_insert.csv'
    print("succesfully loaded data in to database!")
    pd.DataFrame(data=[], columns = insert_file_columns) \
                        .to_csv(write_file_name, index = False)
    print("Succesfully emptied insert table!")