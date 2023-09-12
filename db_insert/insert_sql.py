import pandas as pd
import utils.db_utils as dbu
pd.set_option('display.max_columns', None)


# bank_account_p_key_columns = ['event_type', 'name', 'date' \
# , 'return_type', 'amount', 'bank', 'account']
# transactions_p_key_columns = ['event_type', 'name', 'date', 'return_type' \
# , 'industry', 'instrument', 'quantity', 'amount', 'bank', 'account']
# transactions_c_key_columns = ['date', 'name', 'currency', 'quantity', 'amount']


dbu.insertToDBFromFile('asset_management_p_dev'
                        ,'bank_accounts'
                        ,key_columns = ['event_type', 'name', 'date'
                        , 'return_type', 'amount', 'bank', 'account'])
dbu.insertToDBFromFile('asset_management_p_dev' \
,'transactions',
key_columns = ['event_type', 'name', 'date', 'return_type', 'industry', \
 'instrument', 'quantity', 'amount', 'bank', 'account'])

