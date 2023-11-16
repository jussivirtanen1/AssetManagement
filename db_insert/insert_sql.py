import pandas as pd
import utils.db_utils as dbu
pd.set_option('display.max_columns', None)


# bank_account_p_key_columns = ['event_type', 'name', 'date' \
# , 'return_type', 'amount', 'bank', 'account']
# transactions_p_key_columns = ['event_type', 'name', 'date', 'return_type' \
# , 'industry', 'instrument', 'quantity', 'amount', 'bank', 'account']
# transactions_c_key_columns = ['date', 'name', 'currency', 'quantity', 'amount']

def insertBankAccounts():
    dbu.insertToDBFromFile('asset_management_prod'
                            ,'savings_account'
                            ,key_columns = ['event_type', 'name', 'date'
                            'asset_id', 'owner_id', 'amount', 'currency'])
    
def insertTransactions():
    dbu.insertToDBFromFile('asset_management_prod' \
    ,'asset_transactions'
    ,key_columns = ['asset_id', 'owner_id', 'name', 'date', 'quantity', 'amount'])



if __name__ == "__main__":
    print('Executing insert database operation!')
    # insertBankAccounts()
    # insertTransactions()
    print('Done!')