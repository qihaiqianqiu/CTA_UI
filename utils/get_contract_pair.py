from utils.const import exchange_breed_dict, db_para, client, invalid_month_dict, trade_day
from utils.rename import rename_db_to_param
from utils.date_section_modification import to_trading_day_backwards
import pandas as pd
import re
import datetime

all = ["get_db_contract_pair",  "check_vaild_month"]

def get_param_contract_pair():
    breed_lst = []
    for key, values in exchange_breed_dict.items():
        breed_lst += exchange_breed_dict[key]
    breed_lst += [x.lower() for x in breed_lst]
    cta_table = db_para['tb_to']
    SQL = "SELECT distinct contract, breed from " + cta_table + " where breed in " + str(tuple(breed_lst))
    print(SQL)
    df = client.query_dataframe(SQL).sort_values('contract')
    contract_pair_dict = {}
    for breed_class in df.groupby('breed'):
        contract_pair_lst = []
        contract_lst = breed_class[1]['contract'].tolist()
        contract_pair_lst += [[rename_db_to_param(contract_lst[i]), rename_db_to_param(contract_lst[i+1])] for i in range(len(contract_lst)-1)]
        contract_pair_dict[breed_class[0]] = contract_pair_lst
    return contract_pair_dict


def get_param_contract_pair_with_volume():
    breed_lst = []
    for key, values in exchange_breed_dict.items():
        breed_lst += exchange_breed_dict[key]
    breed_lst += [x.lower() for x in breed_lst]
    cta_table = db_para['tb_to']
    today = datetime.datetime.today().strftime('%Y%m%d')
    previous_trading_date = trade_day[trade_day.index(to_trading_day_backwards(int(today))) - 1]
    SQL = "SELECT distinct contract, breed, max(volume) from " + cta_table + " where breed in " + str(tuple(breed_lst)) + " and trading_date = " + str(previous_trading_date) + " group by contract, breed"
    df = client.query_dataframe(SQL).sort_values('contract')
    contract_pair_dict = {}
    for breed_class in df.groupby('breed'):
        contract_pair_lst = []
        contract_lst = breed_class[1]['contract'].tolist()
        volume_lst = breed_class[1]['max_volume_'].tolist()
        contract_pair_lst += [[rename_db_to_param(contract_lst[i]), rename_db_to_param(contract_lst[i+1]), min(volume_lst[i], volume_lst[i+1])] for i in range(len(contract_lst)-1)]
        contract_pair_dict[breed_class[0]] = contract_pair_lst
    return contract_pair_dict



def get_db_contract_pair():
    breed_lst = []
    for key, values in exchange_breed_dict.items():
        breed_lst += exchange_breed_dict[key]
    breed_lst += [x.lower() for x in breed_lst]
    cta_table = db_para['tb_to']
    SQL = "SELECT distinct contract, breed from " + cta_table + " where breed in " + str(tuple(breed_lst))
    print(SQL)
    df = client.query_dataframe(SQL).sort_values('contract')
    contract_pair_dict = {}
    for breed_class in df.groupby('breed'):
        contract_pair_lst = []
        contract_lst = breed_class[1]['contract'].tolist()
        contract_pair_lst += [[contract_lst[i], contract_lst[i+1]] for i in range(len(contract_lst)-1)]
        contract_pair_dict[breed_class[0]] = contract_pair_lst
    return contract_pair_dict


def check(breed:str, month:int):
    if breed in invalid_month_dict.keys():
        if month in invalid_month_dict[breed]:
            return False
        else:
            return True
    else:
        return False
    
def check_vaild_month(volume_threshold=80):
    contract_pair_dict = get_param_contract_pair_with_volume()
    df = pd.DataFrame()
    today = datetime.datetime.today()
    year = today.year
    month = today.month
    if month == 12:
        year += 1
        nxt_month = 1
    else:
        nxt_month = month + 1
    for key, values in contract_pair_dict.items():
        contract_breed = key.upper()
        contract_pair_lst = values
        breed_df = pd.DataFrame.from_dict({contract_breed:contract_pair_lst}, orient='columns')
        breed_df['contract_pair'] = breed_df[contract_breed].apply(lambda x: x[0] + "-" + re.search("[0-9]+", x[1]).group(0))
        breed_df['first_ins'] = breed_df[contract_breed].apply(lambda x: int(re.search("[0-9]+", x[0]).group(0)))
        breed_df['volume'] = breed_df[contract_breed].apply(lambda x: x[2])
        breed_df = breed_df[breed_df['first_ins'] >= int(str(year)[2:] + str(nxt_month).zfill(2))]
        breed_df['flag'] = breed_df.apply(lambda x: check(contract_breed, int(str(x['first_ins'])[-2:])), axis=1)
        df = pd.concat([df, breed_df], axis=0)
        df = df[['contract_pair', 'flag', 'volume']]
        df = df[df['volume'] >= volume_threshold]
    return df[['contract_pair', 'flag', 'volume']]

if __name__ == "__main__":
    print(check_vaild_month())