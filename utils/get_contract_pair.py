from utils.const import exchange_breed_dict, db_para, client, invalid_month_dict, trade_day
from utils.rename import rename_db_to_param, rename
from utils.date_section_modification import to_trading_day_backwards
import pandas as pd
import re
import datetime

# 获取合约对名称，SP指令，成交量等信息

all = ["get_db_contract_pair",  "check_vaild_month", "get_param_contract_pair_with_volume", "get_param_contract_pair", "check_vaild_month_with_volume", "check", "get_contract_pair_rank", "get_exchange_on"]

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


def get_exchange_on(contract_code):
    print(contract_code)
    first_contract_code = contract_code.split('-')[0]
    breed = re.search("[a-zA-Z]+", first_contract_code).group(0)
    second_contract_code = breed + contract_code.split('-')[1]
    for exchange, values in exchange_breed_dict.items():
        if breed in values:
            first_contract_code = first_contract_code + "." + exchange
            second_contract_code = second_contract_code + "." + exchange
            break 
    return [first_contract_code, second_contract_code]


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
    print(df)
    contract_pair_dict = {}
    for breed_class in df.groupby('breed'):
        contract_pair_lst = []
        contract_lst = breed_class[1]['contract'].tolist()
        volume_lst = breed_class[1]['max_volume_'].tolist()
        contract_pair_lst += [[rename_db_to_param(contract_lst[i]), rename_db_to_param(contract_lst[i+1]), min(volume_lst[i], volume_lst[i+1])] for i in range(len(contract_lst)-1)]
        contract_pair_dict[breed_class[0]] = contract_pair_lst
    print(contract_pair_dict)
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

#检查是否可转抛
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
        if len(contract_pair_lst) == 0:
            # 跳过一些品种，比如说有的品种只有一个合约
            continue
        breed_df = pd.DataFrame.from_dict({contract_breed:contract_pair_lst}, orient='columns')
        breed_df['contract_pair'] = breed_df[contract_breed].apply(lambda x: x[0] + "-" + re.search("[0-9]+", x[1]).group(0))
        breed_df['first_ins'] = breed_df[contract_breed].apply(lambda x: int(re.search("[0-9]+", x[0]).group(0)))
        breed_df['volume'] = breed_df[contract_breed].apply(lambda x: x[2])
        breed_df = breed_df[breed_df['first_ins'] >= int(str(year)[2:] + str(nxt_month).zfill(2))]
        print(breed_df)
        breed_df['flag'] = breed_df.apply(lambda x: check(contract_breed, int(str(x['first_ins'])[-2:])), axis=1)
        df = pd.concat([df, breed_df], axis=0)
        df = df[['contract_pair', 'flag', 'volume']]
        df = df[df['volume'] >= volume_threshold]
    print(df)
    return df[['contract_pair', 'flag', 'volume']]

def get_contract_pair_rank(contract_pair: list):
    contract_pair = [rename(contract) for contract in contract_pair]
    cta_table = db_para['tb_to']
    today = datetime.datetime.today().strftime('%Y%m%d')
    previous_trading_date = trade_day[trade_day.index(to_trading_day_backwards(int(today))) - 1]
    SQL = "SELECT distinct contract, max(volume) from " + cta_table + " where contract in " + str(tuple(contract_pair)) + " and trading_date = " + str(previous_trading_date) + " group by contract"
    print(SQL)
    df = client.query_dataframe(SQL).sort_values('max_volume_', ascending=False)
    print(df)
    # 异常处理：查询不到的话
    if len(df) == 0:
        first_contract = contract_pair[0]
        second_contract = contract_pair[1]
    first_contract = df['contract'].tolist()[0]
    second_contract = df['contract'].tolist()[1]
    return [first_contract, second_contract]


def get_sp_instruction():
    pass


if __name__ == "__main__":
    #print(check_vaild_month())
    #print(get_contract_pair_rank(['IH2309', 'IH2312']))
    #print(get_db_contract_pair())
    print(get_param_contract_pair_with_volume())