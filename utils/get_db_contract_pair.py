from . import const
import pandas as pd

def get_db_contract_pair():
    breed_lst = []
    for key, values in const.exchange_breed_dict.items():
        breed_lst += const.exchange_breed_dict[key]
    breed_lst += [x.lower() for x in breed_lst]
    cta_table = const.db_para['tb_to']
    SQL = "SELECT distinct contract, breed from " + cta_table + " where breed in " + str(tuple(breed_lst))
    print(SQL)
    df = const.client.query_dataframe(SQL).sort_values('contract')
    contract_pair_dict = {}
    for breed_class in df.groupby('breed'):
        contract_pair_lst = []
        contract_lst = breed_class[1]['contract'].tolist()
        contract_pair_lst += [[contract_lst[i], contract_lst[i+1]] for i in range(len(contract_lst)-1)]
        contract_pair_dict[breed_class[0]] = contract_pair_lst
    return contract_pair_dict

if __name__ == "__main__":
    get_db_contract_pair()