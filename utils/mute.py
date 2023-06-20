import pandas as pd
import re
from utils.const import PARAM_PATH
from utils.get_contract_pair import check

all = ["mute_by_instruct"]

def mute_by_instruct(ins:str, operator:int, df:pd.DataFrame):
    if re.search(r'\d', ins):
        figure = re.search("[0-9]+", ins).group(0)
        # 有数字
        str_num = '{:02d}'.format(int(figure))
        df['if_add'] = df.apply(lambda x: operator if (month_check(x['first_instrument'], str_num)) or (month_check(x['second_instrument'], str_num)) or (x['if_add'] == operator)  else int(not bool(operator)), axis = 1)
    if ins == "反套":
        df['if_add'] = df.apply(lambda x: operator if (re.search("[0-9]+", x['first_instrument']).group(0) > re.search("[0-9]+", x['second_instrument']).group(0)) or (x['if_add'] == operator) else int(not bool(operator)), axis = 1)
    if ins == "不可转抛合约":
        df['if_add'] = df.apply(lambda x: operator if not(check(x['kind'], int(x['first_instrument'].split('.')[0][-2:]))) or (x['if_add'] == operator) else int(not bool(operator)), axis = 1)
    if ins == "全合约":
        df['if_add'] = df.apply(lambda x: operator, axis = 1)
    return df
        
def month_check(code, month:str):
    if int(code.split('.')[0][-2:]) == int(month):
        return True
    else:
        return False