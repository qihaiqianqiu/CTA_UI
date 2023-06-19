import pandas as pd
import os, re
from const import PARAM_PATH
all = ["mute_month"]

def mute_by_instruct(ins:str, operator:int):
    df = pd.read_csv(os.path.join(PARAM_PATH, 'BASE', "params.csv"))    
    figure = re.search("[0-9]+", ins).group(0)
    if len(figure) > 0:
        # 有数字
        str_num = '{:02d}'.format(figure)
        df['if_add'] = df.apply(lambda x: operator if (month_check(x['first_instrument'], str_num)) or (month_check(x['second_instrument'], str_num)) or (x['if_add'] == operator)  else int(not bool(operator)), axis = 1)
    if ins == "反套":
        df['if_add'] = df.apply(lambda x: 0 if (x['if_add'] == 0)  else 1, axis = 1)
        
def month_check(code, month:str):
    if int(code.split('.')[0][-2:]) == int(month):
        return True
    else:
        return False
    
def reverse_check():
    pass