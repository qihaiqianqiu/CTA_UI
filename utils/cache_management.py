from utils.date_section_modification import to_predict
from utils.const import PARAM_PATH, INFO_PATH
from utils.transform import param_split
import os
import pandas as pd

__all__ = ["check_cache", "get_most_updated_cache", "cache_param", "save_param"]
# 检查是否已缓存
# 流程：检查文件名 -> 检查region_info内容:{根据套利对检查boundary_info并重算boundary, 根据region_info重算region, 写csv}
def check_cache(account_name:str, date:int, section:int, pairs:pd.Series, args:tuple):
    # 检查文件名
    date, section = to_predict(date, section)
    args_dirname = "q={}_step={}_ratio={}".format(args[0], args[1], args[2])
    ACC_PATH = os.path.join(PARAM_PATH , account_name)
    LOG_PATH = os.path.join(ACC_PATH, 'log')
    ARG_PATH = os.path.join(LOG_PATH, args_dirname)
    filename = "params_" + str(date) + "_" + str(section) + ".csv" 
    FILE_PATH = os.path.join(ARG_PATH, filename)
    flag = False
    # 检查文件内容
    if os.path.exists(FILE_PATH):
        cached = pd.read_csv(FILE_PATH)
        if pairs.equals(cached['pairs_id']):
            flag = True
    return flag, FILE_PATH

# Deprecated
def get_most_updated_cache(account_name:str, pairs:pd.Series, args:tuple):
    # 每次从当前程序的基账户更新region_df
    if account_name == 'BASE':
        BASE_PATH = os.path.join(PARAM_PATH, 'BASE', 'params.csv')
        region_df = param_split(pd.read_csv(BASE_PATH))   
        region_df.to_excel(os.path.join(INFO_PATH, 'region_info.xlsx'))
        flag = True
    # 检查文件名
    else:
        args_dirname = "q={}_step={}_ratio={}".format(args[0], args[1], args[2])
        ACC_PATH = os.path.join(PARAM_PATH, account_name)
        LOG_PATH = os.path.join(ACC_PATH, 'log')
        ARG_PATH = os.path.join(LOG_PATH, args_dirname)
        filename = max(os.listdir(ARG_PATH))
        FILE_PATH = os.path.join(ARG_PATH, filename)
    return flag, FILE_PATH

# 向账户目录下的log目录留存参数表缓存记录
def cache_param(account_name:str, df, date, section, args:tuple):
    # date, section to predict value
    date, section = to_predict(date, section)
    print("Cache predict section", date,section)
    args_dirname = "q={}_step={}_ratio={}".format(args[0], args[1], args[2])
    ACC_PATH = os.path.join(PARAM_PATH, account_name)
    LOG_PATH = os.path.join(ACC_PATH, "log")
    ARG_PATH = os.path.join(LOG_PATH, args_dirname)
    if not os.path.exists(ACC_PATH):
        os.mkdir(ACC_PATH)
    if not os.path.exists(LOG_PATH):
        os.mkdir(LOG_PATH)
    if not os.path.exists(ARG_PATH):
        os.mkdir(ARG_PATH)
    filename = "params_" + str(date) + "_" + str(section) + ".csv" 
    df.to_csv(os.path.join(ACC_PATH, 'params.csv'))
    df.to_csv(os.path.join(ARG_PATH, filename))

# 向账户目录下对应账户直接生成params.csv
def save_param(account_name:str, df):
    ACC_PATH = os.path.join(PARAM_PATH, account_name)
    df.to_csv(os.path.join(ACC_PATH, 'params.csv'))