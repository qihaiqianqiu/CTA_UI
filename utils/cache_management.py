from . import date_section_modification
import os
import pandas as pd

__all__ = ["check_cache", "get_most_updated_cache", "cache_param", "save_param"]
# 检查是否已缓存
# 流程：检查文件名 -> 检查region_info内容:{根据套利对检查boundary_info并重算boundary, 根据region_info重算region, 写csv}
def check_cache(account_name:str, date:int, section:int, pairs:pd.Series, args:tuple):
    # 检查文件名
    date, section = date_section_modification.to_predict(date, section)
    args_dirname = "q={}_step={}_ratio={}".format(args[0], args[1], args[2])
    ACC_PATH = os.path.join(r".\params", account_name)
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

def get_most_updated_cache(account_name:str, pairs:pd.Series, args:tuple):
    # 检查文件名
    args_dirname = "q={}_step={}_ratio={}".format(args[0], args[1], args[2])
    ACC_PATH = os.path.join(r".\params", account_name)
    LOG_PATH = os.path.join(ACC_PATH, 'log')
    ARG_PATH = os.path.join(LOG_PATH, args_dirname)
    filename = max(os.listdir(ARG_PATH))
    FILE_PATH = os.path.join(ARG_PATH, filename)
    flag = True
    return flag, FILE_PATH


def cache_param(account_name:str, df, date, section, args:tuple):
    # date, section to predict value
    date, section = date_section_modification.to_predict(date, section)
    print("Cache predict section", date,section)
    args_dirname = "q={}_step={}_ratio={}".format(args[0], args[1], args[2])
    ACC_PATH = os.path.join(r".\params", account_name)
    LOG_PATH = os.path.join(ACC_PATH, "log")
    ARG_PATH = os.path.join(LOG_PATH, args_dirname)
    if not os.path.exists(ACC_PATH):
        os.mkdir(ACC_PATH)
    if not os.path.exists(LOG_PATH):
        os.mkdir(LOG_PATH)
    if not os.path.exists(ARG_PATH):
        os.mkdir(ARG_PATH)
    filename = "params_" + str(date) + "_" + str(section) + ".csv" 
    df.to_csv(os.path.join(ACC_PATH, 'params.csv'), index=False)
    df.to_csv(os.path.join(ARG_PATH, filename), index=False)


def save_param(account_name:str, df):
    ACC_PATH = os.path.join(r".\params", account_name)
    df.to_csv(os.path.join(ACC_PATH, 'params.csv'), index=False)