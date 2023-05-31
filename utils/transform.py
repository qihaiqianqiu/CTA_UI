"""
将时序预测的结果整理，添加常量列和顺序，生成可用的参数表
"""
import pandas as pd
from . import const

all = ["add_suffix", "param_split"]

# deprecated
def add_suffix(df:pd.DataFrame):
    #print(df)
    # 常量列
    df['indate_date'] = "20220926_0"
    df['region_drift'] = 0
    df['wait_window'] = 3000000
    df['favor_times'] = 3
    df['unfavor_times'] = 1
    df['abs_threshold'] = 50
    df['wait2_windows'] = 2000
    df['after_tick'] = 30
    df['today_fee'] = 2
    df['night_type'] = 1
    df['if_add'] = 1
    df['limitcoef'] = 8
    df['abs_threshold_after'] = 25
    df['before_tick'] = 0
    df['before_cancel_flag'] = 1
    df['before_cancel_num'] = 1
    df['max_position'] = 60
    df['min_position'] = 0
    # 计算列
    df['night_type'] = df.apply(lambda x: 2 if (x['kind'] == "SC" or x['kind'] == "AU" or x['kind'] == 'AG') else 1, axis=1)
    # 排序
    order = const.param_columns
    df = df[order]
    return df

# 从参数表中分离出区信息
def param_split(df:pd.DataFrame):
    region_df = df[["region_drift", "region_0", "region_1", "region_2", "region_3", "region_4", "region_5", "region_6", "region_7", "region_tick_lock" , "region_unit_num"]]
    boundary_df = df[["up_boundary_5", "up_boundary_4", "up_boundary_3", "up_boundary_2", "up_boundary_1", "down_boundary_1", "down_boundary_2", "down_boundary_3", "down_boundary_4", "down_boundary_5", "boundary_unit_num", "boundary_tick_lock"]]
    suffix_df = df[[column for column in df.columns if column not in region_df and column not in boundary_df]]
    return region_df, boundary_df, suffix_df

