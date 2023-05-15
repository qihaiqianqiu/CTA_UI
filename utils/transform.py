"""
将时序预测的结果整理，添加常量列和顺序，生成可用的参数表
"""
import pandas as pd
from . import const

all = ["transform"]
def transform(df:pd.DataFrame):
    #print(df)
    # 常量列
    df = df.drop(columns=['reduce_date', 'clear_date'])
    df = df.rename(columns={"unit_num_base_boundary":"boundary_unit_num", "unit_num_base_region":"region_unit_num"})
    df['indate_date'] = "20220926_0"
    df['region_drift'] = 0
    df['wait_window'] = 3000000
    df['favor_times'] = 3
    df['unfavor_times'] = 1
    df['abs_threshold'] = 50
    df['wait2_windows'] = 2000
    df['after_tick'] = 30
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
    df['night_type'] = df.apply(lambda x: 2 if x['kind'] == "SC" else 1, axis=1)
    # 排序
    order = const.param_columns
    df = df[order]
    return df
