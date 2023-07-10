"""
从成交记录中，根据报单时间进行套利对成交的核算
"""
import pandas as pd
import re
import datetime
import os
from utils.compare import get_param_pairs
from utils.rename import rename, rename_db_to_param
from utils.const import ROOT_PATH

all = ["fix_trade_record"]
# 以下为成交记录合成模块
def combine(df):
    df["总价"] = df['价格'] * df['手数']
    df = df[["套利对", "交易时间", "操作", "价格", "手数", "总价"]].groupby(['套利对',"操作"],as_index=False).aggregate({"套利对":"first", "交易时间":"first", "操作":"first", "价格":"first", "手数":"sum", "总价":"sum"})
    df['价格'] = df['总价'] / df['手数']
    del df['总价']
    return df


def preprocessing(df):
    df['deal'] = df['deal'].astype('int')
    df['time'] = df['time'].apply(lambda x: pd.to_datetime(x))
    df['code'] = df['code'].apply(lambda x: x.strip(' '))
    df['breed'] = df['code'].apply(lambda x:re.sub("[\u4e00-\u9fa5\0-9\,\。]", "", x).upper())
    df = df[df['price'] != '-']
    df['price'] = df['price'].astype('float')
    df['direction'] = df['direction'].apply(lambda x: -1 if '卖' in x else 1)
    df['deal'] = df['deal'] * df['direction']
    df['count'] = df['deal'] * df['price']
    del df['direction']
    return df


def raw_pairing(df, pairing, sec_interval=0, max_interval=90):
    df = df.sort_values('time')
    pairing_slice = []
    pairing_delta = []
    # 按品种遍历成交记录
    for temp in df.groupby('breed', as_index=False):
        if temp[0] not in ['IOC', 'IOP', 'MOC', 'MOP']:
            start = 0
            # 如果两条交易记录落在一定的时间差之内
            for i in range(1, len(temp[1])):
                print(sec_interval)
                if (temp[1].iloc[i]['time'] - temp[1].iloc[start]['time']) > datetime.timedelta(seconds=sec_interval):
                    # 提取切片
                    sliced = temp[1].iloc[start:i]
                    print("hit time interval:", (temp[1].iloc[i]['time'] - temp[1].iloc[start]['time']).total_seconds(), sec_interval, sliced)
                    pairing_slice.append(sliced)
                    start = i
            pairing_slice.append(temp[1].iloc[start:len(temp[1])])

    single_df = pd.DataFrame()
    # 更长的时间间隔 精细配对落单腿
    for pair in pairing_slice:
        if len(pair) > 1 and pair['deal'].sum() == 0:
            pairing_delta.append(pair)
        else:
            single_df = pd.concat([single_df, pair])
    pairing += pairing_delta
    # Recursive
    # 退出条件
    if (len(pairing_delta) == 0 and sec_interval >= max_interval) or len(single_df) == 0:
        print(sec_interval, max_interval)
        print(pairing, single_df)
        return pairing ,single_df
    else:
        ("++++")
        print(single_df)
        return raw_pairing(single_df, pairing, sec_interval+1, max_interval)
  

def parse(df):
    pairs = []
    # single_df deprecated
    single_df = pd.DataFrame()
    # 4秒间隔起-粗配对
    pairs, single_df = raw_pairing(df, pairs)
    #print(pairs)

    # 落单交易记录-细配对
    if len(single_df) > 0:
        append_pairs = []
        print(single_df)
        print("***")
        max_interval_sec = 0
        min_interval_sec = 9999
        for temp in single_df.groupby('breed', as_index=False):
            interval_sec = temp[1].iloc[-1]['time'] - temp[1].iloc[0]['time']
            interval_sec = interval_sec.total_seconds()
            if interval_sec < min_interval_sec:
                min_interval_sec = interval_sec
            if interval_sec > max_interval_sec:
                max_interval_sec = interval_sec
        append_pairs, dropped_df = raw_pairing(single_df, append_pairs, sec_interval=min_interval_sec-1, max_interval=max_interval_sec+1)
        pairs += append_pairs

    for item in pairs:
        if item.iloc[0]['breed'] == "AL":
            print(item)
        item['price'] = item['count'] / item['deal']
    return pairs


def match(df, buffer, param):
    # print(df)
    if len(df) == 2:
        near = df[df['deal'] > 0]['code'].values[0]
        forward = df[df['deal'] < 0]['code'].values[0]
        print(near, forward)
        # 根据Param中的套利对决定near, forward的归属
        if len(param[(param['code1'] == forward) & (param['code2'] == near)]) > 0:
            temp = forward
            forward = near
            near = temp
        forward_dt = re.findall(r"\d+.?\d*",forward)[0].replace(' ','')
        if len(forward_dt) == 3:
            forward_dt = '2' + forward_dt
        pair = (rename_db_to_param(near) + '-' + forward_dt).upper()
        time = df['time'].iloc[0]
        if df[df['code'] == near]['deal'].values[0] > 0:
            operation = "买"
        else:
            operation = "卖"
        num = abs(df['deal'].iloc[0])
        price = df[df['code'] == near]['price'].values[0] - df[df['code'] == forward]['price'].values[0]
        data = pd.DataFrame({"套利对":[pair], "交易时间":time, "操作":operation, "价格":price, "手数":num})
        return data, buffer
    
    else:
        long = df[df['deal'] > 0].iloc[0:1]
        near = long['code']
        short = df[df['deal'] < 0].iloc[0:1]
        forward = short['code']
        volume = min(long['deal'].values[0], -1 * short['deal'].values[0])
        long['deal'] = volume
        short['deal'] = -1 * volume
        pair_df = pd.concat([long,short])
        #print(pair_df)
        buffer.append(pair_df)
        # modify orignal DF
        df.loc[long.index, 'deal'] -= volume
        df.loc[short.index, 'deal'] += volume
        df = df[df['deal'] != 0]
        return match(df, buffer, param)
        

def fix_trade_record(acc_name):
    param_df = get_param_pairs(acc_name)
    param_df['code1'] = param_df['code1'].apply(lambda x: rename(x))
    param_df['code2'] = param_df['code2'].apply(lambda x: rename(x))
    tradeFileDir = os.path.join(ROOT_PATH, "tradings")
    tradeFileDir = os.path.join(tradeFileDir, acc_name)
    if not os.path.exists(tradeFileDir):
        os.mkdir(tradeFileDir)
    tradeFiles = [f for f in os.listdir(tradeFileDir) if "sorted" not in f]
    for filename in tradeFiles:
        if filename.split('.')[0] + '_sorted.csv' in os.listdir(tradeFileDir):
            print(filename)
            continue
        file = os.path.join(tradeFileDir, filename)
        try:
            df = pd.read_csv(file, encoding='gbk')
            print(df.columns)
            if '成交合约' in df.columns:
                df = df[['成交合约','买卖','手数','成交价格','成交时间']]
            else:
                df = df[['合约','买卖','成交手数','成交价格','成交时间']]
            df.columns = ['code','direction','deal', 'price','time']
            df = preprocessing(df)
            trade_record = pd.DataFrame()
            match_buffer = []
            for item in parse(df):
                #print(item)
                pair_res, match_buffer = match(item, match_buffer, param_df)
                trade_record = pd.concat([trade_record, pair_res])
                #print(trade_record)
            for item in match_buffer:
                trade_record = pd.concat([trade_record, match(item, [], param_df)[0]])
            res = combine(trade_record).sort_values('套利对')
            print(res)
            res.to_csv(os.path.join(tradeFileDir, filename.split('.')[0] + '_sorted.csv'), index=False, encoding='gbk')
        except KeyError as e:
            print(e)
            print("Error file format in: " + acc_name)