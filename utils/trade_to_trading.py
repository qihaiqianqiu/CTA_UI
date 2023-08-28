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
import traceback

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
                #print(sec_interval)
                if (temp[1].iloc[i]['time'] - temp[1].iloc[start]['time']) > datetime.timedelta(seconds=sec_interval):
                    # 提取切片
                    sliced = temp[1].iloc[start:i]
                    #print("hit time interval:", (temp[1].iloc[i]['time'] - temp[1].iloc[start]['time']).total_seconds(), sec_interval, sliced)
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
        #print(sec_interval, max_interval)
        #print(pairing, single_df)
        return pairing ,single_df
    else:
        #("++++")
        #print(single_df)
        return raw_pairing(single_df, pairing, sec_interval+1, max_interval)
  

def parse(df):
    pairs = []
    dropped_single = pd.DataFrame()
    # 架构套/瘸腿
    structure = pd.DataFrame()
    # 粗配对
    pairs, single_df = raw_pairing(df, pairs)
    print("粗匹配后的未配对合约数：", len(single_df))
    # 落单交易记录-细配对
    if len(single_df) > 0:
        append_pairs = []
        max_interval_sec = 0
        min_interval_sec = 9999
        for temp in single_df.groupby('breed', as_index=False):
            interval_sec = temp[1].iloc[-1]['time'] - temp[1].iloc[0]['time']
            interval_sec = interval_sec.total_seconds()
            if interval_sec < min_interval_sec:
                min_interval_sec = interval_sec
            if interval_sec > max_interval_sec:
                max_interval_sec = interval_sec
            print("最大时间间隔：", max_interval_sec, "最小时间间隔：", min_interval_sec)
            if max_interval_sec - min_interval_sec > 1000:
                max_interval_sec = min_interval_sec + 900
            dropped_df = pd.DataFrame()
            print("*******{}********".format(temp[1]))
            append_pairs, dropped_df = raw_pairing(temp[1], append_pairs, sec_interval=min_interval_sec-1, max_interval=max_interval_sec+1)
            pairs += append_pairs
            dropped_single = pd.concat([dropped_single, dropped_df])
    if len(dropped_single) > 0:
                            # 结构套
        """
        long = dropped_single[dropped_single['deal'] > 0]
        short = dropped_single[dropped_single['deal'] < 0]

        long_volume = long['deal'].sum()
        short_volume = short['deal'].sum()
        print("异常多空为{}、{}".format(long, short))

        if min(long_volume, abs(short_volume)) == 0:
            ratio = 0
        else:
            ratio = max(long_volume, abs(short_volume)) / min(long_volume, abs(short_volume))
        def is_decimal_convertible_to_integer(decimal_value):
            if decimal_value == 0:
                return 0
            else:
                decimal_part = decimal_value - int(decimal_value)  # 提取小数部分
                return decimal_part == 0
        
        if is_decimal_convertible_to_integer(ratio):
            # 多空腿直接撮合
            for i in range(len(long)):
                for j in range(len(short)):
                    if long.iloc[i]['deal'] > 0:
                        vol = min(long.iloc[i]['deal'], abs(short.iloc[j]['deal']))
                        # 判断多空的比率，倍数加在谁头上
                        if long_volume >= short_volume:
                            long.iloc[i]['deal'] -= vol * ratio
                            short.iloc[j]['deal'] += vol
                            structure = pd.concat([structure, pd.DataFrame({'套利对':[str(int(ratio)) + long.iloc[i]['code'].lower() + '-' + short.iloc[j]['code'].lower()], '交易时间':[long.iloc[i]['time']], '操作':['买'], '价格':[(ratio * long.iloc[i]['price'] - short.iloc[j]['price'])], '手数':[vol]})])
                        else:
                            long.iloc[i]['deal'] -= vol 
                            short.iloc[j]['deal'] += vol * ratio
                            structure = pd.concat([structure, pd.DataFrame({'套利对':[long.iloc[i]['code'].lower() + '-' + str(int(ratio)) + short.iloc[j]['code'].lower()], '交易时间':[long.iloc[i]['time']], '操作':['买'], '价格':[(long.iloc[i]['price'] - ratio * short.iloc[j]['price'])], '手数':[vol]})])
        else:"""
            # 必有瘸腿 可能还包含结构套
        structure = dropped_single.groupby('code', as_index=False).aggregate({"deal":"sum", "price":"first", "time":"first", "breed":"first", "count":"sum"})
        structure = structure[structure['deal']!=0]
        print("结构套：")
        print(structure) 
        if len(structure) > 0:
            structure['price'] = structure['count'] / structure['deal']
            structure = structure.rename(columns={"code":"套利对", "deal":"手数", "price":"价格", "time":"交易时间", "breed":"品种", "count":"总价"})
            structure['操作'] = structure.apply(lambda x: "买" if x['手数'] > 0 else "卖", axis=1)
            structure = structure[['套利对', '交易时间', '操作', '价格', '手数']]                             
        else: structure = pd.DataFrame()     
                 
    return pairs, structure


def match(df, buffer, param):
    df = df[df['deal'] != 0]
    print("df before match:", df)
    if len(df) == 0:
        # 出现在【多次】平对锁/平瘸腿后合成交易记录中
        return pd.DataFrame(), buffer
    
    if len(df) == 2:
        print("in branch1")
        near = df[df['deal'] > 0]['code'].values[0]
        forward = df[df['deal'] < 0]['code'].values[0]
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
        print("in branch2")
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
            continue
        file = os.path.join(tradeFileDir, filename)
        print("读取交易记录：{}".format(file))
        try:
            df = pd.read_csv(file, encoding='gbk', index_col=False)
            print("读取交易记录：{}".format(file))
            print(df)
            if '成交合约' in df.columns:
                df = df[['成交合约','买卖','手数','成交价格','成交时间']]
            else:
                df = df[['合约','买卖','成交手数','成交价格','成交时间']]
            df.columns = ['code','direction','deal', 'price','time']
            df = df.dropna()
            df = preprocessing(df)
            trade_record = pd.DataFrame()
            match_buffer = []
            pairs, structure = parse(df)
            for item in pairs:
                pair_res, match_buffer = match(item, match_buffer, param_df)
                trade_record = pd.concat([trade_record, pair_res])
            for item in match_buffer:
                trade_record = pd.concat([trade_record, match(item, [], param_df)[0]])
            res = combine(trade_record).sort_values('套利对')
            res = pd.concat([res, structure])
            print(res)
            res.to_csv(os.path.join(tradeFileDir, filename.split('.')[0] + '_sorted.csv'), index=False, encoding='gbk')
        except KeyError as e:
            print(traceback.format_exc())
            print("Error file format in: " + file)
            
if __name__ == "__main__":
    fix_trade_record("lq")