"""
用于合成多账户的交易持仓记录进行对比
包含一些导出单独记录时会用到的方法
"""
import os
import pandas as pd, numpy as np
import datetime
import re
import sys
from utils.const import ROOT_PATH 

all = ["get_hold", "get_param_pairs", "export_holdings_compare", "export_trading_compare"]
def get_hold(acc_name):
    ### 读取hold
    reportDir = os.path.join(ROOT_PATH, "report")
    reportFileDir = os.path.join(reportDir, acc_name)
    if not os.path.exists(reportFileDir):
        os.mkdir(reportFileDir)
    # 选择最新的持仓文件
    reportlst = os.listdir(reportFileDir)
    reportFile = os.path.join(reportFileDir, max(reportlst))
    hold = pd.read_csv(reportFile, encoding='gbk')
    if '持仓合约' and '总仓' in hold.columns.tolist():
        hold = hold[['持仓合约','买卖','总仓']]
    if '合约' and '总持仓' in hold.columns.tolist():    
        hold = hold[['合约', '买卖', '总持仓']]
    hold.columns = ['持仓合约','买卖','总仓']
    hold['a'] = hold['持仓合约'].apply(lambda x:x[:2])
    hold.columns = ['code','direction','current','a']
    # 'dt'字段为日期代码 形如2303 or 304 
    hold['dt'] = hold['code'].apply(lambda x:re.findall(r"\d+.?\d*",x)[0].replace(' ',''))
    # [\u4e00-\u9fa5] 匹配中文
    hold['product'] = hold['code'].apply(lambda x:re.sub("[\u4e00-\u9fa5\0-9\,\。]", "", x).upper())
    hold['dt'] = hold['dt'].apply(lambda x:'2'+x if len(x) < 4 else x)
    # 品种代码大写
    hold['code'] = hold['product'] + hold['dt']
    del hold['dt']
    # 计算持仓量 买则持仓*1 卖则持仓*-1 [\u3000] 匹配空格
    hold['direction'] = hold['direction'].apply(lambda x: x.strip())
    hold['current'] = np.where(hold['direction']=='买',hold['current'],-1*hold['current'])
    del hold['direction']
    ## 计算report中的各品种持仓
    hold = pd.DataFrame(hold.groupby('code')['current'].sum())
    hold = hold[hold['current'] != 0]
    hold = hold.reset_index()
    hold['product'] = hold['code'].apply(lambda x:re.sub("[\u4e00-\u9fa5\0-9\,\。]", "", x).upper())
    
    return hold


# 获取账户对应参数表含有的全部套利对
def get_param_pairs(acc_name):
    paramFileDir = os.path.join(ROOT_PATH, "params", acc_name)
    paramFile = os.path.join(paramFileDir , "params.csv")
    df = pd.read_csv(paramFile)
    df = df[['pairs_id']]
    # code-1 近月合约 code-2 远月合约
    # 拆分配对
    df['product'] = df['pairs_id'].apply(lambda x:x[:-9])
    df['code1'] = df['pairs_id'].apply(lambda x:x[:-9]+x[-9:-5])
    df['code2'] = df['pairs_id'].apply(lambda x:x[:-9]+x[-4:])
    df['code1_vol'] = 0
    df['code2_vol'] = 0
    df = df.reset_index()    
    return df


# 以下为持仓对比模块         
def export_holdings_compare(acc_lst):
    tmpvalue_dict = {}
    for acc_name in acc_lst:
        TmpValueDir = os.path.join(ROOT_PATH, "TmpValue")
        TmpValueFileDir = os.path.join(TmpValueDir, acc_name)
        TmpValueFile = os.path.join(TmpValueFileDir, "TmpValue.csv")
        if os.path.exists(TmpValueFile):
            tmpvalue_dict[acc_name] = pd.read_csv(TmpValueFile, header=None)
        else:
            continue
    counter = 0
    for key in tmpvalue_dict.keys():
        temp = tmpvalue_dict[key]
        temp.columns = ['pairs_id', key, 'holder_col1', 'holder_col2', 'holder_col3']
        temp = temp[['pairs_id', key]]
        if counter == 0:
            df = temp
            counter = 1
        else:
            df = pd.merge(left=df, right=temp, on='pairs_id', how='outer')
    try:
        df = df.fillna('0').sort_values(by='pairs_id')
        if not os.path.exists("./holding_compare"):
            os.mkdir("./holding_compare")
        df = df.reset_index(drop=True).set_index("pairs_id")
    except UnboundLocalError as e:
        print("未选中账户！")
    return df


# 以下为成交对比模块
def trade_record_processing(df):
    df['price'] = df['price'].astype('float')
    df['volume'] = df['volume'].astype('float')
    df['stocking'] = df['price'] * df['volume']
    print(df)
    df = df.groupby(['pairs_id', 'operation']).aggregate({"pairs_id":"first", "time":"first", "operation":"first", "price":"first", "volume":"sum", "stocking":"sum"})
    df['price'] = df['stocking'] / df['volume']
    df['price'] = df['price'].apply(lambda x: round(float(x),2))
    df = df[['price', 'volume']]
    print("-----")
    return df


def export_trading_compare(acc_lst:list):
    trading_dict = {}
    for acc_name in acc_lst:
        tradingDir = os.path.join(ROOT_PATH, "tradings")
        tradingFileDir = os.path.join(tradingDir, acc_name)
        # selected most recent trading record
        recent_date = "0"
        for filename in os.listdir(tradingFileDir):
            if '_sorted' in filename:
                dt = re.findall(r"\d+", filename)[0]
                if dt > recent_date:
                    recent_date = dt
        most_recent_tradingFile = max([f for f in tradingFileDir if str(recent_date) + "_sorted.csv" in f])
        tradingFile = os.path.join(tradingFileDir, most_recent_tradingFile)
        # 读取相应trading文件
        if os.path.exists(tradingFile):
            trading_dict[acc_name] = pd.read_csv(tradingFile, encoding='GBK')
        else:
            continue
    counter = 0
    for key in trading_dict.keys():
        temp = trading_dict[key]
        temp.columns = ['pairs_id', 'time', 'operation', 'price', 'volume']
        # 将trading文件按套利对的买/卖合并
        temp = trade_record_processing(temp)
        temp.columns = pd.MultiIndex.from_tuples([('price', key), ('volume', key)], names=["result", "id"])
        if counter == 0:
            df = temp
            counter += 1
        else:
            df = pd.merge(left=df, right=temp, left_on=['pairs_id', 'operation'], right_on=['pairs_id', 'operation'], how='outer')
    if not os.path.exists("./trading_compare"):
        os.mkdir("./trading_compare")
    df = df.sort_index().sort_index(axis=1).fillna("0")
    # Reset index for df to fit pandasModel
    df = df.reset_index()
    df = df.set_index("pairs_id")
    print(df)
    return df