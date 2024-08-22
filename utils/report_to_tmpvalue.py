"""
由持仓和参数表导出对应报告期TmpValue
当一对合约间存在两条Path，解析会存在唯一性问题
2023-05-04 更新：修正参数表中存在反套合约会影响正确计算的问题
"""
import os
import pandas as pd
import networkx as nx
import datetime
import re
from utils.compare import get_param_pairs, get_hold
from utils.const import ROOT_PATH, double_gap, stock_future

all = ["to_tmpvalue"]

def generate_dt_by_gap(start, end, gap):
    start_dt = int(start)
    end_dt = int(end)
    dt_lst = []
    while start_dt <= end_dt:
        dt_lst.append(str(start_dt))
        if start_dt % 100 == 12:
            start_dt += 88
            start_dt += gap
        else:
            start_dt += gap
    return dt_lst

def to_tmpvalue(acc_name):
    # Output Path configurations
    TmpValueDir = os.path.join(ROOT_PATH, "TmpValue")
    TmpValueFileDir = os.path.join(TmpValueDir, acc_name)
    if not os.path.exists(TmpValueFileDir):
        os.mkdir(TmpValueFileDir)
    TmpValueFile = os.path.join(TmpValueFileDir, "TmpValue.csv")
    
    hold = get_hold(acc_name)
    try:
        hold.drop(columns=['stocking_delta'], inplace=True)
    except KeyError:
        pass
    if hold.empty:
        print("持仓为空！")
        return pd.DataFrame()
    hold['dt'] = hold['code'].apply(lambda x:re.findall(r"\d+",x)[0])
    appendix_col = []
    for temp in hold.groupby('product'):
        breed = temp[0]
        if breed not in double_gap:
            gap = 1
        else:
            gap = 2
        holding_data = temp[1].sort_values('dt')
        if breed not in stock_future:
            dt_list = holding_data['dt'].tolist()
            dt_min = dt_list[0]
            dt_max = dt_list[-1]
            dt_lst = generate_dt_by_gap(dt_min, dt_max, gap)
            code_lst = [breed + dt for dt in dt_lst]
            product_lst = [breed for i in range(len(code_lst))]
            current_lst = [0 for i in range(len(code_lst))]
            placeholder_cols = pd.DataFrame({'code':code_lst, 'current':current_lst, 'product':product_lst, 'dt':dt_lst})
            appendix_col.append(placeholder_cols)
    appendix_col = pd.concat(appendix_col)
    appendix_col = appendix_col[~appendix_col['code'].isin(hold['code'])]
    hold = pd.concat([hold, appendix_col], axis=0, join='outer').sort_values('code').reset_index(drop=True)
    tmpvalue_collection = {}
    #hold.to_csv("test.csv", index=False)
    for idx in range(len(hold) - 1):
        current_row = hold.iloc[idx]
        next_row = hold.iloc[idx + 1]
        if int(current_row['current']) != 0 and current_row['product'] == next_row['product']:
            pair_name = current_row['code'] + '-' + next_row['dt']
            pair_vol = current_row['current']
            hold.at[idx + 1, 'current'] += current_row['current']
            hold.at[idx, 'current'] = 0
            tmpvalue_collection[pair_name] = pair_vol
    print(tmpvalue_collection)
    tmpvalue = pd.DataFrame(tmpvalue_collection.items(), columns=['pair', 'current'])
    tmpvalue['place_holder_1'] = 0
    tmpvalue['place_holder_2'] = 0
    tmpvalue['place_holder_3'] = 20230200000000
    tmpvalue.sort_values('pair')
    tmpvalue.to_csv(TmpValueFile, header=None, index=False)
    gap = hold[hold['current'] != 0]
    gap.rename(columns={"pair":"code", "current":"gap"}, inplace=True)
    return gap
            

                
                
                
    
"""    
# 对params里的套利对进行连同图分析，找出入度大于1的套利对
def fix_param_pairs(param_df, hold):
    issue_node = []
    for temp in param_df.groupby('product'):
        code_lst = set(temp[1]['code1'].tolist() + temp[1]['code2'].tolist())
        edge_lst = temp[1][['code1','code2']].values.tolist()
        DG = nx.DiGraph()
        for node in code_lst:
            DG.add_node(node)
        for link in edge_lst:
            DG.add_edge(link[0], link[1])
        issue_node += [i[0] for i in DG.in_degree(DG.nodes) if i[1] > 1]
    # 对存在唯一性问题的合约，在持仓的，打上标签
    exclude_node = [code for code in issue_node if code in hold['code'].values]
    param_df['flag'] = param_df.apply(lambda x: True if x['code2'] in exclude_node else False, axis=1)
    return param_df


def to_tmpvalue(acc_name):
    # Path configurations
    TmpValueDir = os.path.join(ROOT_PATH, "TmpValue")
    TmpValueFileDir = os.path.join(TmpValueDir, acc_name)
    if not os.path.exists(TmpValueFileDir):
        os.mkdir(TmpValueFileDir)
    TmpValueFile = os.path.join(TmpValueFileDir, "TmpValue.csv")

    # 加载参数表，持仓
    param_df = get_param_pairs(acc_name)
    # 去除反套
    param_df['reverse_flag'] = param_df.apply(lambda x: 1 if x['code1'] > x['code2'] else 0, axis=1)
    param_df = param_df[param_df['reverse_flag'] == 0]
    param_df = param_df.drop(columns=['reverse_flag'])

    hold = get_hold(acc_name)
    param_df = fix_param_pairs(param_df, hold)
     
    tmpvalue = pd.DataFrame()
    legging = []
    
    for holding in hold.groupby('product'):
        flag = False
        if not holding[0] in set(param_df['product'].values):
            #写异常日志
            log = "未包含在参数表中的合约_0：\n"
            log += str(holding[1].values)
            legging.append(log)
            
        for params in param_df.groupby('product'):
            # 每个品种逐一配对
            if params[0] == holding[0]:
                # 判断该品种是否存在唯一性问题
                if len(params[1][params[1]['flag']==True]) > 0:
                    flag = True
                counter = 0
                cycle_pairs = []
                print("当前遍历品种：" + holding[0])
                # 每次更新params中对应套利对形成的填充表
                filling_table = params[1]
                target = holding[1]
                tmpvalue_df = pd.DataFrame()
                # 持仓不在参数表中的
                print("****")
                holding_code = [code for code in target['code'].values]
                param_code = filling_table['code1'].values.tolist() + filling_table['code2'].values.tolist()
                external_code = [code for code in holding_code if code not in param_code]
                if len(external_code) > 0:
                    log = "未包含在参数表中的合约_1：\n"
                    for code in external_code:
                        log += str(hold[hold['code'] == code].values) + '\n'
                        target = target[target['code'] != code]
                    legging.append(log)

                while True:
                    # 统计params中的品种来计算diff
                    # 尝试使用loc优化计算速度
                    print("填充表：")
                    print(filling_table)
                    part_1 = filling_table.loc[:,('code1', 'code1_vol')].groupby('code1',as_index=False).aggregate({"code1":"first", "code1_vol":"sum"})
                    part_2 = filling_table.loc[:,('code2', 'code2_vol')].groupby('code2',as_index=False).aggregate({"code2":"first", "code2_vol":"sum"})             
                    current_holding = pd.merge(left=part_1, right=part_2, left_on='code1', right_on='code2', how='outer')
                    current_holding.loc[:,('code1_vol', 'code2_vol')] = current_holding.loc[:,('code1_vol', 'code2_vol')].fillna(0)
                    current_holding.loc[:,('code1', 'code2')] = current_holding.loc[:,('code1', 'code2')].fillna(method='bfill',axis=1).fillna(method='ffill',axis=1)
                    current_holding['vol'] = current_holding['code1_vol'] + current_holding['code2_vol']
                    current_holding = current_holding[['code1', 'vol']]
                    current_holding.columns = ['code', 'vol']
                    print("已填充仓位：")
                    print(current_holding)
                    # 根据diff的值做变换
                    diff = pd.merge(left=target, right=current_holding, on='code', how='outer')
                    diff.loc[:,('current', 'vol')] = diff.loc[:,('current', 'vol')].fillna(0)
                    diff['delta'] = diff['current'] - diff['vol']
                    diff = diff[diff['delta']!=0].sort_values('code')
                    print("仓差：")
                    print(diff)

                    if counter >= 30:
                        # 循环配对
                        log = "未包含在参数表中的合约_2：\n"
                        log += str(diff[['code', 'delta']].values) + '\n'
                        legging.append(log)
                        tmpvalue = pd.concat([tmpvalue, filling_table[['pairs_id','code1_vol']]])
                        break

                    if len(diff) == 1:
                        #瘸腿
                        log = "瘸腿合约：\n"
                        log += str(diff[['code', 'delta']].values)
                        legging.append(log)
                        tmpvalue_df = pd.concat([tmpvalue_df, filling_table[['pairs_id','code1_vol']]])
                        break

                    if len(diff) == 0:
                        tmpvalue_df = pd.concat([tmpvalue_df, filling_table[['pairs_id','code1_vol']]])
                        break
                    # 判断此时的target和上一次是否重合，如果重合则说明进入死循环
                    target_code = diff.iloc[0]['code']
                    if counter >= 2:
                        if target_code == code_before_previous:
                            # 出现死循环,在以后的计算中不再考虑循环结的套利对
                            if target_code in set(filling_table['code1'].values):
                                cycle_pair = filling_table[filling_table['code1'] == target_code].iloc[0]['pairs_id']
                            elif target_code in set(filling_table['code2'].values):
                                cycle_pair = filling_table[filling_table['code2'] == target_code].iloc[0]['pairs_id']
                            # 锁定导致死循环的套利对 - 这个套利对实际上已经完全填充完毕，不应该再对其进行修改
                            if cycle_pair not in cycle_pairs:
                                cycle_pairs.append(cycle_pair)
                            print("死循环：", cycle_pair)                 
                    delta_vol = diff.iloc[0]['delta']
                    if counter >= 1:
                        code_before_previous = previous_code
                    previous_code = target_code
                    # 根据code在参数表中的位置修改值
                    # SettingWithCopy Warning
                    print("checkpoint_filling_table:")
                    excluded_table = filling_table.loc[~filling_table['pairs_id'].isin(cycle_pairs)]
                    print(excluded_table)
                    print("checkpoint_cycle_pairs:")
                    print(cycle_pairs)
                    if target_code in set(excluded_table['code1'].values):
                        filling_table.loc[excluded_table[excluded_table['code1'] == target_code].iloc[0]['index'], 'code1_vol'] += delta_vol
                        filling_table.loc[excluded_table[excluded_table['code1'] == target_code].iloc[0]['index'], 'code2_vol'] -= delta_vol
                    elif target_code in set(excluded_table['code2'].values):
                        filling_table.loc[excluded_table[excluded_table['code2'] == target_code].iloc[0]['index'], 'code2_vol'] += delta_vol
                        filling_table.loc[excluded_table[excluded_table['code2'] == target_code].iloc[0]['index'], 'code1_vol'] -= delta_vol
                    else:
                        # 写异常日志
                        log = "未包含在参数表中的合约_3：\n"
                        log += str(hold[hold['code'] == target_code].values)
                        legging.append(log)
                        diff = diff[diff['code'] != target_code]
                        target = target[target['code'] != target_code]
                        
                    counter += 1
                    
                # 触发唯一性问题
                flag = False
                if flag:
                    # 写异常日志
                    log = "存在唯一性问题的合约\n"
                    log += str(tmpvalue_df.values)
                    legging.append(log)
                    # print("唯一性问题:")
                    # print(filling_table)
                if not flag:
                    tmpvalue = pd.concat([tmpvalue, tmpvalue_df])

                break
                
    tmpvalue['place_holder_1'] = 0
    tmpvalue['place_holder_2'] = 0
    tmpvalue['place_holder_3'] = 20230200000000
    tmpvalue.sort_values('pairs_id')
    tmpvalue[tmpvalue['code1_vol']!=0].to_csv(TmpValueFile, header=None, index=False)
    print(legging)
    # 导出异常日志
    if len(legging) > 0:
        leggingFileName = '异常_' + datetime.date.today().strftime('%Y%m%d') + '.txt'
        leggingFile = os.path.join(TmpValueFileDir , leggingFileName)
        legging_log = datetime.date.today().strftime('%Y%m%d') + '\n'
        for leg in legging:
            legging_log += str(leg) + '\n'
        f = open(leggingFile, 'w')
        f.write(legging_log)

    # 核算TmpValue与report的差距
    tmpvalue.columns = ['pair','current','long','short','timestamp']
    # code-1 近月合约 code-2 远月合约
    # 拆分TmpValue中的配对
    tmpvalue['code1'] = tmpvalue['pair'].apply(lambda x:x[:-9]+x[-9:-5])
    tmpvalue['code2'] = tmpvalue['pair'].apply(lambda x:x[:-9]+x[-4:])
    long = tmpvalue[['code1','current']]
    long.columns = ['code','current']
    short = tmpvalue[['code2','current']]
    short.columns = ['code','current']
    # 买近卖远 近月*1 远月*-1
    long = long.groupby('code')['current'].sum()
    short = short.groupby('code')['current'].sum()*-1
    res = pd.DataFrame(pd.concat([long,short]))
    res = pd.DataFrame(res.groupby('code')['current'].sum())
    res = res.reset_index()
    res['product'] = res['code'].apply(lambda x:x[:-4])

    ### 合并tmp和hold
    # Report 为 real 与 TmpValue中的值核对
    result = pd.merge(res,hold[['code','current']].rename(columns={'current':'real'}),on='code',how='outer')
    # print("result:", result)
    result['product'] = result['code'].apply(lambda x:re.sub("[\u4e00-\u9fa5\0-9\,\。]", "", x).upper())
    result = result.fillna(0)
    result['gap'] = result['real'] - result['current']

    ### 查看各合约的差异
    # 筛选出TmpValue和report持仓量有出入的合约
    code_gap = result.groupby('code')['gap'].sum()
    code_gap = code_gap[code_gap!=0] 
    if len(code_gap) > 0:
        print("TmpValue与report相差的：")
        print(code_gap)
    return code_gap
"""

if __name__ == "__main__":
    to_tmpvalue("HFT")