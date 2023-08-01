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
from utils.const import ROOT_PATH

all = ["to_tmpvalue"]
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
                    print(filling_table)
                    part_1 = filling_table[['code1', 'code1_vol']].groupby('code1',as_index=False).aggregate({"code1":"first", "code1_vol":"sum"})
                    part_2 = filling_table[['code2', 'code2_vol']].groupby('code2',as_index=False).aggregate({"code2":"first", "code2_vol":"sum"})             
                    current_holding = pd.merge(left=part_1, right=part_2, left_on='code1', right_on='code2', how='outer')
                    current_holding[['code1_vol', 'code2_vol']] = current_holding[['code1_vol', 'code2_vol']].fillna(0)
                    current_holding[['code1', 'code2'] ] = current_holding[['code1', 'code2']].fillna(method='bfill',axis=1).fillna(method='ffill',axis=1)
                    current_holding['vol'] = current_holding['code1_vol'] + current_holding['code2_vol']
                    current_holding = current_holding[['code1', 'vol']]
                    current_holding.columns = ['code', 'vol']
                    print("---")
                    print(current_holding)
                    # 根据diff的值做变换
                    diff = pd.merge(left=target, right=current_holding, on='code', how='outer')
                    diff[['current', 'vol']] = diff[['current', 'vol']].fillna(0)
                    diff['delta'] = diff['current'] - diff['vol']
                    diff = diff[diff['delta']!=0].sort_values('code')
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

                    target_code = diff.iloc[0]['code']
                    delta_vol = diff.iloc[0]['delta']
                    # 根据code在参数表中的位置修改值
                    if target_code in set(filling_table['code1'].values):
                        filling_table['code1_vol'].loc[filling_table[filling_table['code1'] == target_code].iloc[0]['index']] += delta_vol
                        filling_table['code2_vol'].loc[filling_table[filling_table['code1'] == target_code].iloc[0]['index']] -= delta_vol
                    elif target_code in set(filling_table['code2'].values):
                        filling_table['code2_vol'].loc[filling_table[filling_table['code2'] == target_code].iloc[0]['index']] += delta_vol
                        filling_table['code1_vol'].loc[filling_table[filling_table['code2'] == target_code].iloc[0]['index']] -= delta_vol
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


if __name__ == "__main__":
    to_tmpvalue("lq")