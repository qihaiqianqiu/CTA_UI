import re
import os
import datetime
import time
import pandas as pd
import numpy as np
import networkx as nx
import matplotlib.pyplot as plt
pd.options.mode.chained_assignment=None

trade_day = [20220809, 20220810, 20220811, 20220812, 20220815, 20220816, 20220817, 20220818, 20220819, 20220822, 20220823, 20220824, 
             20220825, 20220826, 20220829, 20220830, 20220831, 20220901, 20220902, 20220905, 20220906, 20220907, 20220908, 20220909, 
             20220913, 20220914, 20220915, 20220916, 20220919, 20220920, 20220921, 20220922, 20220923, 20220926, 20220927, 20220928,
             20220929, 20220930, 20221010, 20221011, 20221012, 20221013, 20221014, 20221017, 20221018, 20221019, 20221020, 20221021, 
             20221024, 20221025, 20221026, 20221027, 20221028, 20221031, 20221101, 20221102, 20221103, 20221104, 20221107, 20221108, 
             20221109, 20221110, 20221111, 20221114, 20221115, 20221116, 20221117, 20221118, 20221121, 20221122, 20221123, 20221124, 
             20221125, 20221128, 20221129, 20221130, 20221201, 20221202, 20221205, 20221206, 20221207, 20221208, 20221209, 20221212, 
             20221213, 20221214, 20221215, 20221216, 20221219, 20221220, 20221221, 20221222, 20221223, 20221226, 20221227, 20221228, 
             20221229, 20221230, 20230103, 20230104, 20230105, 20230106, 20230109, 20230110, 20230111, 20230112, 20230113, 20230116, 20230117, 20230118, 20230119, 20230120, 20230130, 20230131, 20230201, 20230202, 20230203, 20230206, 20230207, 20230208, 20230209, 20230210, 20230213, 20230214, 20230215, 20230216, 20230217, 20230220, 20230221, 20230222, 20230223, 20230224, 20230227, 20230228, 20230301, 20230302, 20230303, 20230306, 20230307, 20230308, 20230309, 20230310, 20230313, 20230314, 20230315, 20230316, 20230317, 20230320, 20230321, 20230322, 20230323, 20230324, 20230327, 20230328, 20230329, 20230330, 20230331, 20230403, 20230404, 20230406, 20230407, 20230410, 20230411, 20230412, 20230413, 20230414, 20230417, 20230418, 20230419, 20230420, 20230421, 20230424, 20230425, 20230426, 20230427, 20230428, 20230502, 20230503, 20230504, 20230505, 20230508, 20230509, 20230510, 20230511, 20230512, 20230515, 20230516, 20230517, 20230518, 20230519, 20230522, 20230523, 20230524, 20230525, 20230526, 20230529, 20230530, 20230531, 20230601, 20230602, 20230605, 20230606, 20230607, 20230608, 20230609, 20230612, 20230613, 20230614, 20230615, 20230616, 20230619, 20230620, 20230621, 20230623, 20230626, 20230627, 20230628, 20230629, 20230630, 20230703, 20230704, 20230705, 20230706, 20230707, 20230710, 20230711, 20230712, 20230713, 20230714, 20230717, 20230718, 20230719, 20230720, 20230721, 20230724, 20230725, 20230726, 20230727, 20230728, 20230731, 20230801, 20230802, 20230803, 20230804, 20230807, 20230808, 20230809, 20230810, 20230811, 20230814, 20230815, 20230816, 20230817, 20230818, 20230821, 20230822, 20230823, 20230824, 20230825, 20230828, 20230829, 20230830, 20230831, 20230901, 20230904, 20230905, 20230906, 20230907, 20230908, 20230911, 20230912, 20230913, 20230914, 20230915, 20230918, 20230919, 20230920, 20230921, 20230922, 20230925, 20230926, 20230927, 20230928, 20231006, 20231009, 20231010, 20231011, 20231012, 20231013, 20231016, 20231017, 20231018, 20231019, 20231020, 20231023, 20231024, 20231025, 20231026, 20231027, 20231030, 20231031, 20231101, 20231102, 20231103, 20231106, 20231107, 20231108, 20231109, 20231110, 20231113, 20231114, 20231115, 20231116, 20231117, 20231120, 20231121, 20231122, 20231123, 20231124, 20231127, 20231128, 20231129, 20231130, 20231201, 20231204, 20231205, 20231206, 20231207, 20231208, 20231211, 20231212, 20231213, 20231214, 20231215, 20231218, 20231219, 20231220, 20231221, 20231222, 20231225, 20231226, 20231227, 20231228, 20231229]


boundary_dict = {'SC': 0.1,
                'FU': 1,
                'LU': 1,
                'BU': 1,
                'TA': 2,
                'EG': 1,
                'PF': 2,
                'CF': 5,
                'L': 1,
                'V': 1,
                'PP': 1,
                'RU': 5,
                'NR': 5,
                'PG': 1,
                'MA': 1,
                'EB': 1,
                'SA': 1,
                'FG': 1,
                'SP': 2,
                'UR': 1,
                'A': 1,
                'C': 1,
                'CS': 1,
                'M': 1,
                'RM': 1,
                'SR': 1,
                'OI': 1,
                'Y': 2,
                'P': 2,
                'AP': 1,
                'CJ': 5,
                'JD': 1,
                'SF': 2,
                'SM': 2,
                'RB': 1,
                'I': 0.5,
                'HC': 1,
                'J': 0.5,
                'JM': 0.5,
                'ZC': 0.2,
                'AG': 1,
                'AU': 0.02,
                'CU': 10,
                'ZN': 5,
                'AL': 5,
                'PB': 5,
                'SS': 5,
                'NI': 10,
                'SN': 10,
                'IF': 0.2,
                'IH': 0.2,
                'IC': 0.2,
                'IM' : 0.2,
                'CY': 5}


# Contract name from upper case and %ccdddd
def rename(contract:str):
    code = re.search("[a-zA-Z]+", contract).group(0)
    figure = re.search("[0-9]+", contract).group(0)
    code_mapping = {'原油': 'sc', '燃料油': 'fu', '低硫燃料油': 'lu', '沥青': 'bu', 'PTA': 'TA', 'MEG':'eg', '短纤': 'PF',\
           '棉花': 'CF', 'L':'l', 'V':'v', 'PP': 'pp', '橡胶': 'ru', '20号胶': 'nr', '液化石油气': 'pg', '甲醇': 'MA',\
           '苯乙烯': 'eb', '纯碱': 'SA', '玻璃': 'FG', '纸浆': 'sp', '尿素': 'UR', '豆一': 'a', '玉米': 'c', '玉米淀粉': 'cs',\
           '豆粕': 'm', '菜粕': 'RM', '白糖': 'SR', '菜油': 'OI', '豆油': 'y', '棕榈油': 'p', '苹果': 'AP', '红枣': 'CJ',\
           '鸡蛋': 'jd', '硅铁': 'SF', '锰硅': 'SM', '螺纹': 'rb', '铁矿': 'i', '热卷': 'hc', '焦炭': 'j', '焦煤': 'jm',\
           '动力煤': 'ZC', '白银':'ag', '黄金': 'au', '铜': 'cu', '锌': 'zn', '铝': 'al', '铅': 'pb', '不锈钢': 'ss', '镍': 'ni',\
           '锡': 'sn', 'IF': 'IF', 'IH': 'IH', 'IC': 'IC', 'IM': 'IM', '棉纱': 'CY'}
    counter = 0
    for key, values in code_mapping.items():
        if values == code:
            if code not in ["IF","IH","IC","IM"]:
                figure = figure[1:]
        else:
            counter += 1
    if counter == len(code_mapping):
        code = code.lower()
    
    return code + figure

# Get current date + section in a predict format
def get_date_section():
    today = int(datetime.date.today().strftime('%Y%m%d'))
    # Choose trading section to upload
    HMtime = time.strftime("%H:%M", time.localtime())
    print("Current time:", HMtime)
    # Morning Market
    if (HMtime > '02:30') & (HMtime < '11:30'):
        section = 1
    # Afternoon Market
    elif (HMtime > '11:30') & (HMtime < '15:00'):
        section = 2
    # Evening Market
    else:
        today = trade_day[trade_day.index(today) + 1]
        section = 0
    return today, section


# params参数表预测模块
# 输入 期望预测的交易日+单元 - 模型需要的是相邻前一交易单元
def from_predict(predict_date:int, predict_section:int):
    if predict_section == 1 or predict_section == 2:
        section = predict_section - 1
        date = predict_date
    else:
        section = 2
        date = trade_day[trade_day.index(predict_date) - 1]
    return date, section

def to_predict(date:int, section:int):
    if section == 0 or section == 1:
        predict_section = section + 1
        predict_date = date
    if section == 2:
        predict_section = 0
        predict_date = trade_day[trade_day.index(date) + 1]
    return predict_date, predict_section


def transform(df):
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
    order = ['pairs_id','indate_date', 'first_instrument', 'second_instrument', 'prime_instrument', 'boundary_unit_num', \
        'region_drift', 'region_0', 'region_1', 'region_2', 'region_3', 'region_4', 'region_5', 'region_6', 'region_7', \
        'up_boundary_5', 'up_boundary_4', 'up_boundary_3', 'up_boundary_2', 'up_boundary_1', 'down_boundary_1', \
        'down_boundary_2', 'down_boundary_3', 'down_boundary_4', 'down_boundary_5', 'today_fee', 'wait_window', \
        'favor_times', 'unfavor_times', 'abs_threshold', 'boundary_tick_lock', 'wait2_windows', 'after_tick', 'night_type',\
        'if_add', 'limitcoef', 'abs_threshold_after', 'kind', 'before_tick', 'before_cancel_flag', 'before_cancel_num',\
        'max_position', 'min_position', 'region_tick_lock', 'region_unit_num']
    df = df[order]
    return df

# 检查是否已缓存
# 流程：检查文件名 -> 检查region_info内容:{根据套利对检查boundary_info并重算boundary, 根据region_info重算region, 写csv}
def check_cache(account_name:str, date:int, section:int, pairs:pd.Series, args:tuple):
    # 检查文件名
    date, section = to_predict(date, section)
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
    date, section = to_predict(date, section)
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


# 以下为TmpValue更新模块
def get_param_pairs(acc_name):
    paramFileDir = os.path.join("./params", acc_name)
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

def get_hold(acc_name):
    ### 读取hold
    reportDir = os.path.join("./report")
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

          
def fix_tmp_value(acc_name):
    # Path configurations
    TmpValueDir = os.path.join("./TmpValue")
    TmpValueFileDir = os.path.join(TmpValueDir, acc_name)
    if not os.path.exists(TmpValueFileDir):
        os.mkdir(TmpValueFileDir)
    TmpValueFile = os.path.join(TmpValueFileDir, "TmpValue.csv")

    # 加载参数表，持仓
    param_df = get_param_pairs(acc_name)
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

def gap_refill(gap_df):
    pass

# 以下为持仓核算模块         
def export_holdings_compare(acc_lst):
    tmpvalue_dict = {}
    for acc_name in acc_lst:
        TmpValueDir = os.path.join("./TmpValue")
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
        df.to_csv(os.path.join("./holding_compare", str(datetime.date.today()) + '_holding_compare.csv'), index=False)
    except UnboundLocalError as e:
        print("未选中账户！")
    return df


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


def raw_pairing(df, pairing, sec_interval=0, max_interval=180):
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
        return pairing ,single_df
    else:
        return raw_pairing(single_df, pairing, sec_interval+1)
  

def parse(df):
    pairs = []
    # single_df deprecated
    single_df = pd.DataFrame()
    # 4秒间隔起-粗配对
    pairs, single_df = raw_pairing(df, pairs)
    #print(pairs)

    """
    # 落单交易记录-细配对
    if len(single_df) > 0:
        print(single_df)
        print("***")
        for i in range(len(single_df)):
            buffer = []
            index_buffer = []
            time_buffer = []
            single_record = single_df.iloc[i]
            target_breed = single_record['breed']
            target_time = single_record['time']
            for j in range(len(pairs)):
                if pairs[j]['breed'].unique()[0] == target_breed:
                    index_buffer.append(j)
                    buffer.append(pairs[j])
            for k in range(len(buffer)):
                time_diff = abs((buffer[k].iloc[0]['time'] - target_time).total_seconds())
                time_buffer.append(time_diff)
            # 对每一个record，寻找已配对记录中时间最接近的同品种交易对记录
            if len(time_buffer) > 0:
                target_idx = index_buffer[time_buffer.index(min(time_buffer))]
                target_slice = pairs[target_idx].copy(deep=True)
                target_slice = combine(pd.concat([target_slice, pd.DataFrame(single_record).T]))
                del pairs[target_idx]
            else:
                target_slice = combine(pd.DataFrame(single_record).T)
            pairs.append(target_slice)
    """
    for item in pairs:
        item['price'] = item['count'] / item['deal']
    return pairs


def match(df, buffer, param):
    print(df)
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
        pair = (near + '-' + forward_dt).upper()
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
    tradeFileDir = os.path.join("./tradings")
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

# 日成交对比
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
        tradingDir = os.path.join("./tradings")
        tradingFileDir = os.path.join(tradingDir, acc_name)
        # selected most recent trading record
        recent_date = "0"
        for filename in os.listdir(tradingFileDir):
            if '_sorted' in filename:
                dt = re.findall(r"\d+", filename)[0]
                if dt > recent_date:
                    recent_date = dt
        most_recent_tradingFile = "成交记录_" + str(recent_date) + "_sorted.csv"
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
    df.to_csv(os.path.join("./trading_compare", str(datetime.date.today()) + '_day_compare.csv'), encoding='GBK')
    return df
        

if __name__ == "__main__":
    #export_trading_compare(["co5", "jxh", "test"])
    fix_trade_record("co5")
    #fix_tmp_value("test")

"""
        if counter == 0:
            df = temp
            counter = 1
        else:
            df = pd.merge(left=df, right=temp, on='pairs_id', how='outer')
    try:
        df = df.fillna('Null').sort_values(by='pairs_id')
    except UnboundLocalError as e:
        print("未选中账户！")
    return df
"""
"""
# Deprecated
# 持仓核算仅由report生成 得出的记录是客观配对而成
# 与TmpValue中的主观使用的交易对不完全相同，但可以准确配对持仓记录
def recursive_match(df, pairlst):
    # Recursive process
    long = df[df['holding'] > 0]
    short = df[df['holding'] < 0]
    legging = []
    # Exit status
    if len(short) == 0 or len(long) == 0:
        df = pd.concat([long, short])
        # df为空则完美匹配 非空为瘸腿
        if len(df) != 0:
            for i in range(len(df)):
                contract_pair = df.iloc[i]['code'].upper()
                volume = df.iloc[i]['holding']
                # 瘸腿导出单独的日志
                legging.append([contract_pair, volume])
        return legging
    
    else:
        long_code = long.iloc[0]
        short_code = short.iloc[0]
        # constract pairing string
        contract_pair = long_code['breed'] + long_code['dt'] + '-' + short_code['dt']
        volume = min(abs(long_code['holding']), abs(short_code['holding']))
        new_long_holding = long_code['holding'] - volume
        new_short_holding = short_code['holding'] + volume
        long.iloc[0,2] = new_long_holding
        short.iloc[0,2] = new_short_holding
        # Concat back into df
        df = pd.concat([long,short])
        df = df[df['holding'] != 0]
        pairlst.append([contract_pair, volume])
        # Recursive call
        return recursive_match(df, pairlst)

        
def match(df):
    pairs = []
    legging = []
    for breed_grp in df.groupby('breed'):
        breed_df = breed_grp[1]
        breed_df = breed_df.sort_values(by=['code'])
        legging += recursive_match(breed_df, pairs)
    res = pd.DataFrame(pairs, columns=['contract_pair', 'volume'])

    return res, legging


def hold_preprocessing(df):
    df = df[['持仓合约', '买卖', '总仓']]
    df.columns = ['code', 'direction', 'holding']
    df['holding'] = df['holding'].astype('int')
    df['code'] = df['code'].apply(lambda x: x.strip(' '))
    df['direction'] = df['direction'].apply(lambda x: x.strip(' '))
    df['breed'] = df['code'].apply(lambda x:re.sub("[\u4e00-\u9fa5\0-9\,\。]", "", x).upper())
    df['direction'] = df['direction'].apply(lambda x: -1 if '卖' in x else 1)
    df['holding'] = df['holding'] * df['direction']
    df = df[df['holding'] != 0]
    df = df[["code", "breed", "holding"]].groupby('code',as_index=False).aggregate({"code":"first", "breed":"first", "holding":"sum"})
    df['dt'] = df['code'].apply(lambda x:re.findall(r"\d+.?\d*",x)[0].replace(' ',''))
    df['dt'] = df['dt'].apply(lambda x:'2'+x if len(x) < 4 else x)
    return df


def export_holdings():
    acc_name_lst = [name[7:][:-4] for name in os.listdir("./reports")]
    report_lst = [os.path.join("./reports", f) for f in os.listdir("./reports")]
    df_lst = [pd.read_csv(f, encoding='gbk') for f in report_lst]
    legging_log = datetime.date.today().strftime('%Y%m%d') + '\n'
    for i in range(len(df_lst)):
        holding, legging = match(hold_preprocessing(df_lst[i]))
        holding.to_csv("./holdings/持仓_"  + datetime.date.today().strftime('%Y%m%d') + "_" + acc_name_lst[i] + ".csv", index=False)
        holding = holding.rename(columns={"volume":acc_name_lst[i]})
        legging_log += acc_name_lst[i] + ":\n"
        for leg in legging:
            legging_log += str(leg) + '\n'
        legging_log += '\n'
        if i == 0:
            whole_df = holding
        if i >= 1:
            whole_df = whole_df.merge(right=holding, on="contract_pair", how="outer")
    whole_df = whole_df.fillna("Null")
    whole_holding_name = "./holdings/持仓_" + datetime.date.today().strftime('%Y%m%d') + "_whole.csv"
    whole_df.to_csv(whole_holding_name, index=False)
    f = open('./holdings/瘸腿_' + datetime.date.today().strftime('%Y%m%d') + '.txt', 'w')
    f.write(legging_log)
"""