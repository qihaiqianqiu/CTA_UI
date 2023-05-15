import re
import os
import datetime
import time
import pandas as pd
import numpy as np
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
                'CY': 5}


# Contract name to upper case and %ccdddd
def rename(contract:str):
    code = re.search("[a-zA-Z]+", contract).group(0)
    figure = re.search("[0-9]+", contract).group(0)
    code_mapping = {'原油': 'sc', '燃料油': 'fu', '低硫燃料油': 'lu', '沥青': 'bu', 'PTA': 'TA', 'MEG':'eg', '短纤': 'PF',\
           '棉花': 'CF', 'L':'l', 'V':'v', 'PP': 'pp', '橡胶': 'ru', '20号胶': 'nr', '液化石油气': 'pg', '甲醇': 'MA',\
           '苯乙烯': 'eb', '纯碱': 'SA', '玻璃': 'FG', '纸浆': 'sp', '尿素': 'UR', '豆一': 'a', '玉米': 'c', '玉米淀粉': 'cs',\
           '豆粕': 'm', '菜粕': 'RM', '白糖': 'SR', '菜油': 'OI', '豆油': 'y', '棕榈油': 'p', '苹果': 'AP', '红枣': 'CJ',\
           '鸡蛋': 'jd', '硅铁': 'SF', '锰硅': 'SM', '螺纹': 'rb', '铁矿': 'i', '热卷': 'hc', '焦炭': 'j', '焦煤': 'jm',\
           '动力煤': 'ZC', '白银':'ag', '黄金': 'au', '铜': 'cu', '锌': 'zn', '铝': 'al', '铅': 'pb', '不锈钢': 'ss', '镍': 'ni',\
           '锡': 'sn', 'IF': 'IF', 'IH': 'IH', 'IC': 'IC', '棉纱': 'CY'}
    counter = 0
    for key, values in code_mapping.items():
        if values == code:
            if code not in ["IF","IH","IC"]:
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
    if (HMtime > '02:30') & (HMtime < '11:30'):
        section = 1
    elif (HMtime > '11:30') & (HMtime < '15:00'):
        section = 2
    else:
        today = trade_day[trade_day.index(today) + 1]
        section = 0
    return today, section

def transform(df):
    #print(df)
    # 常量列
    df = df.drop(columns=['reduce_date', 'clear_date'])
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
        'up_boundary_5', 'up_boundary_4', 'up_boundary_3', 'up_boundary_2',	'up_boundary_1', 'down_boundary_1', \
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

# 以下为TmpValue更新模块
def get_param_pairs(df):
    df = df[['pairs_id']]
    # code-1 近月合约 code-2 远月合约
    # 拆分配对
    df['code1'] = df['pairs_id'].apply(lambda x:x[:-9]+x[-9:-5])
    df['code2'] = df['pairs_id'].apply(lambda x:x[:-9]+x[-4:])
    
    return df
          
def fix_tmp_value(acc_name):
    # Path configurations
    TmpValueDir = os.path.join("./TmpValue")
    TmpValueFileDir = os.path.join(TmpValueDir, acc_name)
    if not os.path.exists(TmpValueFileDir):
        os.mkdir(TmpValueFileDir)
    TmpValueFile = os.path.join(TmpValueFileDir, "TmpValue.csv")
    
    reportDir = os.path.join("./report")
    reportFileDir = os.path.join(reportDir, acc_name)
    if not os.path.exists(reportFileDir):
        os.mkdir(reportFileDir)
    reportFile = os.path.join(reportFileDir, "report.csv")

    paramFileDir = os.path.join("./params", acc_name)
    paramFile = os.path.join(paramFileDir , "params.csv")

    ### 读取tmpvalue
    # 清洗tmpvalue原始文件
    data = pd.read_csv(TmpValueFile, header=None, names=['pair','current','long','short','timestamp'])
    data = data[data['pair']!='pair']
    data = data.groupby('pair').last()
    data = data[data['current']!='0']
    data = data[data['current']!=0]

    data = data.sort_index()
    data.to_csv(TmpValueFile,header=None)

    ### 计算tmp的各品种持仓
    data = data.reset_index()
    # code-1 近月合约 code-2 远月合约
    # 拆分TmpValue中的配对
    data['code1'] = data['pair'].apply(lambda x:x[:-9]+x[-9:-5])
    data['code2'] = data['pair'].apply(lambda x:x[:-9]+x[-4:])
    tmp_value_possible_pair = data[['pair','code1','code2']]
    tmp_value_possible_pair.columns = ['pairs_id', 'code1', 'code2']
    long = data[['code1','current']]
    long.columns = ['code','current']
    short = data[['code2','current']]
    short.columns = ['code','current']
    # 买近卖远 近月*1 远月*-1
    long = long.groupby('code')['current'].sum()
    short = short.groupby('code')['current'].sum()*-1
    res = pd.DataFrame(pd.concat([long,short]))
    res = res.reset_index()
    res = pd.DataFrame(res.groupby('code')['current'].sum())
    res = res.reset_index()
    res['product'] = res['code'].apply(lambda x:x[:-4])

    print("各合约持仓明细")
    print(res[res['product'] == 'BU'])
    print("各品种持仓是否多空抵消")
    print(res.groupby('product')['current'].sum())


    ### 计算风险度
    ### 读取hold
    hold = pd.read_csv(reportFile, encoding='gbk')
    if '持仓合约' and '总仓' in hold.columns.tolist():
        hold = hold[['持仓合约','买卖','总仓']]
    if '合约' and '总持仓' in hold.columns.tolist():    
        hold = hold[['合约', '买卖', '总持仓']]
    hold.columns = ['持仓合约','买卖','总仓']
    hold['a'] = hold['持仓合约'].apply(lambda x:x[:2])

    ### 计算hold的各品种持仓
    hold = hold[['持仓合约','买卖','总仓','a']]
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
    hold['current'] = np.where(hold['direction']=='买\u3000',hold['current'],-1*hold['current'])
    del hold['direction']
    print("持仓合约代码")
    print(hold['code']) 
    # 'a'列用来筛选合约
    # hold = hold[~hold['a'].isin(['IF','IH','IC','IO','MO'])]

    ## 计算report中的各品种持仓
    hold = pd.DataFrame(hold.groupby('code')['current'].sum())
    hold = hold.reset_index()
    hold['product'] = hold['code'].apply(lambda x:re.sub("[\u4e00-\u9fa5\0-9\,\。]", "", x).upper())
    print(hold.groupby('product')['current'].sum())
    

    ### 合并tmp和hold
    # Report 为 real 与 TmpValue中的值核对
    result = pd.merge(res,hold[['code','current']].rename(columns={'current':'real'}),on='code',how='outer')
    print("result:", result)
    result['product'] = result['code'].apply(lambda x:re.sub("[\u4e00-\u9fa5\0-9\,\。]", "", x).upper())
    result = result.fillna(0)
    # 'current'为 TmpValue -- 'real'为 report
    # 'current' 中的NA对应TmpValue中没有report里有的合约 那么这部分的合约仓位应该在 'real'中也是0 抵消掉了
    # 'real' 中的NA对应report里没有但是TmpValue里有的合约 这部分的合约仓位应该在'current'中也是0 比如 'AL2302-03', 'AL2303-04', 03就抵消掉了
    # 用0填充NA后可以计算出TmpValue和report中的某一合约的差距
    result['gap'] = result['current'] - result['real']

    ### 查看各品种的差异
    # 筛选出TmpValue和report持仓量有出入的品种
    product_gap = result.groupby('product')['gap'].sum()
    product_gap = product_gap[product_gap!=0]

    ### 查看各合约的差异
    # 筛选出TmpValue和report持仓量有出入的合约
    code_gap = result.groupby('code')['gap'].sum()
    code_gap = code_gap[code_gap!=0] 


    ### 生成tmp的调整目标
    # 缺少的仓位 目标反向补回 包含TmpValue中没有的合约
    tmpfix = pd.DataFrame(code_gap * -1).reset_index()
    print("修复目标")
    print(tmpfix)
    tmpfix['product'] = tmpfix['code'].apply(lambda x:re.sub("[\u4e00-\u9fa5\0-9\,\。]", "", x).upper())
    legging = []
    possible_pairs = get_param_pairs(pd.read_csv(paramFile))
    possible_pairs = pd.concat([possible_pairs, tmp_value_possible_pair])
    for product in list(set(tmpfix['product'])):
        counter = 0
        while True:
            long = data[['code1','current']]
            long.columns = ['code','current']
            short = data[['code2','current']]
            short.columns = ['code','current']
            long = long.groupby('code')['current'].sum()
            short = short.groupby('code')['current'].sum()*-1
            res = pd.DataFrame(pd.concat([long,short]))
            res = res.reset_index()

            res = pd.DataFrame(res.groupby('code')['current'].sum())
            res = res.reset_index()
            res['product'] = res['code'].apply(lambda x:x[:-4])

            ### 合并tmp和hold
            result = pd.merge(res,hold[['code','current']].rename(columns={'current':'real'}),on='code',how='outer')
            result['product'] = result['code'].apply(lambda x:re.sub("[\u4e00-\u9fa5\0-9\,\。]", "", x).upper())
            result = result.fillna(0)
            result['gap'] = result['current'] - result['real']

            ### 查看各品种的差异
            product_gap = result.groupby('product')['gap'].sum()
            product_gap = product_gap[product_gap!=0]
            # print(product_gap)

            ### 查看各合约的差异
            code_gap = result.groupby('code')['gap'].sum()
            code_gap = code_gap[code_gap!=0]
            # print(code_gap)

            ### 生成tmp的调整目标
            # 重新生成一遍全体待修正合约 提取对应TmpValue应补多空
            tmpfix = pd.DataFrame(code_gap * -1).reset_index()
            tmpfix['product'] = tmpfix['code'].apply(lambda x:re.sub("[\u4e00-\u9fa5\0-9\,\。]", "", x).upper())
            tmpfix1 = tmpfix[tmpfix['product']==product].reset_index(drop=True)
            print("迭代中的修正目标")
            print(tmpfix1)
            long_list = sorted(tmpfix1[tmpfix1['gap']>0]['code'].tolist())
            short_list = sorted(tmpfix1[tmpfix1['gap']<0]['code'].tolist())
            print(long_list)
            print(short_list)
            # Exit status
            if len(long_list) == 0 or len(short_list) == 0:
                if len(long_list) + len(short_list) == 0:
                    break
                else:
                    #瘸腿
                    leg_code_lst = long_list if len(long_list) > len(short_list) else short_list
                    for code in leg_code_lst:
                        leg_data = [code, tmpfix1[tmpfix1['code'] == code]['gap']]
                        legging.append(leg_data)
                    break
            # i 两种情况对应i出现在code1/code2的位置上
            for i in range(len(long_list)):
                temp = data[(data['code1'].isin([long_list[i]]) & data['code2'].isin(short_list)) | (data['code1'].isin(short_list) & data['code2'].isin([long_list[i]]))]
                # temp如果不为空，说明原TmpValue中存在可以直接修正的配对
                if len(temp) > 0:
                    if long_list[i] in temp['code2'].tolist():
                        ### 优先平掉做该合约到指定数量
                        temp = temp[temp['code2']==long_list[i]]
                        data['current'].loc[data[data['pair']==temp['pair'].iloc[0]].index[0]]-=tmpfix[tmpfix['code']==long_list[i]]['gap'].iloc[0]
                    else:
                        temp = temp[temp['code1']==long_list[i]]
                        data['current'].loc[data[data['pair']==temp['pair'].iloc[0]].index[0]]+=tmpfix[tmpfix['code']==long_list[i]]['gap'].iloc[0]
                # 如果为空，原TmpValue中不存在配对，需要从params中寻找对应的套利对加入
                else:
                    # 优先选择做空该合约的交易对, 如果没有则选择做多的
                    # 算法的角度，如果是优先做空，会把两个合约对的dt越拉越远，最终大概率循环迭代
                    # print(possible_pairs)
                    if counter <= 30:
                        selected_pairs = possible_pairs[possible_pairs['code2'] == long_list[i]]
                        flag = -1
                        if len(selected_pairs) == 0:
                            selected_pairs = possible_pairs[possible_pairs['code1'] == long_list[i]]
                            flag = 1
                    if counter > 30:
                        #print(possible_pairs[possible_pairs['code1'] == 'EG2309'])
                        selected_pairs = possible_pairs[possible_pairs['code1'] == long_list[i]]
                        flag = 1
                        if len(selected_pairs) == 0:
                            selected_pairs = possible_pairs[possible_pairs['code2'] == long_list[i]]
                            flag = -1
                    #print(selected_pairs)
                    if len(selected_pairs) > 0:
                        # Params中找到了对应套利对
                        new_current = tmpfix1[tmpfix1['code']==long_list[i]]['gap'].values[0] * flag
                        data = data.append({'pair':selected_pairs['pairs_id'].values[0],'current': new_current, 'code1':selected_pairs['code1'].values[0], 'code2':selected_pairs['code2'].values[0]}, ignore_index=True)
                    if len(selected_pairs) == 0 or counter > 120:
                        # Params & TmpValue中均不存在配对, 则强制配对并标记在瘸腿日志中
                        force_pair = long_list[i] + '-' + short_list[0][-4:]
                        force_volume = min(abs(tmpfix1[tmpfix1['code']==long_list[i]]['gap'].values[0]), abs(tmpfix1[tmpfix1['code']==short_list[0]]['gap'].values[0]))
                        data = data.append({'pair':force_pair, 'current': force_volume, 'code1':long_list[i], 'code2':short_list[0]}, ignore_index=True)
                        force_log = "强制撮合套利对：\n"
                        force_log += str(force_pair) + " Volume: " + str(force_volume)
                        legging.append(force_log)
                    data = data.fillna(method='ffill')
                counter += 1

    data = data.set_index('pair')[['current','long','short','timestamp']]
    data = data.groupby('pair').sum()
    data = data[data['current']!='0']
    data = data[data['current']!=0]
    data.to_csv(TmpValueFile, header=None)
    # 导出瘸腿日志
    if len(legging) > 0:
        leggingFileName = '异常_' + datetime.date.today().strftime('%Y%m%d') + '.txt'
        leggingFile = os.path.join(TmpValueFileDir , leggingFileName)
        legging_log = datetime.date.today().strftime('%Y%m%d') + '\n'
        for leg in legging:
            legging_log += str(leg) + '\n'
        f = open(leggingFile, 'w')
        f.write(legging_log)

# 以下为持仓核算模块
# 持仓核算仅由report生成 得出的记录是客观配对而成
# 与TmpValue中的主观使用的交易对不完全相同，但可以准确配对持仓记录
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
                