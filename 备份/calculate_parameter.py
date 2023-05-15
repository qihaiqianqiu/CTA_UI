# boundary: 一个完整的predict流程
from sklearn.linear_model import ElasticNet
from sklearn.linear_model import Lasso
from sklearn.linear_model import LinearRegression
import numpy as np
# Dependencies
import os, re, numpy as np, pandas as pd
import matplotlib.pyplot as plt
from pylab import mpl
mpl.rcParams['font.sans-serif'] = ['SimHei']
mpl.rcParams['axes.unicode_minus'] = False
from scipy.stats import norm
import matplotlib.dates as mdate
import datetime as dt
from time import *
from clickhouse_driver import Client
import utils
from tqdm.gui import tqdm_gui
import time


# 配置常量    
# Database config
para = {
    'ip' : '192.168.3.186',
    'port' : '9090',
    'pw' : '',
    'db_to' : 'cofund',
    'tb_to' : 'orderbook'
}

# Tradingday list
trade_day = [20220809, 20220810, 20220811, 20220812, 20220815, 20220816, 20220817, 20220818, 20220819, 20220822, 20220823, 20220824, 
            20220825, 20220826, 20220829, 20220830, 20220831, 20220901, 20220902, 20220905, 20220906, 20220907, 20220908, 20220909, 
            20220913, 20220914, 20220915, 20220916, 20220919, 20220920, 20220921, 20220922, 20220923, 20220926, 20220927, 20220928,
            20220929, 20220930, 20221010, 20221011, 20221012, 20221013, 20221014, 20221017, 20221018, 20221019, 20221020, 20221021, 
            20221024, 20221025, 20221026, 20221027, 20221028, 20221031, 20221101, 20221102, 20221103, 20221104, 20221107, 20221108, 
            20221109, 20221110, 20221111, 20221114, 20221115, 20221116, 20221117, 20221118, 20221121, 20221122, 20221123, 20221124, 
            20221125, 20221128, 20221129, 20221130, 20221201, 20221202, 20221205, 20221206, 20221207, 20221208, 20221209, 20221212, 
            20221213, 20221214, 20221215, 20221216, 20221219, 20221220, 20221221, 20221222, 20221223, 20221226, 20221227, 20221228, 
            20221229, 20221230, 20230103, 20230104, 20230105, 20230106, 20230109, 20230110, 20230111, 20230112, 20230113, 20230116, 
            20230117, 20230118, 20230119, 20230120, 20230130, 20230131, 20230201, 20230202, 20230203, 20230206, 20230207, 20230208, 
            20230209, 20230210, 20230213, 20230214, 20230215, 20230216, 20230217, 20230220, 20230221, 20230222, 20230223, 20230224, 
            20230227, 20230228, 20230301, 20230302, 20230303, 20230306, 20230307, 20230308, 20230309, 20230310, 20230313, 20230314, 
            20230315, 20230316, 20230317, 20230320, 20230321, 20230322, 20230323, 20230324, 20230327, 20230328, 20230329, 20230330, 
            20230331, 20230403, 20230404, 20230406, 20230407, 20230410, 20230411, 20230412, 20230413, 20230414, 20230417, 20230418, 
            20230419, 20230420, 20230421, 20230424, 20230425, 20230426, 20230427, 20230428, 20230502, 20230503, 20230504, 20230505, 
            20230508, 20230509, 20230510, 20230511, 20230512, 20230515, 20230516, 20230517, 20230518, 20230519, 20230522, 20230523, 
            20230524, 20230525, 20230526, 20230529, 20230530, 20230531, 20230601, 20230602, 20230605, 20230606, 20230607, 20230608, 
            20230609, 20230612, 20230613, 20230614, 20230615, 20230616, 20230619, 20230620, 20230621, 20230623, 20230626, 20230627, 
            20230628, 20230629, 20230630, 20230703, 20230704, 20230705, 20230706, 20230707, 20230710, 20230711, 20230712, 20230713, 
            20230714, 20230717, 20230718, 20230719, 20230720, 20230721, 20230724, 20230725, 20230726, 20230727, 20230728, 20230731, 
            20230801, 20230802, 20230803, 20230804, 20230807, 20230808, 20230809, 20230810, 20230811, 20230814, 20230815, 20230816, 
            20230817, 20230818, 20230821, 20230822, 20230823, 20230824, 20230825, 20230828, 20230829, 20230830, 20230831, 20230901, 
            20230904, 20230905, 20230906, 20230907, 20230908, 20230911, 20230912, 20230913, 20230914, 20230915, 20230918, 20230919, 
            20230920, 20230921, 20230922, 20230925, 20230926, 20230927, 20230928, 20231006, 20231009, 20231010, 20231011, 20231012, 
            20231013, 20231016, 20231017, 20231018, 20231019, 20231020, 20231023, 20231024, 20231025, 20231026, 20231027, 20231030, 
            20231031, 20231101, 20231102, 20231103, 20231106, 20231107, 20231108, 20231109, 20231110, 20231113, 20231114, 20231115, 
            20231116, 20231117, 20231120, 20231121, 20231122, 20231123, 20231124, 20231127, 20231128, 20231129, 20231130, 20231201, 
            20231204, 20231205, 20231206, 20231207, 20231208, 20231211, 20231212, 20231213, 20231214, 20231215, 20231218, 20231219, 
            20231220, 20231221, 20231222, 20231225, 20231226, 20231227, 20231228, 20231229]

# Clickhouse config
client = Client(host=para['ip'], port=para['port'])
client.execute('use cofund')
print("client init successfully.")

# Where parameter stored 
PARA_PATH = r".\info\boundary_info"

    
# Get data from start_date[MorningMarket] to end_date[EveningMarket]
def get_pairwise_data(contract_pair:list, start_date:int, end_date:int):
    import time
    start = time.time()
    select_clause = 'SELECT contract, trading_date, time, ap1, av1, bp1, bv1 from ctp_future_tick '
    where_clause = 'WHERE contract in ' + str(tuple(contract_pair)) + ' and trading_date >= '+ str(start_date) + ' and trading_date <= ' + str(end_date)
    query = select_clause + where_clause
    res = client.query_dataframe(query)
    print("SQL TIME: ", time.time()-start)
    if len(res) > 0:
        res = res.sort_values(by=['trading_date', 'time'])
        res['time'] = res['time'].apply(lambda x: x.split(':')[0] + ':' + x.split(':')[1] + ':' + x.split(':')[2] + ':000' if int(x.split(':')[3]) < abs(int(x.split(':')[3])-500) else x.split(':')[0] + ':' + x.split(':')[1] + ':' + x.split(':')[2] + ':500')

        # Inner join
        pair_data_0 = res[res['contract'] == contract_pair[0]]
        pair_data_1 = res[res['contract'] == contract_pair[1]]
        pair_data = pd.merge(pair_data_0, pair_data_1, how='inner', on=['trading_date','time'],suffixes=('_' + contract_pair[0], '_' + contract_pair[1]))
        pair_data.drop(columns=['contract_'+contract_pair[0], 'contract_'+contract_pair[1]], inplace=True)
        pair_data.sort_values(by=['trading_date', 'time'])
        pair_data = pair_data.fillna(method='ffill').replace([np.inf, -np.inf], np.nan).dropna()
    
        return pair_data
    
    else:
        return pd.DataFrame()
    

# 单一套利对，起止交易日之间全部早、午、夜市
def pair_plot_section(contract_pair:list, start_date:int, end_date:int):
    # Trading_section = [['9','11:30'], ['13:30','15:00'] ,['21:00','2:30']] or [1D, 3D, 5D, 11D, 22D]
    pair_data = get_pairwise_data(contract_pair, start_date=trade_day[trade_day.index(start_date)], end_date=trade_day[trade_day.index(end_date)])
    if len(pair_data) > 0:
        df_0 = pair_data[(pair_data['time'] > '09') & (pair_data['time'] < '11:30')]
        df_1 = pair_data[(pair_data['time'] > '13:30') & (pair_data['time'] < '15:00')]
        df_2 = pair_data[(pair_data['time'] > '21') | (pair_data['time'] < '02:30')]
        df = [df_0, df_1, df_2]
        #买近卖远
        df_abr_b = [pd.concat([temp['trading_date'], (temp['ap1_' + contract_pair[0]] - temp['bp1_' + contract_pair[1]])], axis=1) for temp in df]
        #卖近买远
        df_abr_s = [(temp['bp1_' + contract_pair[0]] - temp['ap1_' + contract_pair[1]]) for temp in df]
        return df_abr_b, df_abr_s
    else:
        return [pd.DataFrame(), pd.DataFrame(), pd.DataFrame()], [pd.DataFrame(), pd.DataFrame(), pd.DataFrame()]


def check_update_flag(contract_pair:list, q:float):
    # future_pair: ['fu2301', 'fu2305'] --> Paramater CSV: PARA_PATH/fu2301-fu2305.csv
    pair_filename = contract_pair[0] + '-' + contract_pair[1] + '.csv'
    PARA = os.path.join(PARA_PATH, "q=" + str(q))
    flag = os.path.exists(os.path.join(PARA, pair_filename))
    return flag, pair_filename
    
    
def export_parameter_csv(contract_pair_group:list, start_date:int, end_date:int, q:float):
    start = time.time()
    for pair in contract_pair_group:
        column_lst = ['date','max(buy)', 'min(buy)', 'mean(buy)', 'mean-std(buy)', 'mean-2std(buy)', 'mean-2.5std(buy)', 'price_quantile_90(buy)', 'price_quantile_95(buy)', 'price_quantile_N(buy)', 'max(sell)', 'min(sell)', 'mean(sell)', 'mean+std(sell)', 'mean+2std(sell)', 'mean+2.5std(sell)', 'price_quantile_90(sell)', 'price_quantile_95(sell)', 'price_quantile_N(sell)']
        df = pd.DataFrame(columns=column_lst)
        # Appending module
        update_flag, pair_fname = check_update_flag(pair,q)
        print("增量模式：", update_flag)
        if update_flag:
            Boundary = os.path.join(PARA_PATH, "q=" + str(q))
            # Reading saved params file of current pair
            df = pd.read_csv(os.path.join(Boundary, pair_fname))
            saved_date = int(df['date'].max().split('_')[0])
            saved_section = int(df['date'].max().split('_')[1])
            # Append from next trading section
            if saved_section == 0 or saved_section == 1:
                # Append to next trading day
                start_date = saved_date
                df_b, df_s = pair_plot_section(pair, start_date, start_date)
                for i in range(saved_section, 2):
                    df_b_temp = df_b[i]
                    df_s_temp = df_s[i]
                    df_combine = pd.concat([df_b_temp,df_s_temp],axis=1)
                    if len(df_combine) > 0:
                        for temp in pd.concat([df_b_temp,df_s_temp],axis=1).groupby('trading_date'):
                            # 交易单元代码轮换
                            if i == 0:
                                date = str(trade_day[trade_day.index(temp[0])]) + '_' + str(1)
                            if i == 1:
                                date = str(trade_day[trade_day.index(temp[0])]) + '_' + str(2)
                            max_0 = temp[1].iloc[:,1].max()
                            min_0 = temp[1].iloc[:,1].min()
                            mean_0 = temp[1].iloc[:,1].mean()
                            std_0 = temp[1].iloc[:,1].std()
                            q_0_90 = temp[1].iloc[:,1].quantile(.9)
                            q_0_95 = temp[1].iloc[:,1].quantile(.95)
                            q_0_N = temp[1].iloc[:,1].quantile(q)
                            max_1 = temp[1].iloc[:,2].max()
                            min_1 = temp[1].iloc[:,2].min()
                            mean_1 = temp[1].iloc[:,2].mean()
                            std_1 = temp[1].iloc[:,2].std()
                            q_1_90 = temp[1].iloc[:,2].quantile(.1)
                            q_1_95 = temp[1].iloc[:,2].quantile(.05)
                            q_1_N = temp[1].iloc[:,2].quantile(1-q)
                            #print(q_0_90,q_0_95, max_1,min_1)
                            row_data = {'date': [date], 'max(buy)': max_0, 'min(buy)': min_0, 'mean(buy)':mean_0, 'mean-std(buy)':mean_0-std_0, 'mean-2std(buy)':mean_0-2*std_0, 'mean-2.5std(buy)':mean_0-2.5*std_0, 'price_quantile_90(buy)':q_0_90, 'price_quantile_95(buy)':q_0_95, 'price_quantile_N(buy)':q_0_N, 'max(sell)':max_1, 'min(sell)':min_1, 'mean(sell)':mean_1, 'mean+std(sell)':mean_1+std_1, 'mean+2std(sell)':mean_1+2*std_1, 'mean+2.5std(sell)':mean_1+2.5*std_1, 'price_quantile_90(sell)':q_1_90, 'price_quantile_95(sell)':q_1_95, 'price_quantile_N(sell)':q_1_N}
                            df = pd.concat([df,pd.DataFrame(row_data)])
            # Pushing start pointer one day later
            start_date = trade_day[trade_day.index(saved_date)+1]
        if start_date <= end_date:
            # Appending following days
            # Query DB            
            df_b, df_s = pair_plot_section(pair, start_date, end_date)
            for i in range(3):
                df_b_temp = df_b[i]
                df_s_temp = df_s[i]
                df_combine = pd.concat([df_b_temp,df_s_temp],axis=1)
                if len(df_combine) > 0:
                    for temp in pd.concat([df_b_temp,df_s_temp],axis=1).groupby('trading_date'):
                        # 交易单元代码轮换
                        if i == 0:
                            date = str(trade_day[trade_day.index(temp[0])]) + '_' + str(1)
                        if i == 1:
                            date = str(trade_day[trade_day.index(temp[0])]) + '_' + str(2)
                        if i == 2:
                            date = str(trade_day[trade_day.index(temp[0])]) + '_' + str(0)
                        max_0 = temp[1].iloc[:,1].max()
                        min_0 = temp[1].iloc[:,1].min()
                        mean_0 = temp[1].iloc[:,1].mean()
                        std_0 = temp[1].iloc[:,1].std()
                        q_0_90 = temp[1].iloc[:,1].quantile(.9)
                        q_0_95 = temp[1].iloc[:,1].quantile(.95)
                        q_0_N = temp[1].iloc[:,1].quantile(q)
                        max_1 = temp[1].iloc[:,2].max()
                        min_1 = temp[1].iloc[:,2].min()
                        mean_1 = temp[1].iloc[:,2].mean()
                        std_1 = temp[1].iloc[:,2].std()
                        q_1_90 = temp[1].iloc[:,2].quantile(.1)
                        q_1_95 = temp[1].iloc[:,2].quantile(.05)
                        q_1_N = temp[1].iloc[:,2].quantile(1-q)
                        #print(q_0_90,q_0_95, max_1,min_1)
                        row_data = {'date': [date], 'max(buy)': max_0, 'min(buy)': min_0, 'mean(buy)':mean_0, 'mean-std(buy)':mean_0-std_0, 'mean-2std(buy)':mean_0-2*std_0, 'mean-2.5std(buy)':mean_0-2.5*std_0, 'price_quantile_90(buy)':q_0_90, 'price_quantile_95(buy)':q_0_95, 'price_quantile_N(buy)':q_0_N, 'max(sell)':max_1, 'min(sell)':min_1, 'mean(sell)':mean_1, 'mean+std(sell)':mean_1+std_1, 'mean+2std(sell)':mean_1+2*std_1, 'mean+2.5std(sell)':mean_1+2.5*std_1, 'price_quantile_90(sell)':q_1_90, 'price_quantile_95(sell)':q_1_95, 'price_quantile_N(sell)':q_1_N}
                        df = pd.concat([df,pd.DataFrame(row_data)])
    df = df.sort_values('date')
    print("DataLoading time: ", time.time()-start)
    return df


# TimeSeries Prediction on one pair
def predict_boundary_rolling(param_data, step=20, start_date='20220908_0', predict_date='20221118_0'):
    feature_columns_buy = ['price_quantile_N(buy)']
    feature_columns_sell = ['price_quantile_N(sell)']
    #feature_columns_buy = ['mean(buy)', 'mean-std(buy)', 'mean-2std(buy)', 'mean-2.5std(buy)', 'price_quantile_90(buy)', 'mean(sell)', 'mean+std(sell)', 'mean+2std(sell)', 'mean+2.5std(sell)', 'price_quantile_90(sell)']
    #feature_columns_sell = ['mean(buy)', 'mean-std(buy)', 'mean-2std(buy)', 'mean-2.5std(buy)', 'price_quantile_90(buy)', 'mean(sell)', 'mean+std(sell)', 'mean+2std(sell)', 'mean+2.5std(sell)', 'price_quantile_90(sell)']
    alpha = 0.3
    #print(param_data)
    try:
        end_date = [date for date in param_data['date'] if date >= start_date and date <= predict_date]
        print("last section selected: ", end_date[-1]) 
    
    # Rolling data with fixed window_size=step, stored in data_lst:list
        data_lst = [param_data[(param_data['date']>=start_date) & (param_data['date']<=end) & (param_data['date']>=end_date[end_date.index(end)-step])] for end in end_date]
    except IndexError as e:
        print("Step too long for current pairs DataVolume:")
        print(param_data)
        data_lst = [param_data[(param_data['date']>=start_date) & (param_data['date']<=end)] for end in end_date]

    data_lst = [x for x in data_lst if len(x)>0]

    # Linear model
    enet = ElasticNet(alpha, l1_ratio=0.5, max_iter=15000)

    try:
    # Rolling from start
        X_train_buy_diff = np.array([data['price_quantile_N_diff(buy)'][:-1] for data in data_lst[:-1]]).reshape(len(data_lst)-1,step)
        X_train_sell_diff = np.array([data['price_quantile_N_diff(sell)'][:-1] for data in data_lst[:-1]]).reshape(len(data_lst)-1,step)

        y_train_buy_diff = np.array([data.iloc[-1]['price_quantile_N_diff(buy)'] for data in data_lst[:-1]]).reshape(len(data_lst)-1,1)
        y_train_sell_diff = np.array([data.iloc[-1]['price_quantile_N_diff(sell)'] for data in data_lst[:-1]]).reshape(len(data_lst)-1,1)
    
        X_test_buy_diff = np.array(data_lst[-1]['price_quantile_N_diff(buy)'][1:]).reshape(1,step)
        X_test_sell_diff = np.array(data_lst[-1]['price_quantile_N_diff(sell)'][1:]).reshape(1,step)
        
        y_pred_buy_diff = enet.fit(X_train_buy_diff, y_train_buy_diff).predict(X_test_buy_diff)[0] + data_lst[-1][feature_columns_buy].iloc[-1].values
        y_pred_sell_diff = enet.fit(X_train_sell_diff, y_train_sell_diff).predict(X_test_sell_diff)[0] + data_lst[-1][feature_columns_sell].iloc[-1].values

    except (IndexError, ValueError) as e:
        print(e)
        y_pred_buy_diff = 99999
        y_pred_sell_diff = -99999

    print("ENet:{buy_diff_predict:%f, sell_diff_predict:%f}" %(y_pred_buy_diff, y_pred_sell_diff))
    return [y_pred_buy_diff, y_pred_sell_diff]
    # 获取真实值
    #whole_df = pd.concat([whole_df, df])
    #true_df = test_df[test_df['date']>start_date][['price_quantile_90(buy)', 'price_quantile_95(buy)', 'max(buy)', 'min(buy)', 'price_quantile_90(sell)', 'price_quantile_95(sell)', 'max(sell)', 'min(sell)']].reset_index(drop=True)
    #whole_df = whole_df.reset_index(drop=True)
    #whole_df = pd.concat([whole_df, true_df], axis=1).to_csv(os.path.join(r"/mnt/cofund_z/300_Group/HFT/ctp_model", pair_file), index=False)


# 进度条计时器在这里放
def predict_boundary(contract_pair:list, end_date:int, end_date_section:int, q=0.95, step=20, start_date=20220908):
    # 1
    PARA = os.path.join(PARA_PATH, "q=" + str(q))
    if not os.path.exists(PARA):
        os.mkdir(PARA)
    contract_pair = [utils.rename(contract_pair[0]), utils.rename(contract_pair[1])]
    print(contract_pair)
    pair_name = contract_pair[0] + '-' + contract_pair[1]
    param_df = export_parameter_csv([contract_pair], start_date, end_date, q)
    param_df.to_csv(os.path.join(PARA, pair_name + '.csv'), index=False)
    # 2 - 回测
    #export_tick_csv([contract_pair], start_date, end_date)
    # 3 - 预测
    param_df = param_df.fillna(method='ffill')
    param_df['price_quantile_N_diff(buy)'] = param_df['price_quantile_N(buy)'].diff(1)
    param_df['price_quantile_N_diff(sell)'] = param_df['price_quantile_N(sell)'].diff(1)
    param_df = param_df.dropna()
    print("----------------")
    print("Step=%d" %(step))
    result = predict_boundary_rolling(param_data=param_df, start_date=str(start_date)+"_0", predict_date=str(end_date)+'_'+str(end_date_section), step=step)
    return round(float(min(result[0], result[1])),2), round(float(abs(result[0]-result[1])),2)


# 计算unit_num_base_region是否落入时间区间
def predict_region(region_num:int, end_date:int, reduce_date:int, clear_date:int):
    # 读取region_info并判断所处时间区间
    # if in : /2四舍五入
    # if not in : 不变
    if end_date >= reduce_date and end_date <= clear_date:
        region_num /= 2
        return round(region_num+0.01)
    else:
        return region_num


# 生成一张info表，与账户的budget值迭代后在ui上展示
# info表包含且仅包含完整的区界信息，传入transform函数后可以得到合适的params.csv
def predict_info(region_info:pd.DataFrame, end_date:int, end_section:int, q:float, step:int, ratio:float, flag:bool, cache_path:str):
    # 根据账户资金调整unit_num
    region_info['unit_num_base_region'] = region_info.apply(lambda x: predict_region(x['unit_num_base_region'], end_date, x['reduce_date'], x['clear_date']), axis=1)
    region_info['unit_num_base_boundary'] = region_info.apply(lambda x: predict_region(x['unit_num_base_boundary'], end_date, x['reduce_date'], x['clear_date']), axis=1)
    region_info['up_boundary_5'] = "99999"
    region_info['down_boundary_5'] = "-99999"
    region_info['kind'] = region_info['pairs_id'].apply(lambda x: re.search("[a-zA-Z]+", x).group(0))
    if not flag:
        # boundary predict
        region_info[['down_boundary_1', 'boundary_tick_lock']] = region_info.apply(lambda x: predict_boundary([x['first_instrument'], x['second_instrument']], end_date, end_section, q=q, step=step), axis=1, result_type="expand")
        region_info['down_boundary_2'] = (region_info['down_boundary_1'] - region_info['boundary_tick_lock']).apply(lambda x: round(x,2))
        region_info['down_boundary_3'] = (region_info['down_boundary_2'] - region_info['boundary_tick_lock']).apply(lambda x: round(x,2))
        region_info['down_boundary_4'] = (region_info['down_boundary_3'] - region_info['boundary_tick_lock']).apply(lambda x: round(x,2))

        region_info['up_boundary_1'] = (region_info['down_boundary_1'] + region_info['boundary_tick_lock']).apply(lambda x: round(x,2))
        region_info['up_boundary_2'] = (region_info['up_boundary_1'] + region_info['boundary_tick_lock']).apply(lambda x: round(x,2))
        region_info['up_boundary_3'] = (region_info['up_boundary_2'] + region_info['boundary_tick_lock']).apply(lambda x: round(x,2))
        region_info['up_boundary_4'] = (region_info['up_boundary_3'] + region_info['boundary_tick_lock']).apply(lambda x: round(x,2))
        region_info['boundary_tick_lock'] = region_info['boundary_tick_lock'] * ratio
        # 除价跳单位
        region_info['boundary_tick_lock'] = region_info['boundary_tick_lock'] / region_info['kind'].map(utils.boundary_dict)
        region_info['boundary_tick_lock'] = region_info['boundary_tick_lock'].apply(lambda x: round(x))
    else:
        cached_info = pd.read_csv(cache_path)
        cached_boundary = cached_info[['pairs_id', 'down_boundary_1', 'boundary_tick_lock', 'down_boundary_2', 'down_boundary_3', 'down_boundary_4', 'up_boundary_1', 'up_boundary_2', 'up_boundary_3', 'up_boundary_4']]
        region_info = region_info.merge(cached_boundary, left_on='pairs_id', right_on='pairs_id')
        # selected boundary tick predicted
        # if need, could select boundary tick in region_info.xlsx
        region_info = region_info.drop(columns='boundary_tick_lock_x').rename(columns={'boundary_tick_lock_y':'boundary_tick_lock'})
    return region_info


if __name__ == '__main__':
    df = pd.read_excel(r".\info\region_info.xlsx", sheet_name="Sheet1")
    predict_info(df, 20221207, 0, 0.95, 6).to_csv('C:\PycharmProjects\PycharmProjects\info.csv')



