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
import time
from . import rename
from . import const

__all__ = ["predict_info"]
# Get data from start_date[MorningMarket] to end_date[EveningMarket]
def get_pairwise_data(contract_pair:list, start_date:int, end_date:int):
    import time
    start = time.time()
    select_clause = 'SELECT contract, trading_date, time, ap1, av1, bp1, bv1 from ctp_future_tick '
    where_clause = 'WHERE contract in ' + str(tuple(contract_pair)) + ' and trading_date >= '+ str(start_date) + ' and trading_date <= ' + str(end_date)
    query = select_clause + where_clause
    res = const.client.query_dataframe(query)
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
    pair_data = get_pairwise_data(contract_pair, start_date=const.trade_day[const.trade_day.index(start_date)], end_date=const.trade_day[const.trade_day.index(end_date)])
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
    # future_pair: ['fu2301', 'fu2305'] --> Paramater CSV: BOUNDARY_PATH/fu2301-fu2305.csv
    pair_filename = contract_pair[0] + '-' + contract_pair[1] + '.csv'
    PARA = os.path.join(const.BOUNDARY_PATH, "q=" + str(q))
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
            Boundary = os.path.join(const.BOUNDARY_PATH, "q=" + str(q))
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
                                date = str(const.trade_day[const.trade_day.index(temp[0])]) + '_' + str(1)
                            if i == 1:
                                date = str(const.trade_day[const.trade_day.index(temp[0])]) + '_' + str(2)
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
            start_date = const.trade_day[const.trade_day.index(saved_date)+1]
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
                            date = str(const.trade_day[const.trade_day.index(temp[0])]) + '_' + str(1)
                        if i == 1:
                            date = str(const.trade_day[const.trade_day.index(temp[0])]) + '_' + str(2)
                        if i == 2:
                            date = str(const.trade_day[const.trade_day.index(temp[0])]) + '_' + str(0)
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
    PARA = os.path.join(const.BOUNDARY_PATH, "q=" + str(q))
    if not os.path.exists(PARA):
        os.mkdir(PARA)
    contract_pair = [rename(contract_pair[0]), rename(contract_pair[1])]
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
        region_info['boundary_tick_lock'] = region_info['boundary_tick_lock'] / region_info['kind'].map(const.boundary_dict)
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



