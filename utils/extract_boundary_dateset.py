import numpy as np
# Dependencies
import os, numpy as np, pandas as pd
from pylab import mpl
mpl.rcParams['font.sans-serif'] = ['SimHei']
mpl.rcParams['axes.unicode_minus'] = False
from time import *
from utils.rename import rename_db_to_param
import pandas as pd
import os
from utils.date_section_modification import *
from utils.cache_management import *
from utils.calculate_parameter import *
import os
from utils.const import exchange_breed_dict, db_para,  client, default_args, trade_day, BOUNDARY_PATH
from utils.rename import rename
import re

def get_param_contract_pair():
    breed_lst = []
    for key, values in exchange_breed_dict.items():
        breed_lst += exchange_breed_dict[key]
    breed_lst += [x.lower() for x in breed_lst]
    cta_table = db_para['tb_to']
    SQL = "SELECT distinct contract, breed from " + cta_table + " where breed in " + str(tuple(breed_lst))
    print(SQL)
    df = client.query_dataframe(SQL).sort_values('contract')
    contract_pair_dict = {}
    for breed_class in df.groupby('breed'):
        contract_pair_lst = []
        contract_lst = breed_class[1]['contract'].tolist()
        contract_pair_lst += [[rename_db_to_param(contract_lst[i]), rename_db_to_param(contract_lst[i+1])] for i in range(len(contract_lst)-1)]
        contract_pair_dict[breed_class[0]] = contract_pair_lst
    return contract_pair_dict



        
        
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
    
    
def check_update_flag(contract_pair:list, q:float):
    # future_pair: ['fu2301', 'fu2305'] --> Paramater CSV: BOUNDARY_PATH/fu2301-fu2305.csv
    pair_filename = contract_pair[0] + '-' + contract_pair[1] + '.csv'
    PARA = os.path.join(BOUNDARY_PATH, "q=" + str(q))
    flag = os.path.exists(os.path.join(PARA, pair_filename))
    if flag:
        df = pd.read_csv(os.path.join(PARA, pair_filename))
        if len(df) == 0:
            flag = False
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
            Boundary = os.path.join(BOUNDARY_PATH, "q=" + str(q))
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


# 进度条计时器在这里放
def export_boundary_dataset(q=0.95, start_date=20220908):
    date, section = get_date_section()
    date, section = from_predict(date, section)
    region_df = pd.read_excel('./info/region_info.xlsx', sheet_name="Sheet1")
    pairs = region_df['pairs_id']
    append_pairs = [f.split('.')[0] for f in os.listdir(r'Z:\300_Group\HFT\Program\CTA_UI\info\boundary_info\q=0.95')]
    pairs = pairs.append(pd.Series(append_pairs))
    pairs = pairs.drop_duplicates()
    PARA = os.path.join(BOUNDARY_PATH, "q=" + str(q))
    if not os.path.exists(PARA):
        os.mkdir(PARA)
    # 重组为合规的Params表
    for pair in pairs:
        try:
            first_contract_code = rename(pair.split('-')[0])
            breed = re.search("[a-zA-Z]+", first_contract_code).group(0)
            second_contract_code = rename(breed + pair.split('-')[1])
            pair_name = first_contract_code + '-' + second_contract_code
            param_df = export_parameter_csv([first_contract_code, second_contract_code], start_date, date, q)
            param_df.to_csv(os.path.join(PARA, pair_name + '.csv'), index=False)
            print("已成功导出套利对数据：", pair_name)
        except Exception as e:
            print("导出套利对数据失败：", pair_name, e)

if __name__ == "__main__":
    print(export_boundary_dataset())
