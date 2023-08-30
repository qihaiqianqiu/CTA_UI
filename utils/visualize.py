# Dependencies
import os
import numpy as np, pandas as pd
import re
# 非交互模式防止内存泄露
import matplotlib
matplotlib.use('Agg') 
from matplotlib.colors import Normalize
import matplotlib.cm as cm
from matplotlib.patches import PathPatch
from math import log,sqrt
from scipy import stats
from scipy.interpolate import interp1d
import matplotlib.pyplot as plt
import matplotlib as mpl
mpl.rcParams['font.sans-serif'] = ['SimHei']
mpl.rcParams['axes.unicode_minus'] = False
mpl.rcParams['ytick.labelsize'] = 100
mpl.rcParams['xtick.labelsize'] = 70
mpl.rcParams['ytick.major.size'] = 8
mpl.rcParams['ytick.major.width'] = 2
from PyQt5.QtWidgets import QDialog, QLabel, QVBoxLayout
from PyQt5.QtCore import Qt
from scipy.stats import norm
import matplotlib.dates as mdate
import matplotlib.font_manager as fm
import matplotlib.patches as patches
import time
import traceback
import datetime
mpl.rcParams.update({
'text.usetex': False,
'font.family': 'stixgeneral',
'mathtext.fontset': 'stix',
})
from tabulate import tabulate
from multiprocessing import Pool
from utils.const import trade_day, PLOT_PATH
from utils.rename import rename, rename_db_to_param
from utils.get_contract_pair import get_db_contract_pair
from utils.date_section_modification import get_date_section, from_predict
from utils.calculate_parameter import get_pairwise_data

all = ['plot_continuous_contract', 'plot_time_series', 'plot_volume_split', "create_aligned_table", "show_table_in_message_box", "stock_to_pie", "show_message_box"]
logger = open(os.path.join(PLOT_PATH, "visual_log.txt"), "a+")
err = open(os.path.join(PLOT_PATH, "visual_error.txt"), "a+")


# 字符串表格的输出
def create_aligned_table(data):
    # 获取每列最大宽度
    column_widths = [max(map(len, column)) for column in zip(*data)]
    
    # 设置表头
    headers = data[0]
    
    # 设置列宽度
    aligned_data = [[str(item).ljust(width) for item, width in zip(row, column_widths)] for row in data[1:]]
    
    # 使用tabulate生成表格
    table = tabulate(aligned_data, headers, tablefmt="plain")
    
    return table


def show_table_in_message_box(data):
    table_html = "<table style='border-collapse: collapse;'>"
    for row in data:
        table_html += "<tr>"
        for item in row:
            table_html += "<td style='border: 1px solid black; padding: 5px; white-space: nowrap;'>{}</td>".format(item)
        table_html += "</tr>"
    table_html += "</table>"
    
    return table_html

def show_message_box(error_log):
    table_output = create_aligned_table(error_log)
    html_table = show_table_in_message_box(error_log)
    with open("changelog.txt", "a+", encoding='utf-8') as err_file:
        err_file.write(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S') + '\n')
        err_file.write(table_output + '\n')
    dialog = QDialog()
    dialog.setWindowTitle("传输完成")
    layout = QVBoxLayout()
    label = QLabel()
    label.setTextFormat(Qt.RichText)
    label.setText(html_table)
    layout.addWidget(label)
    dialog.setLayout(layout)
    return dialog
    
# 持仓字典画饼图
def stock_to_pie(data):
    sorted_data = sorted(data.items(), key=lambda x: abs(x[1]), reverse=True)
    labels = [item[0] for item in sorted_data]
    sizes = [abs(item[1]) for item in sorted_data]

    positive_profit = {k: v for k, v in data.items() if v > 0}
    negative_profit = {k: v for k, v in data.items() if v < 0}

    positive_sizes = list(positive_profit.values())
    positive_sum = sum(positive_sizes)

    negative_sizes = list(negative_profit.values())
    negative_sizes = [abs(value) for value in negative_sizes]
    negative_sum = sum(negative_sizes)
    
    positive_labels = [f"{k}: {v/positive_sum * 100:.2f}%" for k, v in positive_profit.items()]
    positive_percent = [v / positive_sum * 100 for k, v in positive_profit.items()]
    negative_labels = [f"{k}: {abs(v)/negative_sum * 100:.2f}%" for k, v in negative_profit.items()]
    negative_percent = [abs(v) / negative_sum *100 for k, v in negative_profit.items()]

    title_fontsize = 21
    title_font = fm.FontProperties(weight='bold', size=title_fontsize)
    label_fontsize = 18
    label_font = fm.FontProperties(weight='bold', size=label_fontsize)

    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(45, 24))

    # 饼图1：正收益
    wedges1, _ = ax1.pie(positive_sizes, labels=None, autopct=None, startangle=90, textprops={'fontsize': label_fontsize})
    ax1.set_title(f"Positive Profit: {positive_sum}", fontproperties=title_font)

    # 饼图2：负收益
    wedges2, _= ax2.pie(negative_sizes, labels=None, autopct=None, startangle=90, textprops={'fontsize': label_fontsize})
    ax2.set_title(f"Negative Profit: {negative_sum}", fontproperties=title_font)

    # 调整图例位置
    ax1.legend(wedges1, positive_labels, loc="center left", bbox_to_anchor=(1, 0.5), fontsize=label_fontsize)
    ax2.legend(wedges2, negative_labels, loc="center left", bbox_to_anchor=(1, 0.5), fontsize=label_fontsize)

    # 添加指向品种名称的线和文字
    for i, p in enumerate(wedges1):
        if positive_percent[i] < 5:
            print("skip {}".format(positive_labels[i]))
            continue
        ang = (p.theta2 - p.theta1) / 2.0 + p.theta1
        y = np.sin(np.deg2rad(ang))
        x = np.cos(np.deg2rad(ang))
        horizontalalignment = {-1: "right", 1: "left"}[int(np.sign(x))]
        connectionstyle = "angle,angleA=0,angleB={}".format(ang)
        kw = dict(arrowprops=dict(arrowstyle="->", lw=1, connectionstyle=connectionstyle))
        xy = (x, y)
        if p.theta2 - p.theta1 < 10:
            xy = (0.5*x, 1.5*y)
        xytext = (1.35 * np.sign(x), 1.4 * y)
        ax1.annotate(
            positive_labels[i],
            xy=xy,
            xytext=xytext,
            horizontalalignment=horizontalalignment,
            **kw,
            fontsize=21
        )
        print("draw positive label", positive_labels[i])

    for i, p in enumerate(wedges2):
        if negative_percent[i] < 5:
            print("skip", negative_percent[i])
            continue
        ang = (p.theta2 - p.theta1) / 2.0 + p.theta1
        y = np.sin(np.deg2rad(ang))
        x = np.cos(np.deg2rad(ang))
        horizontalalignment = {-1: "right", 1: "left"}[int(np.sign(x))]
        connectionstyle = "angle,angleA=0,angleB={}".format(ang)
        kw = dict(arrowprops=dict(arrowstyle="->", lw=1, connectionstyle=connectionstyle))
        xy = (x, y)
        if p.theta2 - p.theta1 < 10:
            xy = (0.5*x, 1.5*y)
        xytext = (1.35 * np.sign(x), 1.4 * y)
        ax2.annotate(
            negative_labels[i],
            xy=xy,
            xytext=xytext,
            horizontalalignment=horizontalalignment,
            **kw,
            fontsize=21
        )
        print("draw negative label", negative_labels[i])
        
    # 调整子图布局
    plt.subplots_adjust(wspace=0.5)

    plt.savefig(os.path.join(PLOT_PATH, "profit_distribution.png"))
    
    return plt

# Get data from start_date[MorningMarket] to end_date[EveningMarket]
# 连续合约对模块（蝶式套）返回对应数据  
def bar_plot_get_continous_data(contract_pair_lst:list, date:int, section:int):
    df_section = []
    label_section = []
    for contract_pair in contract_pair_lst:
        print("绘制套利对{}连续合约图像的{}_{}： ".format(contract_pair, date, section))
        try:
            if section == 2:
                pair_data = get_pairwise_data(contract_pair, start_date=trade_day[trade_day.index(date)+1], end_date=trade_day[trade_day.index(date)+1])
            else:
                pair_data = get_pairwise_data(contract_pair, start_date=trade_day[trade_day.index(date)], end_date=trade_day[trade_day.index(date)])
            if len(pair_data) == 0:
                continue
        except Exception as e:
            continue
        if section == 0:
            df = pair_data[(pair_data['time'] > '09') & (pair_data['time'] < '11:30')]

        if section == 1:
            df = pair_data[(pair_data['time'] > '13:30') & (pair_data['time'] < '15:00')]
            
        if section == 2:
            df = pair_data[(pair_data['time'] > '21') | (pair_data['time'] < '02:30')]
        
        if len(df) > 0:
            df_section.append({contract_pair[0] + "-" + contract_pair[1]: df})
            label_section.append(str(contract_pair[0] + "-" + contract_pair[1]))

    return df_section, label_section, [str(date), str(section)]
    

# 成交量返回对应数据
def bar_plot_get_volume_split_data(contract_pair_lst:list, date:int, section:int, batch_num = 40, volume_threshold = 1000):
    df_section = []
    label_section = []
    volume_batch_section = []
    for contract_pair in contract_pair_lst:
        print("绘制套利对{}成交分段图像的{}_{}： ".format(contract_pair, date, section))
        try:
            if section == 2:
                pair_data = get_pairwise_data(contract_pair, start_date=trade_day[trade_day.index(date)+1], end_date=trade_day[trade_day.index(date)+1])
            else:
                pair_data = get_pairwise_data(contract_pair, start_date=trade_day[trade_day.index(date)], end_date=trade_day[trade_day.index(date)])
            if len(pair_data) == 0:
                continue
        except Exception as e:
            continue

        if section == 0:
            df = pair_data[(pair_data['time'] > '09') & (pair_data['time'] < '11:30')]

        if section == 1:
            df = pair_data[(pair_data['time'] > '13:30') & (pair_data['time'] < '15:00')]
            
        if section == 2:
            df = pair_data[(pair_data['time'] > '21') | (pair_data['time'] < '02:30')]

        if len(df) > 0:
            print(df)
            df['volume'] = df.apply(lambda x: min(x['volume_'+contract_pair[0]] ,x['volume_'+contract_pair[1]]), axis=1)
            # 计算成交量分段
            max_vol = df['volume'].max()
            min_vol = df['volume'].min()
            volume_batch = int((max_vol - min_vol) / batch_num)
            if volume_batch == 0 or max_vol < volume_threshold:
                print("Volume batch for {} is {}, expected volume is {} --> insufficient value".format(contract_pair, volume_batch, max_vol))
                continue
            df['volume_section'] = df['volume'].apply(lambda x: int((x - min_vol) / volume_batch))
            df['volume_section'] = df['volume_section'].apply(lambda x: x if x < batch_num else batch_num - 1)
            df_vol_lst = []
            label_lst = []
            for i in range(batch_num):
                df_vol_lst.append(df[df['volume_section'] == i])
                label_lst.append(str((i+1) * volume_batch))
            # 切分
            df_section.append({contract_pair[0] + "-" + contract_pair[1]: df_vol_lst})
            label_section.append(label_lst)
            volume_batch_section.append(volume_batch)

    return df_section, label_section, volume_batch_section, [str(date), str(section)]


# BarPlot
def bar_plot_get_time_series_data(contract_pair:list, start_date:int, end_date:int):
    save_date = end_date
    end_date = trade_day[trade_day.index(end_date) + 1]
    df_whole = get_pairwise_data(contract_pair, start_date, end_date)
    df_section = []
    label_section = []
    if len(df_whole) > 0:
        for date in range(start_date, trade_day[trade_day.index(end_date) + 1]):
            if date in trade_day:
                pair_data = df_whole[df_whole['trading_date']==date]
                if len(pair_data) > 0:
                    df_s0 = pair_data[(pair_data['time'] > '09') & (pair_data['time'] < '11:30')]
                    x_label_s0 = str(pair_data['trading_date'].iloc[0]) + '_0'
            
                    df_s1 = pair_data[(pair_data['time'] > '13:30') & (pair_data['time'] < '15:00')]
                    x_label_s1 = str(pair_data['trading_date'].iloc[0]) + '_1'
                        
                    df_s2 = pair_data[(pair_data['time'] > '21') | (pair_data['time'] < '02:30')]
                    x_label_s2 = str(trade_day[trade_day.index(pair_data['trading_date'].iloc[0]) - 1]) + '_2'

                    if len(df_s2) > 0:
                        df_section.append({contract_pair[0] + "-" + contract_pair[1]:df_s2})
                        label_section.append(x_label_s2)    

                    if len(df_s0) > 0:
                        df_section.append({contract_pair[0] + "-" + contract_pair[1]:df_s0})
                        label_section.append(x_label_s0)

                    if len(df_s1) > 0:
                        df_section.append({contract_pair[0] + "-" + contract_pair[1]:df_s1})
                        label_section.append(x_label_s1)
    return df_section, label_section, save_date


# 将单元数据处理为箱线
def bar_plot_data_segmentation(df_contract_lst:list):
    bar_section = []
    vol_section = []
    for df_dict in df_contract_lst:
        for contract_pair, pair_data in df_dict.items():
            contract_pair = contract_pair.split('-')
        #counter = 0
            # 买可得
            df_abr_b = (pair_data['ap1_' + contract_pair[0]] - pair_data['bp1_' + contract_pair[1]])
            # 卖可得
            df_abr_s = (pair_data['bp1_' + contract_pair[0]] - pair_data['ap1_' + contract_pair[1]])

            # 成交量选取主动腿的
            volume = min(pair_data['volume_' + contract_pair[0]].max() - pair_data['volume_' + contract_pair[0]].min(), pair_data['volume_' + contract_pair[1]].max() -pair_data['volume_' + contract_pair[1]].min())

            # 筛选可成价
            df_abr_mean = float(pd.concat([df_abr_b, df_abr_s], ignore_index=True).mean())
            df_abr = pd.concat([df_abr_b[df_abr_b <= df_abr_mean], df_abr_s[df_abr_s >= df_abr_mean]], ignore_index=True)
            vol_section.append(volume)
            bar_section.append(df_abr)
            
    return vol_section, bar_section
    


# 绘图过程
def plot_fig(vol_section, bar_section, label_lst, fig_title, export_dir, filename, x_rotation=True):
    #vol_values = [float(i)/sum(vol_values) for i in vol_values]
    # create a normalizer
    norm = Normalize(vmin=min(vol_section), vmax=max(vol_section))
    # normalize values
    norm_values = norm(vol_section)
    # choose a colormap
    cmap = cm.magma
    # create colors
    colors = cmap(norm_values)
    # map values to a colorbar
    mappable = cm.ScalarMappable(norm=norm, cmap=cmap)
    mappable.set_array(colors)

    fig, ax = plt.subplots()
    if x_rotation:
        ax.tick_params(labelrotation=90)
    ax.set_title(fig_title)
    ax.set_xticklabels(label_lst)
    ax.title.set_size(120)
    
    ba = ax.boxplot(bar_section, patch_artist=True, showfliers=False)
    patches = ba["boxes"]

    for p, c in zip(patches, colors):
        p.set_facecolor(c)

    cb = fig.colorbar(mappable)
    cb.set_label("Dealable_Volume", fontdict={'size':60})
    cb.ax.tick_params(labelsize=80)
    fig.set_size_inches(192, 64)
    if not os.path.exists(export_dir):
        os.mkdir(export_dir)
    print("Plot exported at:", os.path.join(export_dir, filename))
    plt.savefig(os.path.join(export_dir, filename))
    fig.clf()
    plt.close()


def bar_plot_time_series(contract_pair:list, start_date:int, end_date:int):
    try:
        df_section, label_section, save_date = bar_plot_get_time_series_data(contract_pair, start_date, end_date)
        if len(df_section) == 0:
            print("contract pair {} has been outdated".format(contract_pair))
            return
    except Exception as e:
        print(traceback.format_exc())
        err.write("{" + time.strftime("%y:%m:%d:%H:%M", time.localtime()) + "}" + " ##Get_TimeSeries_Data" + " Fail:" + str(contract_pair) + '\n')
        err.write("==Detail==:\n")
        err.write(traceback.format_exc()+'\n')
        err.write('============================================\n')
        return
    try:
        vol_section, bar_section = bar_plot_data_segmentation(df_section)
        DIR = os.path.join(os.path.abspath(PLOT_PATH), str(save_date))
        figname = str(save_date) + '-' + rename_db_to_param(str(contract_pair[0])) + '-' + re.search("[0-9]+", rename_db_to_param(str(contract_pair[1]))).group(0) + '.png'
        plot_fig(vol_section, bar_section, label_section, fig_title = str(contract_pair), export_dir=DIR, filename=figname)
        logger.write("{" + time.strftime("%y:%m:%d:%H:%M", time.localtime()) + "}" + " ##TimeSeries_Plot" + " Success:" + str(contract_pair))
        logger.write('\n')
    except Exception as e:
        print(traceback.format_exc())
        err.write("{" + time.strftime("%y:%m:%d:%H:%M", time.localtime()) + "}" + " ##TimeSeries_Plot" + " Fail:" + str(contract_pair))
        err.write('\n')
        err.write("==Detail==:\n")
        err.write(traceback.format_exc()+'\n')
        err.write('============================================\n')
        return

def bar_plot_continous_data(date:int, section:int):
    contract_dict = get_db_contract_pair()
    for breed in contract_dict:
        print(contract_dict[breed], date, section)
        try:
            df_section, label_section, save_date = bar_plot_get_continous_data(contract_dict[breed], date, section)
        except Exception as e:
            print(traceback.format_exc())
            err.write("{" + time.strftime("%y:%m:%d:%H:%M", time.localtime()) + "}" + " ##Get_Continous_Data" + " Fail:" + str(breed) + '\n')
            err.write("==Detail==:\n")
            err.write(traceback.format_exc()+'\n')
            err.write('============================================\n')
            continue
        
        vol_section, bar_section = bar_plot_data_segmentation(df_section)
        DIR = os.path.join(os.path.abspath(PLOT_PATH), save_date[0])
        figname = save_date[0] + '_' + save_date[1] + '-' + str(breed) + '.png'
        try:
            plot_fig(vol_section, bar_section, label_section, fig_title = str(breed), export_dir=DIR, filename=figname, x_rotation=False)
            logger.write("{" + time.strftime("%y:%m:%d:%H:%M", time.localtime()) + "}" + " ##Continous_Plot" + " Success:" + str(breed))
            logger.write('\n')
        except Exception as e:
            print(traceback.format_exc())
            err.write("{" + time.strftime("%y:%m:%d:%H:%M", time.localtime()) + "}" + " ##Continous_Plot" + " Fail:" + str(breed))
            err.write('\n')
            err.write("==Detail==:\n")
            err.write(traceback.format_exc()+'\n')
            err.write('============================================\n')
            continue

def bar_plot_volume_split_data(date:int, section:int, batch_num = 40):
    contract_dict = get_db_contract_pair()
    for breed in contract_dict:
        try:
            df_part_lst, label_part_lst, volume_part_lst, save_date = bar_plot_get_volume_split_data(contract_dict[breed], date, section, batch_num)
            # 将根据成交量切分的df列表解开成一个个df
        except Exception as e:
            print(traceback.format_exc())
            err.write("{" + time.strftime("%y:%m:%d:%H:%M", time.localtime()) + "}" + " ##Get_Volume_Data" + " Fail:" + str(breed) + '\n')
            err.write("==Detail==:\n")
            err.write(traceback.format_exc()+'\n')
            err.write('============================================\n')
            continue

        try:
            for i in range(len(df_part_lst)):
                df_section_part_lst = []
                df_section_part = df_part_lst[i]
                label_section_part = label_part_lst[i]
                vol_section_part = volume_part_lst[i]
                for key, value in df_section_part.items():
                    contract_pair = key
                    df_part = value
                for df in df_part:
                    df_section_part_lst.append({contract_pair:df})
                vol_section, bar_section = bar_plot_data_segmentation(df_section_part_lst)
                print("++++++++")
                print(vol_section)
                print(label_part_lst[i])
                DIR = os.path.join(os.path.abspath(PLOT_PATH), save_date[0])
                near = str(contract_pair).split('-')[0]
                forward = str(contract_pair).split('-')[1]
                figname = save_date[0] + '-' + save_date[1] + '-' + rename_db_to_param(near) + '-' + re.search("[0-9]+", rename_db_to_param(forward)).group(0) + "-volume_split" + '.png'
                plot_fig(vol_section, bar_section, label_section_part, fig_title = str(contract_pair) + '_' + str(vol_section_part), export_dir=DIR, filename=figname, x_rotation=False)
                logger.write("{" + time.strftime("%y:%m:%d:%H:%M", time.localtime()) + "}" + " ##Volume_Plot" + " Success:" + str(contract_pair))
                logger.write('\n')
        except Exception as e:
            print(traceback.format_exc())
            err.write("{" + time.strftime("%y:%m:%d:%H:%M", time.localtime()) + "}" + " ##Volume_Plot" + " Fail:" + str(contract_pair))
            err.write('\n')
            err.write("==Detail==:\n")
            err.write(traceback.format_exc()+'\n')
            err.write('============================================\n')

            continue
            
            

# Histo
# Plotting
def pair_plot_section(date, future_pair_code, section, plot=True):
    # Trading_section = [['9','11:30'], ['13:30','15:00'] ,['21:00','2:30']] or [1D, 3D, 5D, 11D, 22D]
    df_whole = get_pairwise_data(future_pair_code, start_date=trade_day[trade_day.index(date)-21], end_date=date)

    if section == 0:
        pair_data = get_pairwise_data(future_pair_code, start_date=date, end_date=date)
        df = pair_data[(pair_data['time'] > '09') & (pair_data['time'] < '11:30')]
        x_label = str(pair_data['trading_date'].iloc[0]) + '-Morning'
        
    if section == 1:
        pair_data = get_pairwise_data(future_pair_code, start_date=date, end_date=date)
        df = pair_data[(pair_data['time'] > '13:30') & (pair_data['time'] < '15:00')]
        x_label = str(pair_data['trading_date'].iloc[0]) + '-Afternoon'
        
    if section == 2:
        pair_data = get_pairwise_data(future_pair_code, start_date=date, end_date=date)
        df = pair_data[(pair_data['time'] > '21') | (pair_data['time'] < '02:30')]
        x_label = str(pair_data['trading_date'].iloc[0]) + '-Evening'
        
    if section == 3:
        pair_data = get_pairwise_data(future_pair_code, start_date=date, end_date=date)
        df = pair_data
        x_label = str(pair_data['trading_date'].iloc[0]) + '-1D'
        
    if section == 4:
        # trade_day backward 3 days
        pair_data = get_pairwise_data(future_pair_code, start_date=trade_day[trade_day.index(date)-2], end_date=date)
        df = pair_data
        x_label = str(pair_data['trading_date'].iloc[0]) + '-3D'
    
    if section == 5:
        # trade_day backward 7 days
        pair_data = get_pairwise_data(future_pair_code, start_date=trade_day[trade_day.index(date)-4], end_date=date)
        df = pair_data
        x_label = str(pair_data['trading_date'].iloc[0]) + '-5D'
    
    if section == 6:
        # trade_day backward 11 days
        pair_data = get_pairwise_data(future_pair_code, start_date=trade_day[trade_day.index(date)-10], end_date=date)
        df = pair_data
        x_label = str(pair_data['trading_date'].iloc[0]) + '-11D'
        
    if section == 7:
        # trade_day backward 22 days
        pair_data = get_pairwise_data(future_pair_code, start_date=trade_day[trade_day.index(date)-21], end_date=date)
        df = pair_data
        x_label = str(pair_data['trading_date'].iloc[0]) + '-22D'
        

    #买近卖远
    df_abr_b = (df['ap1_' + future_pair_code[0]] - df['bp1_' + future_pair_code[1]])
    #卖近买远
    df_abr_s = (df['bp1_' + future_pair_code[0]] - df['ap1_' + future_pair_code[1]])
    
    if plot:
        # Calculate x-label limit values
        # b and s are two series of spread value
        df_whole_b = (df_whole['ap1_' + future_pair_code[0]] - df_whole['bp1_' + future_pair_code[1]])
        df_whole_s = (df_whole['bp1_' + future_pair_code[0]] - df_whole['ap1_' + future_pair_code[1]])
        x_min = min(min(df_whole_b.values.tolist()), min(df_whole_s.values.tolist()))
        x_max = max(max(df_whole_b.values.tolist()), max(df_whole_s.values.tolist()))
        plt.xlim(x_min-5, x_max+5)
        
        # plotting
        df_abr_b.hist(bins=66,log=True,color='purple',label='buy {0} sell {1}'.format(future_pair_code[0], future_pair_code[1]),alpha=0.4) 
        df_abr_s.hist(bins=66,log=True,label='buy {0} sell {1}'.format(future_pair_code[1], future_pair_code[0]),alpha=0.6) 
        plt.ylabel('Frequency') 
        plt.xlabel(x_label)

        # Append more statistic features to plot
        # 0 - b , 1 - s
        df_mean = pd.concat([df_abr_b.value_counts(), df_abr_s.value_counts()], axis=1)
        df_mean.fillna(0, inplace=True)
        if len(df_mean) > 0:
            # Mean & Std [seperate]
            mean_0 = df_abr_b.mean()
            std_0 = df_abr_b.std()
            mean_1 = df_abr_s.mean()
            std_1 = df_abr_s.std()
            plt.axvline(mean_0, color='r', linestyle='--', label=str(mean_0))
            plt.axvline(mean_1, color='r', linestyle='--', label=str(mean_1))
            plt.axvline(mean_0 - 2 * std_0, color='b', linestyle=':',label="buy-2σ:" + str(mean_0 - 2 * std_0))
            plt.axvline(mean_1 + 2 * std_1, color='k', linestyle=':',label="sell+2σ:" + str(mean_1 + 2 * std_1))

            # Mean for accessable overlapping ticks (same spread for two series)
            df_mean['count'] = df_mean.apply(lambda x: x[df_mean.columns[0]] if x[df_mean.columns[0]] <= x[df_mean.columns[1]] else x[df_mean.columns[1]], axis=1)
            df_mean['mean'] = df_mean.apply(lambda x: x['count'] * x.name, axis=1)
            mean_value = df_mean['mean'].sum() / df_mean['count'].sum()
            plt.axvline(mean_value, color='y', linestyle='-', label=str(mean_value))
            plt.legend(loc='upper right')
    else:
        pass

    return df_abr_b, df_abr_s


# Grouped
def pair_plot_group(date, future_pair_code_group):
    plt.tight_layout()
    plt.figure(figsize=(18,96))
    length = len(future_pair_code_group)
    for i in range(length):
        for j in range(8):
            plt.subplot(8, length, j*length+i+1)
            pair_plot_section(date, future_pair_code_group[i], j)


def export_parameter_csv(date, future_pair_code_group):
    for pair in future_pair_code_group:
        name = pair[0] + '-' + pair[1]
        df = pd.DataFrame(columns=['date', 'mean(buy)', 'mean(sell)', 'mean-σ(buy)', 'mean-2σ(buy)', 'mean+σ(sell)', 'mean+2σ(sell)'])
        for i in range(3):
            df_b, df_s = pair_plot_section(date, pair, i, plot=False)
            if len(df_b) > 0 and len(df_s) > 0:
                mean_0 = df_b.mean()
                std_0 = df_b.std()
                mean_1 = df_s.mean()
                std_1 = df_s.std()
                df.loc[i] = [date + '_' + str(i), mean_0, mean_1, mean_0 - std_0, mean_0 - 2*std_0, mean_1 + std_1, mean_1 + 2*std_1]
    return df 

# Deprecated and replaced by all contract_pair in database
def get_contract_lst():
    pair_df = pd.DataFrame()
    for root, dirs, files in os.walk(r"Z:\300_Group\HFT\Program\CTA_UI\params", topdown=False):
        for name in files:
            if "params.csv" in os.path.join(root, name):
                pair_df = pd.concat([pair_df, pd.read_csv(os.path.join(root, name))[['first_instrument','second_instrument']]])
    pairs = pair_df[['first_instrument','second_instrument']].drop_duplicates().values.tolist()
    contract_pair_lst = [[rename(x[0]), rename(x[1])] for x in pairs]
    print(len(contract_pair_lst))
    
    return contract_pair_lst


def plot_time_series_single(date:int, contract_pair:list, back_period=30):
    start = trade_day[trade_day.index(date)-back_period]
    bar_plot_time_series(contract_pair, start, date)


def plot_continuous_contract():
    # 画连续合约图像
    date, section = get_date_section()
    date, section = from_predict(date, section)
    date, section = from_predict(date, section)
    bar_plot_continous_data(date, section)


def plot_volume_split():
    # 画连续合约图像
    date, section = get_date_section()
    date, section = from_predict(date, section)
    date, section = from_predict(date, section)
    bar_plot_volume_split_data(date, section)


def plot_time_series(date:int, back_period=30):    
    # 画合约时序图像
    date_dir = os.path.join(os.path.abspath(PLOT_PATH), str(date))
    if not os.path.exists(date_dir):
        os.mkdir(date_dir)
    contract_pair_dict = get_db_contract_pair()
    for key in contract_pair_dict:
        with Pool(5) as p:
            for i in range(0, len(contract_pair_dict[key]), 5):
                contract_pairs = contract_pair_dict[key][i:i+5]
                arg_lst = [(date, x, back_period) for x in contract_pairs]
                p.starmap(plot_time_series_single, arg_lst)
            p.close() 
            p.join()


if __name__ == "__main__":
    data = [['account', 'link', 'from', 'to', 'status'], ['ch3', 'UI -> Cloud', 'D:\\local_repo\\CTA_UI\\params\\ch3\\params.csv', 'CTA/feng/ch3/params.csv', '√'], ['ch3', 'UI -> Cloud', 'D:\\local_repo\\CTA_UI\\sftp_configs\\huajing34.json', 'CTA/feng/ch3/huajing34.json', '√'], ['ch5', 'UI -> Cloud', 'D:\\local_repo\\CTA_UI\\params\\ch5\\params.csv', 'CTA/feng/ch5/params.csv', '√'], ['ch5', 'UI -> Cloud', 'D:\\local_repo\\CTA_UI\\sftp_configs\\huajing34.json', 'CTA/feng/ch5/huajing34.json', '√'], ['ch3', 'UI -> Market', '-', '-', 'SSHException'], ['ch5', 'UI -> Market', '-', '-', 'SSHException'], ['lq', 'UI -> Cloud', 'D:\\local_repo\\CTA_UI\\params\\lq\\params.csv', 'CTA/hh/lq/params.csv', '√'], ['lq', 'UI -> Cloud', 'D:\\local_repo\\CTA_UI\\sftp_configs\\lq.json', 'CTA/hh/lq/lq.json', '√'], ['lq', 'UI -> Market', '-', '-', 'TypeError'], ['lqqq', 'UI -> Cloud', 'D:\\local_repo\\CTA_UI\\params\\lqqq\\params.csv', 'CTA/hh/lqqq/params.csv', '√'], ['lqqq', 'UI -> Cloud', 'D:\\local_repo\\CTA_UI\\sftp_configs\\lqq - 副本.json', 'CTA/hh/lqqq/lqq - 副本.json', '√'], ['lqqq', 'UI -> Market', '-', '-', 'TypeError'], ['lqq', 'UI -> Cloud', 'D:\\local_repo\\CTA_UI\\params\\lqq\\params.csv', 'CTA/hh/lqq/params.csv', '√'], ['lqq', 'UI -> Cloud', 'D:\\local_repo\\CTA_UI\\sftp_configs\\lqq.json', 'CTA/hh/lqq/lqq.json', '√'], ['lqq', 'UI -> Market', '-', '-', 'TypeError'], ['lq', 'UI -> Cloud', 'D:\\local_repo\\CTA_UI\\params\\lq\\params.csv', 'CTA/hh/lq/params.csv', '√'], ['lq', 'UI -> Cloud', 'D:\\local_repo\\CTA_UI\\sftp_configs\\lq_market_test.json', 'CTA/hh/lq/lq_market_test.json', '√'], ['lq', 'UI -> Market', '-', '-', 'SSHException']]
    print(str(create_aligned_table(data)))
