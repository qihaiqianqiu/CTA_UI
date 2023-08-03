import time
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
import pandas as pd
import os
import datetime
import time
from utils.path_exp_switch import *
from utils.flink import set_up_ssh_reverse_tunnel, pull_from_market_to_trading, pull_from_market_to_cloud, request_from_trading_to_market, request_from_cloud_to_UI
import threading
import json
import traceback
import re
import sys

__all__ = ["invoke_monitor"]

# 文件监视器部分 - file_monitor.py
class fileMonitor(FileSystemEventHandler):
    """检测某一根目录下的文件变化
        每隔5分钟，读取目录下存放的所有CSV文件，缓存DataFrame进内存。
        发生对比时进行更改，然后保存到文件。
    Args:
        FileSystemEventHandler (_type_): 
        path:根目录路径
        is_market:是否是市场端
        is_UI:是否是UI端
        是否是云端可以通过is_market和is_UI来判断
        使用不同的flag部署在不同的机器上
    """
    def __init__(self, path, is_market, is_UI) -> None:
        super().__init__()
        self.path = path
        self.cache = self.cache_all_csv_files()
        self.is_market = is_market
        self.is_UI = is_UI
        self.config_files = []
        print("开始监听目录：", self.path)
        # 检查sftp_configs目录下的所有配置文件
        for root, dirs, files in os.walk(os.path.join(self.path, "sftp_configs")):
            for file in files:
                if file.endswith(".json") and file != "limit.json" and file != "config.json" and file != "configure.json":
                    self.config_files.append(os.path.join(root, file))
        if is_market:
            threading.Thread(target=self.planned_time_market).start()
            if is_market:
                self.set_up_ssh_tunnel()
            
        

    # 查找属性path目录下的所有链路配置文件
    def set_up_ssh_tunnel(self):
        if self.is_market:
            for config in self.config_files:
                # 检查是否需要使用SSH隧道
                ftp_config = json.load(open(config))
                if ftp_config["marketServer"]["host"] != "localhost":
                    threading.Thread(target=set_up_ssh_reverse_tunnel, args=(config,)).start()
        else:
            pass
        
    def get_all_csv_files(self):
        csv_files = []
        for root, dirs, files in os.walk(self.path):
            for file in files:
                if file == "params.csv":
                    csv_files.append(os.path.join(root, file))
        return csv_files

    def cache_all_csv_files(self):
        # 缓存所有参数表文件，用以计算差异
        csv_files = self.get_all_csv_files()
        self.cache = {}
        for file in csv_files:
            print("缓存参数表文件：", file)
            self.cache[file] = pd.read_csv(file)
        return self.cache
        
    def compare_csv_file(self, new_df, old_df, index_columns='pairs_id', suff=('_new', '_old')):
        log_txt = ""
        if not len(old_df) == 0:
            new_df = new_df.reset_index(drop=True).set_index(index_columns)
            old_df = old_df.reset_index(drop=True).set_index(index_columns)
            # 添加、删除套利对检测
            append_pairs = [pairs for pairs in new_df.index if pairs not in old_df.index]
            remove_pairs = [pairs for pairs in old_df.index if pairs not in new_df.index]
            if len(append_pairs):
                for pairs in append_pairs:
                    log_txt += "    +++添加套利对: " + pairs + '\n'
            if len(remove_pairs):
                for pairs in remove_pairs:
                    log_txt += "    ---删除套利对: " + pairs + '\n'
            # 参数值修改检测
            modified = pd.merge(new_df, old_df, how='inner', left_index=True, right_index=True, suffixes=suff).fillna(0)
            string_col = ['indate_date', 'first_instrument', 'second_instrument', 'prime_instrument', 'kind', 'SP_InstrumentID']
            suf_a = suff[0]
            suf_b = suff[1]
            for col in modified.columns:
                if col.endswith(suf_a):
                    col_b = col.replace(suf_a, suf_b)
                    if col.replace(suf_a, '') not in string_col:
                        modified[col] = modified[col].astype(float)
                        modified[col_b] = modified[col_b].astype(float)
                    else:
                        modified[col] = modified[col].astype(str)
                        modified[col_b] = modified[col_b].astype(str)
                    modified[col.replace(suf_a, '')] = modified.apply(lambda x: 0 if x[col] == x[col_b] else str(x[col_b]) + '->' + str(x[col]), axis=1)
                    modified.drop(columns=[col, col_b], inplace=True)
            modified = modified[modified != 0].dropna(axis=0, how='all').dropna(axis=1, how='all').fillna(0)
        
            # 生成修改日志
            for index, row in modified.iterrows():
                log_txt += "    修改套利对: " + index + '\n'
                for col, value in row.items():
                    if value != 0:
                        log_txt +=  "        [" +  col + "] 修改前: " + value.split('->')[0] +  " 修改后: " + value.split('->')[1] + '\n'
        else:
            log_txt += "    首次上传参数表  " + '\n'

        return log_txt
    
    
    def on_created(self, event):
        f = open(os.path.join(self.path, 'changelog.txt'), 'a+')
        # 文件增删处理
        if not event.is_directory and event.src_path.endswith(".csv") and event.event_type == 'created':
            file_path = event.src_path  # 获取变化的文件路径
            print("检测到文件增删：", file_path)
            # 如果是交易日志文件，转发到服务器
            file_name = os.path.basename(file_path)
            acc = os.path.basename(os.path.dirname(file_path))
            try:
                if re.search(r'holding_\d{6}\.csv', file_name) or re.search(r'trading_\d{6}\.csv', file_name):
                    print("文件为交易日志文件" + file_name + "转发到云服务器")
                    # 找到其所属账户 
                    for config in self.config_files:
                        acc_lst = json.load(open(config))['accountList']
                        if acc in acc_lst:
                            pull_from_market_to_cloud(config, file_name)
                            break
            except Exception as e:
                error_info = traceback.format_exc()
                print(error_info)
                    
                
    def on_modified(self, event):
        if self.is_UI:
            return
        f = open(os.path.join(self.path, 'changelog.txt'), 'a+')  # 记录日志
        # 文件变化处理
        if not event.is_directory and event.src_path.endswith(".csv") and event.event_type == 'modified':
            file_path = event.src_path  # 获取变化的文件路径
            print("检测到文件修改：", file_path)
            # 计算文件变化
            file_name = os.path.basename(file_path)
            acc = os.path.basename(os.path.dirname(file_path))
            try:
                if file_name == "limit.json" or "config.json" or "configure.json" or "configure.xml":
                    pass
                
                if file_name == "params.csv":
                    f.write(datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')+'\n')
                    f.write(file_path+'\n')
                    time.sleep(3)
                    new_df = pd.read_csv(file_path)
                    print("read_new_df_success")
                    if file_path not in self.cache:
                        old_df = pd.DataFrame() 
                    else:
                        old_df = self.cache[file_path]
                    modify_log = self.compare_csv_file(new_df, old_df)
                    # 在此处添加你要执行的操作，如运行其他脚本或发送通知等
                    print("已记录文件 {} 发生变化".format(file_path))
                    f.write(modify_log)
                    f.write("==================================================\n") 
                    # 重新缓存文件
                    self.cache_all_csv_files()
                    # 行情端 -> 交易端
                    if self.is_market:
                        # 读取对应的链路配置文件
                        for config in self.config_files:
                            acc_lst = json.load(open(config))['accountList']
                            if acc in acc_lst:
                                pull_from_market_to_trading(config)
                                break
                        print("@成功从行情端推送至交易端:", file_path)
                        
                if re.search(r'holding_\d{6}\.csv', file_name) or re.search(r'trading_\d{6}\.csv', file_name):
                    print("文件为交易日志文件" + file_name + "转发到云服务器")
                    # 行情端 -> 云端
                    if self.is_market:
                        # 找到其所属账户 
                        for config in self.config_files:
                            acc_lst = json.load(open(config))['accountList']
                            if acc in acc_lst:
                                pull_from_market_to_cloud(config, file_name)
                                break
                        print("@成功从行情端推送至云端:", file_path)
            
            except Exception as e:
                error_info = traceback.format_exc()
                print(error_info)
        f.close()
        
        
    def planned_time_market(self):
        if_quest = False
        error_log = [["Account", "Link", "From", "To", "Status"]]

        def process_config(config):
            # 根据行情服务器host判断链路形式
            # 1. 本地行情服务器
            try:
                logger = request_from_trading_to_market(config)
                for log in logger:
                    error_log.append(log)
            except Exception as e:
                error_acc_lst = json.load(open(config))['accountList']
                for acc in error_acc_lst:
                    error_log.append([acc, "Trading -> UI", "-", "-", type(e).__name__])
            
            
        while True:
            current_time = time.strftime("%H:%M", time.localtime())
            print("当前时间：", current_time)
            if not if_quest:
                if current_time > "15:01" and current_time < "15:05": 
                    try:
                        for config in self.config_files:
                            print("@processing", config)
                            process_config(config)
                        print("@成功下载日志文件")
                    except Exception as e:
                        print(traceback.format_exc())
                    error_log = [["Account", "Link", "From", "To", "Status"]]
                    if_quest = True
            # 新的一天之后 重置if_quest
            if current_time > "00:00" and current_time < "00:05" and if_quest:
                if_quest = False
            time.sleep(30)


def invoke_monitor(target_path, is_market, is_UI):
    ## 尝试配置为行情端服务器
    #path = "./"  # 被监视的目录路径
    event_handler = fileMonitor(path=target_path, is_market=is_market, is_UI=is_UI)
    observer = Observer()
    observer.schedule(event_handler, target_path, recursive=True)
    observer.start()

    try:
        while True:
            print("监听中...")
            time.sleep(30)
    except KeyboardInterrupt:
        observer.stop()
        observer.join()
        
if __name__ == "__main__":
    invoke_monitor(target_path="D:\\CTA_mkt\\", is_market=True, is_UI=False)