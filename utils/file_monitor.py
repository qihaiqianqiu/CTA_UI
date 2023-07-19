import time
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
import pandas as pd
import os
import datetime
import time
from utils.path_exp_switch import *
from utils.flink import *
import threading
import asyncio
import traceback


class fileMonitor(FileSystemEventHandler):
    """检测某一根目录下的文件变化
        每隔5分钟，读取目录下存放的所有CSV文件，缓存DataFrame进内存。
        发生对比时进行更改，然后保存到文件。
    Args:
        FileSystemEventHandler (_type_): 
        path:根目录路径
        logger:日志记录器
    """
    def __init__(self, path, is_market) -> None:
        super().__init__()
        self.path = path
        self.cache = self.cache_all_csv_files()
        self.is_market = is_market
        self.set_up_ssh_tunnel()
        self.processed_files ={}
    
    def set_up_ssh_tunnel(self):
        if self.is_market:
            config_files = []
            for root, dirs, files in os.walk(self.path):
                for file in files:
                    if file.endswith(".json"):
                        config_files.append(os.path.join(root, file))
            for config in config_files:
                threading.Thread(target=set_up_ssh_reverse_tunnel, args=(config,)).start()

        else:
            pass
        
    def get_all_csv_files(self):
        csv_files = []
        for root, dirs, files in os.walk(self.path):
            for file in files:
                if file.endswith(".csv"):
                    csv_files.append(os.path.join(root, file))
        return csv_files

    def cache_all_csv_files(self):
        csv_files = self.get_all_csv_files()
        self.cache = {}
        for file in csv_files:
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
                        log_txt +=  "        ###" +  col + " 修改前: " + value.split('->')[0] +  " 修改后: " + value.split('->')[1] + '\n'
        else:
            log_txt += "    首次上传参数表  " + '\n'

        return log_txt
        
    def on_modified(self, event):
        f = open(os.path.join(self.path, 'changelog.txt'), 'a+')  # 记录日志
        if not event.is_directory and event.src_path.endswith(".csv") and event.event_type == 'modified':
            if event.src_path in self.processed_files:
                self.processed_files[event.src_path] = False
            if self.processed_files[event.src_path]:
                return
            print("EVENT INFORMATION: ", event)
            file_path = event.src_path  # 获取变化的文件路径
            print("检测到文件修改：", file_path)
            # 计算文件变化
            try:
                if file_path.endswith(".csv"):
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
                    f.write("############################################\n") 
                    # 重新缓存文件
                    self.cache_all_csv_files()
                    # 行情端 -> 交易端
                    if self.is_market:
                        # 读取对应的链路配置文件
                        acc_dir = os.path.dirname(file_path)
                        print("在路径", acc_dir, "下查找链路配置文件")
                        config_file = [f for f in os.listdir(acc_dir) if f.endswith('.json')][0]
                        print("找到链路配置文件：", config_file)
                        pull_from_market_to_trading(os.path.join(acc_dir, config_file))
                        print("@成功从行情端推送至交易端:", file_path)
                    self.processed_files[event.src_path] = True
            except Exception as e:
                error_info = traceback.format_exc()
                print(error_info)
        f.close()
        



if __name__ == "__main__":
    path = r"D:\CTA_mkt"  # 被监视的目录路径
    event_handler = fileMonitor(path, is_market=True)
    observer = Observer()
    observer.schedule(event_handler, path, recursive=True)
    observer.start()

    try:
        while True:
            print("监听中...")
            time.sleep(20)
    except KeyboardInterrupt:
        observer.stop()
        observer.join()