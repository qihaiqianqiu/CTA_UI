import time
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
import pandas as pd
import os
import datetime
import sftp
import json
import time
from utils.path_exp_switch import *
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
    def __init__(self, path) -> None:
        super().__init__()
        self.path = path
        self.cache = self.cache_all_csv_files()
        self.last_modified = 0
        
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
    
    def diff_dataframe(df:pd.DataFrame, suffixes=('_new', '_old')):
        string_col = ['indate_date', 'first_instrument', 'second_instrument', 'prime_instrument', 'kind', 'SP_InstrumentID']
        suf_a = suffixes[0]
        suf_b = suffixes[1]
        for col in df.columns:
            if col.endswith(suf_a):
                col_b = col.replace(suf_a, suf_b)
                df[col] = df[col].astype(str)
                df[col_b] = df[col_b].astype(str)
                df[col.replace(suf_a, '')] = df.apply(lambda x: 0 if x[col] == x[col_b] else x[col_b] + '->' + x[col], axis=1)
                df.drop(columns=[col, col_b], inplace=True)
                
        print("after diff:", df)
        return df
        
        
    def compare_csv_file(self, new_df, old_df, index_columns='pairs_id', suff=('_new', '_old')):
        log_txt = ""
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
        return log_txt
        
    def on_modified(self, event):
        f = open(os.path.join(self.path, 'changelog.txt'), 'a+')  # 记录日志
        current_time = time.time()
        if current_time - self.last_modified > 1:
            print("时间差满足要求")
            if event.is_directory:
                return
            file_path = event.src_path  # 获取变化的文件路径
            print(file_path)
            # 计算文件变化
            if file_path.endswith(".csv"):
                f.write(datetime.datetime.fromtimestamp(current_time).strftime('%Y-%m-%d %H:%M:%S')+'\n')
                f.write(file_path+'\n')
                new_df = pd.read_csv(file_path)
                old_df = self.cache[file_path]
                modify_log = self.compare_csv_file(new_df, old_df)
                # 在此处添加你要执行的操作，如运行其他脚本或发送通知等
                print("文件 {} 发生变化".format(file_path))
                f.write(modify_log)
                # 重新缓存文件
                self.cache_all_csv_files()
        f.close()
        self.last_modified = current_time


if __name__ == "__main__":
    path = r"D:\local_repo\CTA_UI\params"  # 被监视的目录路径
    event_handler = fileMonitor(path)
    observer = Observer()
    observer.schedule(event_handler, path, recursive=True)
    observer.start()

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        observer.stop()
        observer.join()