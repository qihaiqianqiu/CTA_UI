import os
import heapq
import time
import traceback
import datetime
import sys
import subprocess

BASE_DIR = os.path.dirname(os.path.realpath(sys.argv[0]))

def delete_glog_limit():
    # delete glog
    glog_dir = os.path.join(BASE_DIR, "glog")
    modify_time = [(os.path.getmtime(os.path.join(glog_dir, f)), os.path.join(glog_dir, f)) for f in os.listdir(glog_dir) if os.path.isfile(os.path.join(glog_dir, f))]
    newest = heapq.nlargest(5, modify_time)
    # 保留最新的5个交易日志
    remain_lst = [file for time, file in newest]
    for f in os.listdir(glog_dir):
        if os.path.join(glog_dir, f) not in remain_lst:
            os.remove(os.path.join(glog_dir, f))
            print("删除glog文件", os.path.join(glog_dir, f))
            
    limit_dir = os.path.join(BASE_DIR, "")
    limit_time = [(os.path.getmtime(os.path.join(limit_dir, f)), os.path.join(limit_dir, f)) for f in os.listdir(limit_dir) if os.path.isfile(os.path.join(limit_dir, f)) and "LimitValue" in f]
    newest = heapq.nlargest(5, limit_time)
    remain_lst = [file for time, file in newest]
    for f in os.listdir(limit_dir):
        if "LimitValue" in f and os.path.join(limit_dir, f) not in remain_lst:
            os.remove(os.path.join(limit_dir, f))
            print("删除LimitValue文件", os.path.join(limit_dir, f))

def init_CTP(exe):
    subprocess.Popen(os.path.join(BASE_DIR, exe))

def shutdown_CTP(exe):
    subprocess.call(["taskkill", "/F", "/IM", os.path.join(BASE_DIR, exe)])


            

night = 0
morning = 0
afternoon = 0
while True:
    try:
        HMtime = time.strftime("%H:%M", time.localtime())
        print("Current time:", HMtime)
        # 夜盘开盘 启动程序
        if HMtime > '20:55' and HMtime < '20:59' and not night:
            init_CTP("CTPtest-test.exe")
            night = 1
            print("夜盘开盘")
        # 夜盘收盘 关闭程序 删除glog limit
        if HMtime > '02:31' and HMtime < '08:50' and night:
            shutdown_CTP("CTPtest-test.exe")
            delete_glog_limit()
            night = 0
            print("夜盘收盘")
        # 早盘开盘 启动程序
        if HMtime > '08:55' and HMtime < '08:59' and not morning:
            init_CTP("CTPtest-test.exe")
            morning = 1
            print("早盘开盘")
        # 早盘收盘 关闭程序 删除glog limit
        if HMtime > '11:31' and HMtime < '13:20' and morning:
            shutdown_CTP("CTPtest-test.exe")
            delete_glog_limit()
            morning = 0
            print("早盘收盘")
        # 午盘开盘 启动程序
        if HMtime > '13:25' and HMtime < '13:29' and not afternoon:
            init_CTP("CTPtest-test.exe")
            afternoon = 1
            print("午盘开盘")
        # 午盘收盘 关闭程序 删除glog limit
        if HMtime > '15:00' and HMtime < '20:55' and afternoon:
            shutdown_CTP("CTPtest-test.exe")
            delete_glog_limit()
            init_CTP("CTPtest-GetPosAndTrd.exe")
            afternoon = 0
            print("午盘收盘")
            time.sleep(10)
            shutdown_CTP("CTPtest-GetPosAndTrd.exe")
        print("交易服务器监听中...")
        print("监控目录:", BASE_DIR)
        time.sleep(15)
    except Exception as e:
        error_info = traceback.format_exc()
        with open(os.path.join(BASE_DIR, "trading_monitor_error_log.txt", "a+", encoding='utf-8')) as err_file:
                err_file.write(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S') + '\n')
                err_file.write(error_info + '\n')
        time.sleep(15)
    
            