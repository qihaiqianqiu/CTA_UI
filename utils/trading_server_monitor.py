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
            try:
                os.remove(os.path.join(glog_dir, f))
                print("删除glog文件", os.path.join(glog_dir, f))
            except Exception as e:
                error_info = traceback.format_exc()
                with open(os.path.join(BASE_DIR, "trading_monitor_error_log.txt"), "a+", encoding='utf-8') as err_file:
                        err_file.write(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S') + '\n')
                        err_file.write(error_info + '\n')
                time.sleep(1)
                continue
                
            
    limit_dir = os.path.join(BASE_DIR, "")
    limit_time = [(os.path.getmtime(os.path.join(limit_dir, f)), os.path.join(limit_dir, f)) for f in os.listdir(limit_dir) if os.path.isfile(os.path.join(limit_dir, f)) and "LimitValue" in f]
    newest = heapq.nlargest(5, limit_time)
    remain_lst = [file for time, file in newest]
    for f in os.listdir(limit_dir):
        if "LimitValue" in f and os.path.join(limit_dir, f) not in remain_lst:
            try:
                os.remove(os.path.join(limit_dir, f))
                print("删除LimitValue文件", os.path.join(limit_dir, f))
            except Exception as e:
                error_info = traceback.format_exc()
                with open(os.path.join(BASE_DIR, "trading_monitor_error_log.txt"), "a+", encoding='utf-8') as err_file:
                        err_file.write(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S') + '\n')
                        err_file.write(error_info + '\n')
                time.sleep(1)
                continue
            
            

def init_CTP(exe):
    subprocess.Popen(os.path.join(BASE_DIR, exe))

def shutdown_CTP(exe):
    subprocess.call(["taskkill", "/F", "/IM", exe])


            

night = 0
morning = 0
afternoon = 0
counter = 0
while True:
    HMtime = time.strftime("%H:%M:%S", time.localtime())
    # 夜盘开盘 启动程序
    if (HMtime > '20:55:00' or HMtime < '02:30:00') and not night:
        init_CTP("CTPtest-test.exe")
        night = 1
        print("夜盘开盘")
    # 夜盘收盘 关闭程序 删除glog limit
    if HMtime > '02:35:00' and HMtime < '08:50:00' and night:
        shutdown_CTP("CTPtest-test.exe")
        #等待20秒，解除对日志文件的读写 
        time.sleep(20)
        delete_glog_limit()
        night = 0
        print("夜盘收盘")
    # 早盘开盘 启动程序
    # 早盘特殊挂单时间 08:59:55
    if HMtime > '08:59:55' and HMtime < '11:30:00' and not morning:
        init_CTP("CTPtest-test.exe")
        morning = 1
        print("早盘开盘")
    # 早盘收盘 关闭程序 删除glog limit
    if HMtime > '11:35:00' and HMtime < '13:20:00' and morning:
        shutdown_CTP("CTPtest-test.exe")
        #等待20秒，解除对日志文件的读写 
        time.sleep(20)
        delete_glog_limit()
        morning = 0
        print("早盘收盘")
    # 午盘开盘 启动程序
    if HMtime > '13:25:00' and HMtime < '15:00:00' and not afternoon:
        init_CTP("CTPtest-test.exe")
        afternoon = 1
        print("午盘开盘")
    # 午盘收盘 关闭程序 删除glog limit
    if HMtime > '15:00:00' and HMtime < '20:55:00' and afternoon:
        init_CTP("CTPtest-GetPosAndTrd.exe")
        shutdown_CTP("CTPtest-test.exe")
        #等待60秒，解除对日志文件的读写 
        time.sleep(60)
        delete_glog_limit()
        afternoon = 0
        print("午盘收盘")
        shutdown_CTP("CTPtest-GetPosAndTrd.exe")
    counter += 1
    if counter == 60:
        print("Current time:", HMtime)
        print("交易服务器监听中...")
        counter = 0
    time.sleep(1)

    
            