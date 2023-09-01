# -*- coding: utf-8 -*-
import os
import heapq
import time
import traceback
import datetime
import sys
import subprocess
import shutil

op_type = sys.platform
BASE_DIR = os.path.dirname(os.path.realpath(sys.argv[0]))

# 处理往期日志文件：glog.runlog.limitValue
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
    
    runlog_dir = os.path.join(BASE_DIR, "RunLog")
    if not os.path.exists(runlog_dir):
        os.makedirs(runlog_dir)
    # 将limit文件夹下的文件移动到runlog文件夹下
    runlog_outside = [f for f in os.listdir(limit_dir) if os.path.isfile(os.path.join(limit_dir, f)) and "RunLog" in f]
    for runlog in runlog_outside:
        try:
            shutil.move(os.path.join(limit_dir, runlog), os.path.join(runlog_dir, runlog))
        except Exception as e:
            error_info = traceback.format_exc()
            with open(os.path.join(BASE_DIR, "trading_monitor_error_log.txt"), "a+", encoding='utf-8') as err_file:
                    err_file.write(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S') + '\n')
                    err_file.write(error_info + '\n')
            time.sleep(1)
            continue
                
            

def init_CTP(exe_type):
    if op_type == "win32":
        if exe_type == "TRADE":
            subprocess.Popen(os.path.join(BASE_DIR, "CTPtest-test.exe"))
        if exe_type == "GETPOS":
            subprocess.Popen(os.path.join(BASE_DIR, "CTPtest-GetPosAndTrd.exe"))
    else:
        if exe_type == "TRADE":
            print("Start Init Linux tradinghost:", os.path.join(BASE_DIR, "ctp-test"))
            subprocess.Popen(os.path.join(BASE_DIR, "ctp-test"))
            print("Init Linux tradinghost success:", os.path.join(BASE_DIR, "ctp-test"))
        if exe_type == "GETPOS":
            subprocess.Popen(os.path.join(BASE_DIR, "ctp-getposandtrd"))

def shutdown_CTP():
    if op_type == "win32":
        subprocess.call(["taskkill", "/F", "/IM", "CTPtest-test.exe"])
    else:
        subprocess.call(["killall", "-9", "ctp-test"])


            

night = 0
morning = 0
afternoon = 0
afternoon_pre = 0
counter = 0

f = open("monitor_log.txt", "a+", encoding="utf-8")
         
while True:
    HMtime = time.strftime("%H:%M:%S", time.localtime())
    # 夜盘开盘 启动程序
    if (HMtime > '20:59:59' or HMtime < '02:30:00') and not night:
        init_CTP("TRADE")
        night = 1
        print("夜盘开盘")
        f.write(str(HMtime) + ": Night trading start\n")
    # 夜盘收盘 关闭程序 删除glog limit
    if HMtime > '02:35:00' and HMtime < '08:50:00' and night:
        shutdown_CTP()
        #等待20秒，解除对日志文件的读写 
        time.sleep(20)
        delete_glog_limit()
        night = 0
        print("夜盘收盘")
        f.write(str(HMtime) + ": Night trading end\n")
    # 早盘开盘 启动程序
    # 早盘特殊挂单时间 08:59:54
    if HMtime > '08:59:59' and HMtime < '11:30:00' and not morning:
        init_CTP("TRADE")
        morning = 1
        print("早盘开盘")
        f.write(str(HMtime) + ": Morning trading start\n")
    # 早盘收盘 关闭程序 删除glog limit
    if HMtime > '11:35:00' and HMtime < '12:50:00' and morning:
        shutdown_CTP()
        #等待20秒，解除对日志文件的读写 
        time.sleep(20)
        delete_glog_limit()
        morning = 0
        print("早盘收盘")
        f.write(str(HMtime) + ": Morning trading end\n")
    # 午盘开盘 启动程序
    if HMtime > '12:59:54' and HMtime < '15:00:00' and not afternoon:
        init_CTP("TRADE")
        afternoon = 1
        afternoon_pre = 1
        print("午盘开盘")
        f.write(str(HMtime) + ": Afternoon trading start\n")
    # 午盘收盘 关闭程序 删除glog limit
    # 先取交易记录
    if HMtime > '14:55:00' and HMtime < '14:59:59' and afternoon_pre:
        init_CTP("GETPOS")
        print("获取交易记录")
        afternoon_pre = 0
        f.write(str(HMtime) + ": Get PosAndTrading in advance\n")
    # 多等15秒吧
    if HMtime > '15:00:10' and HMtime < '20:55:00' and afternoon:
        init_CTP("GETPOS")
        print("获取交易记录")
        shutdown_CTP()
        f.write(str(HMtime) + ": Afternoon trading end\n")
        #等待60秒，解除对日志文件的读写 
        time.sleep(60)
        delete_glog_limit()
        afternoon = 0
        print("午盘收盘")
    counter += 1
    if counter == 60:
        print("Current time:", HMtime)
        print("交易服务器监听中...")
        counter = 0
        f.write(str(HMtime) + ": reset monitor\n")
    time.sleep(1)

    
            