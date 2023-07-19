import time
import json
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
import pandas as pd
import os
import datetime
import time
import threading
import asyncio
import traceback
import paramiko
import socket
import select
import uuid

def windows_to_linux(path):
    return path.replace("\\", "/")

def linux_to_windows(path):
    return path.replace("/", "\\")

ROOT_PATH = ""

all = ["SSHConnection"] 
class SSHConnection(object):
 
    def __init__(self, host='192.168.88.182', port=22, username='root',pwd='123456'):
        self.host = host
        self.port = port
        self.username = username
        self.pwd = pwd
        self.__k = None
 
    def connect(self):
        transport = paramiko.Transport((self.host,self.port))
        transport.connect(username=self.username,password=self.pwd)
        self.__transport = transport
 
    def close(self):
        self.__transport.close()
 
    def upload(self,local_path,target_path):
        # 连接，上传
        # file_name = self.create_file()
        sftp = paramiko.SFTPClient.from_transport(self.__transport)
        # 将location.py 上传至服务器 /tmp/test.py
        sftp.put(local_path, target_path)
 
    def download(self,remote_path,local_path):
        sftp = paramiko.SFTPClient.from_transport(self.__transport)
        sftp.get(remote_path,local_path)
        
    def reverse_forward_tunnel(self, server_port, remote_host, remote_port):
        def handler(chan, host, port):
            sock = socket.socket()
            try:
                sock.connect((host, port))
            except Exception as e:
                print('Forwarding request to %s:%d failed: %r' % (host, port, e))
                return
            
            print('Connected!  Tunnel open %r -> %r -> %r' % (chan.origin_addr,
                                                                chan.getpeername(), (host, port)))
            while True:
                r, w, x = select.select([sock, chan], [], [])
                if sock in r:
                    data = sock.recv(1024)
                    if len(data) == 0:
                        break
                    chan.send(data)
                if chan in r:
                    data = chan.recv(1024)
                    if len(data) == 0:
                        break
                    sock.send(data)
            chan.close()
            sock.close()
            print('Tunnel closed from %r' % (chan.origin_addr,))
        self.__transport.set_keepalive(30)
        self.__transport.request_port_forward('', server_port)
        while True:
            chan = self.__transport.accept(1000)
            if chan is None:
                continue
            thr = threading.Thread(target=handler, args=(chan, remote_host, remote_port))
            thr.setDaemon(True)
            thr.start()
        
    def cmd(self, command):
        ssh = paramiko.SSHClient()
        ssh._transport = self.__transport
        # 执行命令
        stdin, stdout, stderr = ssh.exec_command(command)
        # 获取命令结果
        result = stdout.read()
        print (str(result,encoding='utf-8'))
        return result

def pull_from_UI_to_cloud(config_file):
    """
    从UI端向云服务器传送参数表，链路配置
    Args:
        config_file (_type_): _description_
    """
    ftp_config_dir = os.path.join(ROOT_PATH, "sftp_configs", config_file)
    ftp_config = json.load(open(ftp_config_dir))
    cloud_server_para = ftp_config["cloudServer"]
    ssh = SSHConnection(host=cloud_server_para['host'], port=cloud_server_para['port'],
                             username=cloud_server_para['username'], pwd=cloud_server_para['pwd'])
    ssh.connect()
    # 参数表推送
    username = ftp_config["userName"]
    account_list = ftp_config["accountList"]
    dest_user_dir = os.path.join("CTA", username)
    ssh.cmd("mkdir -p " + windows_to_linux(dest_user_dir))
    for acc in account_list:
        # 首先，保证云服务器端建立相应的存储目录
        dest_acc_dir = os.path.join(dest_user_dir, acc)
        ssh.cmd("mkdir -p " + windows_to_linux(dest_acc_dir))
        # 上传参数表, 链路配置表
        param_dir = os.path.join(ROOT_PATH, "params", acc, "params.csv")
        print(param_dir)
        ssh.upload(param_dir, windows_to_linux(os.path.join(dest_acc_dir, "params.csv")))
        ssh.upload(ftp_config_dir, windows_to_linux(os.path.join(dest_acc_dir, config_file)))
        print("参数表成功上传至云端", dest_acc_dir)
    ssh.close()

def pull_from_UI_to_market(config_file):
    """
    从UI端向行情服务器传送参数表，链路配置
    Args:
        config_file (_type_): _description_
    """
    ftp_config_dir = os.path.join(ROOT_PATH, "sftp_configs", config_file)
    ftp_config = json.load(open(ftp_config_dir))
    cloud_server_para = ftp_config["cloudServer"]
    market_server_para = ftp_config["marketServer"]
    
    ssh = SSHConnection(host=cloud_server_para['host'], port=cloud_server_para['reverse_port'],
                             username=market_server_para['username'], pwd=market_server_para['pwd'])
    ssh.connect()
    # 参数表推送
    username = ftp_config["userName"]
    account_list = ftp_config["accountList"]
    dest_user_dir = os.path.join(market_server_para["mktDir"], username)
    ssh.cmd("mkdir " + dest_user_dir)
    for acc in account_list:
        # 首先，保证行情服务器端建立相应的存储目录
        dest_acc_dir = os.path.join(dest_user_dir, acc)
        ssh.cmd("mkdir " + dest_acc_dir)
        # 上传参数表, 链路配置表
        param_dir = os.path.join(ROOT_PATH, "params", acc, "params.csv")
        print(param_dir)
        ssh.upload(param_dir, os.path.join(dest_acc_dir, "params.csv"))
        ssh.upload(ftp_config_dir, os.path.join(dest_acc_dir, config_file))
        print("参数表成功上传至行情端", dest_acc_dir)
    ssh.close()
        
"""
Relevant Terminal
SSH_reverse_tunnel = "ssh -R 9876:localhost:22 root@39.97.106.35"

rsync_example = "rsync -avPz --port 8730 --password-file=/cygdrive/C/Users/Han.Hao/AppData/Local/cwrsync/bin/cta_password.txt root@39.97.106.35::cta/ /cygdrive/C/Users/Han.Hao/test"
# "rsync_pwd_path" & "rsync dest path"
"""
# 以下部署在行情服务器上 CTA目录下【同步自云服务器】
# 行情服务器的方法挂载在后台实时运行
# 获取根目录下的所有config并推送参数表
def set_up_ssh_reverse_tunnel(config_file):
    ftp_config = json.load(open(os.path.join(ROOT_PATH, "sftp_configs", config_file)))
    cloud_server_para = ftp_config["cloudServer"]
    ssh = SSHConnection(host=cloud_server_para['host'], port=cloud_server_para['port'],
                              username=cloud_server_para['username'], pwd=cloud_server_para['pwd'])
    ssh.connect()
    print("SSH Tunnel Connected")
    ssh.reverse_forward_tunnel(cloud_server_para['reverse_port'], ftp_config['marketServer']['host'], ftp_config['marketServer']['port'])
    
def pull_from_market_to_trading(config_file):
    """从行情服务器向交易服务器传送参数表"""
    # 获取config文件所在的目录，找到对应交易员与账户
    ftp_config = json.load(open(config_file))
    trade_server_para = ftp_config["tradeServer"]
    ssh = SSHConnection(host=trade_server_para['host'], port=trade_server_para['port'],
                             username=trade_server_para['username'], pwd=trade_server_para['pwd'])
    ssh.connect()
    # 参数表推送
    username = ftp_config["userName"]
    mkt_dir = ftp_config["marketServer"]["mktDir"]
    account_list = ftp_config["accountList"]
    trade_dir_list = ftp_config["tradeDirList"]
    for idx in range(len(trade_dir_list)):
        ssh.cmd("mkdir " + trade_dir_list[idx])
        # 推送参数表到交易端
        market_param_dir = os.path.join(mkt_dir, username, account_list[idx], "params.csv")
        trade_param_dir = os.path.join(trade_dir_list[idx], "params.csv")
        print("从行情端获取参数表", market_param_dir, "上传至交易端", trade_param_dir)
        ssh.upload(market_param_dir, trade_param_dir)
        print("行情服务器参数表成功上传至交易端", trade_param_dir)    
    ssh.close() 
    
def request_from_market_to_cloud(config_file):
    """行情服务器从云服务器获取最新的参数表与链路信息"""
    # 根目录 行情服务器的CTA文件夹
    ftp_config = json.load(open(os.path.join(ROOT_PATH, "sftp_configs", config_file)))
    cloud_server_para = ftp_config["cloudServer"]
    ssh = SSHConnection(host=cloud_server_para['host'], port=cloud_server_para['port'],
                             username=cloud_server_para['username'], pwd=cloud_server_para['pwd'])
    ssh.connect()
    username = ftp_config['userName']
    cta_cloud_dir = os.path.join("CTA", username)
    os.system("mkdir " + username)
    # 获取最新的链路配置表
    ssh.download(windows_to_linux(os.path.join(cta_cloud_dir, config_file)), os.path.join(username, config_file))
    for acc in ftp_config['accountList']:
        acc_cloud_dir = os.path.join(cta_cloud_dir, acc)
        acc_dest_dir = os.path.join(username, acc)
        # 首先保证本地目录已经创建
        os.system("mkdir " + acc_dest_dir)
        # 获取最新的参数表
        ssh.download(windows_to_linux(os.path.join(acc_cloud_dir, "params.csv")), os.path.join(acc_dest_dir, "params.csv"))
        print("云服务器参数表成功下载至行情端", acc_dest_dir)
    ssh.close()    

def request_from_trading_to_market(config_file):
    pass

def pull_from_cloud_to_market(config_file):
    pass

"""
async def config_task(config):
    f = open("error_log.txt", "a+")
    while True:
        try:
            request_from_market_cloud(config)
            pull_from_market_to_trading(config)
        except Exception as e:
            f.write(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S') + '\n')
            error_info = traceback.format_exc()
            f.write(str(error_info))
        await asyncio.sleep(1)


async def main():
    config_lst = [f for f in os.listdir("sftp_configs") if ".json" in f]
    for config in config_lst:
        await config_task(config)
    
asyncio.run(main())        
"""

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
    event_handler = fileMonitor(path, is_market=False)
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