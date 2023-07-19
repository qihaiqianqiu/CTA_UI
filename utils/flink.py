import json
from . import sftp
from . import path_exp_switch
import os
import pandas as pd
from utils.const import ROOT_PATH, INFO_PATH
from utils.path_exp_switch import windows_to_linux

__all__ = ["set_up_ssh_reverse_tunnel", "pull_from_market_to_trading", "request_from_market_to_cloud", "request_from_trading_to_market", "pull_from_cloud_to_market", "pull_from_UI_to_cloud", "pull_from_UI_to_market"]


def pull_from_UI_to_cloud(config_file):
    """
    从UI端向云服务器传送参数表，链路配置
    Args:
        config_file (_type_): _description_
    """
    ftp_config_dir = os.path.join(ROOT_PATH, "sftp_configs", config_file)
    ftp_config = json.load(open(ftp_config_dir))
    cloud_server_para = ftp_config["cloudServer"]
    ssh = sftp.SSHConnection(host=cloud_server_para['host'], port=cloud_server_para['port'],
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
    
    ssh = sftp.SSHConnection(host=cloud_server_para['host'], port=cloud_server_para['reverse_port'],
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
    ssh = sftp.SSHConnection(host=cloud_server_para['host'], port=cloud_server_para['port'],
                              username=cloud_server_para['username'], pwd=cloud_server_para['pwd'])
    ssh.connect()
    print("SSH Tunnel Connected")
    ssh.reverse_forward_tunnel(cloud_server_para['reverse_port'], ftp_config['marketServer']['host'], ftp_config['marketServer']['port'])
    
def pull_from_market_to_trading(config_file):
    """从行情服务器向交易服务器传送参数表"""
    # 获取config文件所在的目录，找到对应交易员与账户
    ftp_config = json.load(open(config_file))
    trade_server_para = ftp_config["tradeServer"]
    ssh = sftp.SSHConnection(host=trade_server_para['host'], port=trade_server_para['port'],
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
    ssh = sftp.SSHConnection(host=cloud_server_para['host'], port=cloud_server_para['port'],
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
if __name__ == "__main__":
    import threading
    thread_a = threading.Thread(target=set_up_ssh_reverse_tunnel, args=("lq.json",))
    thread_b = threading.Thread(target=set_up_ssh_reverse_tunnel, args=("lqq.json",))
    thread_a.start()
    thread_b.start()

