import json
from . import sftp
import re
import os
import datetime
from utils.const import ROOT_PATH
from utils.path_exp_switch import windows_to_linux

__all__ = ["set_up_ssh_reverse_tunnel", "pull_from_market_to_trading", "request_from_market_to_cloud", "request_from_trading_to_market", "pull_from_UI_to_cloud", "pull_from_UI_to_market", "pull_from_market_to_cloud", "request_from_cloud_to_UI"]


# 文件链路定义部分 - flink.py
# 其中有很多的链路，UI端只使用UI或者Market(当UI与Market重合时作为MarketServer调用)的链路
def pull_from_UI_to_cloud(config_file):
    """
    从UI端向云服务器传送参数表，链路配置
    Args:
        config_file (_type_): _description_
    """
    log = []
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
        dt_stamp = datetime.datetime.now().strftime('%Y%m%d_%H:%M:%S')
        log_info = ssh.upload(param_dir, windows_to_linux(os.path.join(dest_acc_dir, "params" + "_" + dt_stamp + '.csv')))
        log_info.insert(0, acc)
        log_info.insert(1, "UI -> Cloud")
        log.append(log_info)
        log_info = ssh.upload(param_dir, windows_to_linux(os.path.join(dest_acc_dir, "params.csv")))
        log_info.insert(0, acc)
        log_info.insert(1, "UI -> Cloud")
        log.append(log_info)
        log_info = ssh.upload(ftp_config_dir, windows_to_linux(os.path.join(dest_acc_dir, config_file)))
        log_info.insert(0, acc)
        log_info.insert(1, "UI -> Cloud")        
        log.append(log_info)
    ssh.close()
    return log


def pull_from_UI_to_market(config_file):
    """
    从UI端向行情服务器传送参数表，链路配置
    Args:
        config_file (_type_): _description_
    """
    log = []
    ftp_config_dir = os.path.join(ROOT_PATH, "sftp_configs", config_file)
    ftp_config = json.load(open(ftp_config_dir))
    cloud_server_para = ftp_config["cloudServer"]
    market_server_para = ftp_config["marketServer"]
    
    # 参数表推送
    account_list = ftp_config["accountList"]
    if ftp_config["marketServer"]["host"] == "localhost":
        # 本地行情服务器，直连交易服务器
        trade_dir_list = ftp_config["tradeDirList"]
        trade_server_para = ftp_config["tradeServer"]
        ssh = sftp.SSHConnection(host=trade_server_para['host'], port=trade_server_para['port'],
                             username=trade_server_para['username'], pwd=trade_server_para['pwd'])
        ssh.connect()
        for idx in range(len(trade_dir_list)):
            # 首先，保证交易服务器端建立相应的存储目录
            ssh.cmd("mkdir " + trade_dir_list[idx])
            dest_acc_dir = trade_dir_list[idx]
            acc = account_list[idx]
            ssh.cmd("mkdir " + dest_acc_dir)
            # 上传参数表, 链路配置表
            param_dir = os.path.join(ROOT_PATH, "params", acc, "params.csv")
            log_info = ssh.upload(param_dir, os.path.join(dest_acc_dir, "params.csv"))
            log_info.insert(0, acc)
            log_info.insert(1, "UI -> Trading")
            log.append(log_info)
        ssh.close()
        
    else:
        # 远程行情服务器，在行情侧建立好反向SSH隧道，从UI端使用反向端口连接行情服务器
        ssh = sftp.SSHConnection(host=cloud_server_para['host'], port=cloud_server_para['reverse_port'],
                    username=market_server_para['username'], pwd=market_server_para['pwd'])
        ssh.connect()
        for acc in account_list:
            username = ftp_config["userName"]
            # 首先，保证行情服务器端建立相应的存储目录
            dest_user_dir = os.path.join(market_server_para["mktDir"], username)
            ssh.cmd("mkdir " + dest_user_dir)
            dest_acc_dir = os.path.join(dest_user_dir, acc)
            ssh.cmd("mkdir " + dest_acc_dir)
            # 上传参数表, 链路配置表
            param_dir = os.path.join(ROOT_PATH, "params", acc, "params.csv")
            log_info = ssh.upload(param_dir, os.path.join(dest_acc_dir, "params.csv"))
            log_info.insert(0, acc)
            log_info.insert(1, "UI -> Market")
            log.append(log_info)
            log_info = ssh.upload(ftp_config_dir, os.path.join(dest_acc_dir, config_file))
            log_info.insert(0, acc)
            log_info.insert(1, "UI -> Market")
            log.append(log_info)
            print("参数表成功上传至行情端", dest_acc_dir)
        ssh.close()
    return log


# 从云服务器下载日志文件到本地，以生成交易记录
def request_from_cloud_to_UI(config):    
    # 当行情服务器和本地UI分离时，UI从云服务器下载每天的成交记录 
    ftp_config = json.load(open(config))
    cloud_server_para = ftp_config["cloudServer"]
    
    ssh = sftp.SSHConnection(host=cloud_server_para['host'], port=cloud_server_para['port'],
                            username=cloud_server_para['username'], pwd=cloud_server_para['pwd'])
    ssh.connect()
    # 日志文件推送
    username = ftp_config["userName"]
    account_list = ftp_config["accountList"]
    UI_dir = ftp_config["localUIDir"]
    cta_cloud_dir = os.path.join("CTA", username)
    log = []
    
    for acc in account_list:
        # 首先建立对应目录
        report_file_dir = os.path.join(UI_dir, "report", acc)
        if not os.path.exists(report_file_dir):
            os.mkdir(report_file_dir)
        trading_file_dir = os.path.join(UI_dir, "tradings", acc)
        if not os.path.exists(trading_file_dir):
            os.mkdir(trading_file_dir)
        dest_acc_dir = windows_to_linux(os.path.join(cta_cloud_dir, acc))
        # 获取云服务器的日志listdir
        res = ssh.cmd("ls " + dest_acc_dir)
        output = res.decode("GBK").splitlines()
        trading_files = [re.search(r'trading_\d{6}\.csv', f).group() for f in output if re.search(r'trading_\d{6}\.csv', f)]
        trading_file = windows_to_linux(os.path.join(dest_acc_dir, max(trading_files)))
        holding_files = [re.search(r'holding_\d{6}\.csv', f).group() for f in output if re.search(r'holding_\d{6}\.csv', f)]
        holding_file = windows_to_linux(os.path.join(dest_acc_dir, max(holding_files)))
        log_info = ssh.download(holding_file, os.path.join(report_file_dir, max(holding_files)))
        log_info.insert(0, acc)
        log_info.insert(1, "Cloud -> UI")
        log.append(log_info)
        log_info = ssh.download(trading_file, os.path.join(trading_file_dir, max(trading_files)))
        log_info.insert(0, acc)
        log_info.insert(1, "Cloud -> UI")
        log.append(log_info)
    ssh.close()
    return log
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
    ftp_config = json.load(open(config_file))
    cloud_server_para = ftp_config["cloudServer"]
    ssh = sftp.SSHConnection(host=cloud_server_para['host'], port=cloud_server_para['port'],
                              username=cloud_server_para['username'], pwd=cloud_server_para['pwd'])
    ssh.connect()
    print("使用配置文件{}建立反向隧道：SSH Tunnel Connected".format(config_file))
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
    trade_dir_list = ftp_config["tradeDirList"]
    account_list = ftp_config["accountList"]

    if ftp_config["marketServer"]["host"] == "localhost":
        # 本地行情服务器
        mkt_dir = ftp_config["localUIDir"]
        for idx in range(len(trade_dir_list)):
            ssh.cmd("mkdir " + trade_dir_list[idx])
            # 推送参数表到交易端
            market_param_dir = os.path.join(mkt_dir, account_list[idx], "params.csv")
            trade_param_dir = os.path.join(trade_dir_list[idx], "params.csv")
            print("从行情端获取参数表", market_param_dir, "上传至交易端", trade_param_dir)
            ssh.upload(market_param_dir, trade_param_dir)
            print("行情服务器参数表成功上传至交易端", trade_param_dir)
    else:
        username = ftp_config["userName"]
        mkt_dir = ftp_config["marketServer"]["mktDir"]
        for idx in range(len(trade_dir_list)):
            ssh.cmd("mkdir " + trade_dir_list[idx])
            # 推送参数表到交易端
            market_param_dir = os.path.join(mkt_dir, username, account_list[idx], "params.csv")
            trade_param_dir = os.path.join(trade_dir_list[idx], "params.csv")
            print("从行情端获取参数表", market_param_dir, "上传至交易端", trade_param_dir)
            ssh.upload(market_param_dir, trade_param_dir)
            print("行情服务器参数表成功上传至交易端", trade_param_dir)    
    ssh.close() 


def request_from_trading_to_market(config_file):
    """从交易服务器向行情服务器传送交易日志"""
    # 获取config文件所在的目录，找到对应交易员与账户
    ftp_config = json.load(open(config_file))
    trade_server_para = ftp_config["tradeServer"]
    ssh = sftp.SSHConnection(host=trade_server_para['host'], port=trade_server_para['port'],
                             username=trade_server_para['username'], pwd=trade_server_para['pwd'])
    ssh.connect()
    # 日志文件推送
    trade_dir_list = ftp_config["tradeDirList"]
    account_list = ftp_config["accountList"]
    log = []
    if ftp_config["marketServer"]["host"] == "localhost":
        # 本地行情服务器
        mkt_dir = ftp_config["localUIDir"]
        for idx in range(len(trade_dir_list)):
            # 推送日志文件到行情端
            # 确保本地目录存在
            market_trading_dir = os.path.join(mkt_dir, "tradings", account_list[idx])
            market_report_dir = os.path.join(mkt_dir, "report", account_list[idx])
            if not os.path.exists(market_trading_dir) or os.path.exists(market_report_dir):
                os.system("mkdir " + market_trading_dir)
                os.system("mkdir " + market_report_dir)
            trade_trading_dir = os.path.join(trade_dir_list[idx], "tradings")
            trade_report_dir = os.path.join(trade_dir_list[idx], "report")
            # 获取交易服务器的日志listdir
            res = ssh.cmd("dir " + trade_trading_dir)
            output = res.decode("GBK").splitlines()
            trading_files = [re.search(r'trading_\d{6}\.csv', f).group() for f in output if re.search(r'trading_\d{6}\.csv', f)]
            trading_file = os.path.join(trade_trading_dir, max(trading_files))
            res = ssh.cmd("dir " + trade_report_dir)
            output = res.decode("GBK").splitlines()
            holding_files = [re.search(r'holding_\d{6}\.csv', f).group() for f in output if re.search(r'holding_\d{6}\.csv', f)]
            holding_file = os.path.join(trade_report_dir, max(holding_files))
            print("从交易端获取交易日志文件", trading_file, "下载至行情端", os.path.join(market_trading_dir, max(trading_files)))
            log_info = ssh.download(trading_file, os.path.join(market_trading_dir, max(trading_files)))
            log_info.insert(0, account_list[idx])
            log_info.insert(1, "Trading -> UI")
            log.append(log_info)
            print("交易服务器日志文件成功下载至行情端", os.path.join(mkt_dir, trading_file))
            print("从交易端获取交易日志文件", holding_file, "下载至行情端", os.path.join(market_report_dir, max(holding_files)))
            log_info = ssh.download(holding_file, os.path.join(market_report_dir, max(holding_files)))
            log_info.insert(0, account_list[idx])
            log_info.insert(1, "Trading -> UI")
            log.append(log_info)
            print("交易服务器日志文件成功下载至行情端", os.path.join(mkt_dir, holding_file))
    else:
        # 远程行情服务器
        username = ftp_config["userName"]
        mkt_dir = ftp_config["marketServer"]["mktDir"]
        for idx in range(len(trade_dir_list)):
            ssh.cmd("mkdir " + trade_dir_list[idx])
            # 推送日志文件到行情端
            market_dir = os.path.join(mkt_dir, username, account_list[idx])
            trade_trading_dir = os.path.join(trade_dir_list[idx], "tradings")
            trade_report_dir = os.path.join(trade_dir_list[idx], "report")
            # 获取交易服务器的日志listdir
            res = ssh.cmd("dir " + trade_trading_dir)
            output = res.decode("GBK").splitlines()
            trading_files = [re.search(r'trading_\d{6}\.csv', f).group() for f in output if re.search(r'trading_\d{6}\.csv', f)]
            trading_file = os.path.join(trade_trading_dir, max(trading_files))
            res = ssh.cmd("dir " + trade_report_dir)
            output = res.decode("GBK").splitlines()
            holding_files = [re.search(r'holding_\d{6}\.csv', f).group() for f in output if re.search(r'holding_\d{6}\.csv', f)]
            holding_file = os.path.join(trade_report_dir, max(holding_files))
            print("从交易端获取交易日志文件", trading_file, "下载至行情端", market_dir)
            log_info = ssh.download(trading_file, os.path.join(market_dir, max(trading_files)))
            log_info.insert(0, account_list[idx])
            log_info.insert(1, "Trading -> Market")
            log.append(log_info)
            print("交易服务器日志文件成功下载至行情端", os.path.join(market_dir, trading_file))
            print("从交易端获取交易日志文件", holding_file, "下载至行情端", market_dir)
            log_info = ssh.download(holding_file, os.path.join(market_dir, max(holding_files)))
            log_info.insert(0, account_list[idx])
            log_info.insert(1, "Trading -> Market")
            log.append(log_info)
            print("交易服务器日志文件成功下载至行情端", os.path.join(market_dir, holding_file))
    ssh.close()
    return log
    
def request_from_market_to_cloud(config_file):
    """行情服务器从云服务器获取最新的参数表与链路信息"""
    # 根目录 行情服务器的CTA文件夹
    ftp_config = json.load(open(config_file))
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


def pull_from_market_to_cloud(config_file, file):
    """
    从行情端向云服务器传送持仓和交易记录
    在行情端的on_modified触发时调用
    Args:
        config_file (_type_): _description_
    """
    ftp_config_dir = os.path.join(config_file)
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
    
    if ftp_config["marketServer"]["host"] == "localhost":
        # 本地行情服务器
        mkt_dir = ftp_config["localUIDir"]
        for acc in account_list:
            # 找到对应的本地目录
            if "holding" in file:
                file_dir = os.path.join(mkt_dir, "report", acc, file)
            if "trading" in file:
                file_dir = os.path.join(mkt_dir, "tradings", acc, file)
            dest_acc_dir = os.path.join(dest_user_dir, acc)
            ssh.cmd("mkdir -p " + windows_to_linux(dest_acc_dir))
            # 上传交易日志文件,包括持仓和交易记录
            ssh.upload(file_dir, windows_to_linux(os.path.join(dest_acc_dir, file)))
            print("交易日志成功上传至云端", dest_acc_dir)
    else:
        mkt_dir = ftp_config["marketServer"]["mktDir"]
        for acc in account_list:
            # 首先，保证云服务器端建立相应的存储目录
            dest_acc_dir = os.path.join(dest_user_dir, acc)
            ssh.cmd("mkdir -p " + windows_to_linux(dest_acc_dir))
            # 上传交易日志文件,包括持仓和交易记录
            file_dir = os.path.join(mkt_dir, username, acc, file)
            print(file_dir)
            ssh.upload(file_dir, windows_to_linux(os.path.join(dest_acc_dir, file)))
            print("交易日志成功上传至云端", dest_acc_dir)
    ssh.close()


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

