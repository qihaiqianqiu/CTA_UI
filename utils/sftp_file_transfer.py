import json
from . import sftp
from . import path_exp_switch
import os
import pandas as pd
from utils.const import ROOT_PATH, INFO_PATH
from utils.path_exp_switch import windows_to_linux

__all__ = ["get_acc_lst", "sftp_conn", "sftp_transfer"]

def get_acc_lst():
    df = pd.read_excel(os.path.join(INFO_PATH, "account_info.xlsx"), sheet_name="Sheet1")
    acc_lst = df['id'].values.tolist()
    return acc_lst


def sftp_conn(type):
    ftp_config = json.load(open(os.path.join(ROOT_PATH, "FTP_config.json")))
    if type == 'bridge':
        server_para = ftp_config['BRIDGE_SERVER_ADDR']
    if type == 'trade':
        server_para = ftp_config['TRADE_SERVER_ADDR']

    ssh = sftp.SSHConnection(host=server_para['host'], port=server_para['port'],
                            username=server_para['username'], pwd=server_para['pwd'])
    ssh.connect()

    return ssh


def sftp_transfer(ssh_conn, acc, rule):
    ftp_config = json.load(open((os.path.join(ROOT_PATH, "FTP_config.json"))))
    local_dir = ROOT_PATH
    dest_dir = ftp_config["DEST_DIR"]
    file_lst = ftp_config["PULL_FILE"]
    if rule == "activate":
        # 参数表分发
        for file in file_lst["local"]:
            try:
                parma_dir = os.path.join(local_dir, "params")
                param_acc_dir = os.path.join(parma_dir, acc)
                param_file_dir = os.path.join(param_acc_dir, file)

                dest_acc_dir = path_exp_switch.windows_to_linux(os.path.join(dest_dir, acc))
                ssh_conn.cmd("mkdir -p " + dest_acc_dir)
                
                dest_file_dir = path_exp_switch.windows_to_linux(os.path.join(dest_acc_dir, file))
                ssh_conn.upload(param_file_dir, dest_file_dir)
                print("successfully transfer ", param_file_dir, "->", dest_file_dir)
            except Exception as e:
                print(e)
                print("check dir: ", param_file_dir, " or ", dest_file_dir)

    if rule == "planned_times":
        for file in file_lst["remote"]:
            try:
                holding_dir = os.path.join(local_dir, "holdings")
                holding_acc_dir = os.path.join(holding_dir, acc)
                hold_file_dir = os.path.join(holding_acc_dir, file)
    
                dest_acc_dir = path_exp_switch.windows_to_linux(os.path.join(dest_dir, acc))
                ssh_conn.cmd("mkdir -p " + dest_acc_dir)
                
                dest_file_dir = path_exp_switch.windows_to_linux(os.path.join(dest_acc_dir, file))

                ssh_conn.download(dest_file_dir, hold_file_dir)
            except Exception as e:
                print(e)
                print("check dir: ", dest_file_dir, " or ", hold_file_dir)

        