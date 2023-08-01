"""_summary_ 计算持仓数据并显示在UI上
1.向交易端发出导出指令
2.下载导出数据至本地
3.计算并可视化
想先写一个简单一点的，弹出一个界面，每个按钮对应一个账户，点击之后计算该账户的持仓数据并显示在界面上。应该会画一个饼图吧
"""
from PyQt5 import QtCore
from PyQt5.QtWidgets import QDialog, QPushButton, QVBoxLayout
import pandas as pd
import os, re
import json
from utils.const import ROOT_PATH
from utils import sftp
from utils.flink import request_from_trading_to_market
from utils.compare import get_hold
from utils.visualize import stock_to_pie

class stockCounter(QDialog):
    def __init__(self):
        super().__init__()
        self.layout = QVBoxLayout()
        df = pd.read_excel(os.path.join(ROOT_PATH, "info", "account_info.xlsx"), sheet_name="Sheet1")
        acc_lst = df['id'].values.tolist()
        for acc in acc_lst:
            btn = QPushButton(acc)
            btn.setFixedSize(150, 75)
            btn.clicked.connect(self.calculate_stocking)
            self.layout.addWidget(btn)
        
        
        
    @QtCore.pyqtSlot()
    def export_stocking(self, target_acc):
        # 找到账户所在链路信息
        config_file_lst = os.listdir(os.path.join(ROOT_PATH, "sftp_configs"))
        for config in config_file_lst:
            acc_config_lst = json.load(open(os.path.join(ROOT_PATH, "sftp_configs", config)))['accountList']
            for acc in acc_config_lst:
                if acc == target_acc:
                    target_config = config
                    break
        else:
            print("No account information")
        ftp_config = json.load(open(os.path.join(ROOT_PATH, "sftp_configs", target_config)))
        # 使用指令导出持仓数据
        trade_server_para = ftp_config["tradeServer"]
        ssh = sftp.SSHConnection(host=trade_server_para['host'], port=trade_server_para['port'],
                                username=trade_server_para['username'], pwd=trade_server_para['pwd'])
        ssh.connect()
        # 导出持仓指令(如果交易服务器有多账户，只导出第一个的)
        changedir_cmd = "cd " + ftp_config["tradeDirList"][0]
        export_cmd = "cmd.exe /c CTPtest-GetPosAndTrd.exe"
        ssh.cmd(changedir_cmd + " & " + export_cmd)
        ssh.close()
        # 下载持仓数据
        request_from_trading_to_market(ftp_config)
                  
        
    def calcualte_stocking(self, target_acc):
        # 获取最新的持仓文件
        stocking = {}
        df = get_hold(target_acc)
        for temp in df.groupby('product'):
            breed = temp[0]
            value = temp[1]['stocking_delta'].apply(lambda x: float(x.replace(",", "")))
            stocking[breed] = value.sum()
        plt = stock_to_pie(stocking) 
            
    

if __name__ == "__main__":
    """从交易服务器向行情服务器传送交易日志"""
    # 获取config文件所在的目录，找到对应交易员与账户
    df = get_hold("lq")
    stocking = {}
    for temp in df.groupby('product'):
        breed = temp[0]
        value = temp[1]['stocking_delta'].apply(lambda x: float(str(x).replace(",", "")))
        stocking[breed] = value.sum()
    stock_to_pie(stocking)
        