"""_summary_ 计算持仓数据并显示在UI上
1.向交易端发出导出指令
2.下载导出数据至本地
3.计算并可视化
想先写一个简单一点的，弹出一个界面，每个按钮对应一个账户，点击之后计算该账户的持仓数据并显示在界面上。应该会画一个饼图吧
"""
from PyQt5 import QtCore
from PyQt5.QtWidgets import QDialog, QPushButton, QVBoxLayout, QWidget, QMainWindow, QApplication, QScrollArea
from matplotlib.backends.backend_qt5agg import FigureCanvasQTAgg as FigureCanvas
from matplotlib.backends.backend_qt5agg import NavigationToolbar2QT as NavigationToolbar
import matplotlib
import pandas as pd
import os
import json
import sys
from utils.const import ROOT_PATH, PLOT_PATH
from utils import sftp
from utils.flink import request_from_trading_to_market
from utils.compare import get_hold
from utils.visualize import stock_to_pie
import time
matplotlib.use('Qt5Agg')

class stockCounter(QDialog):
    def __init__(self):
        super().__init__()
        self.layout = QVBoxLayout()
        df = pd.read_excel(os.path.join(ROOT_PATH, "info", "account_info.xlsx"), sheet_name="Sheet1")
        acc_lst = df['id'].values.tolist()
        self.btn_group = []
        # 闭包函数，用于创建按钮点击事件
        def createButtonClickedFunc(button_text):
            def buttonClicked():
                self.calculate_stocking(button_text)
            return buttonClicked
        for acc in acc_lst:
            btn = QPushButton(acc)
            btn.setFixedSize(150, 75)
            self.btn_group.append(btn)
            self.layout.addWidget(btn)
        for idx in range(len(self.btn_group)):
            account_name = self.btn_group[idx].text()
            self.btn_group[idx].clicked.connect(createButtonClickedFunc(account_name))
        self.setLayout(self.layout)
        
    @QtCore.pyqtSlot()
    def export_stocking(self, target_acc):
        # 找到账户所在链路信息
        config_file_lst = os.listdir(os.path.join(ROOT_PATH, "sftp_configs"))
        if_config_exist = False
        for config in config_file_lst:
            acc_config_lst = json.load(open(os.path.join(ROOT_PATH, "sftp_configs", config)))['accountList']
            for acc in acc_config_lst:
                print(acc, target_acc, acc == target_acc)
                if acc == target_acc:
                    target_config = config
                    if_config_exist = True
                    break
            else:
                print("No account information")
            if if_config_exist:
                break
        else:
            print("No config file")
            
        target_config_path = os.path.join(ROOT_PATH, "sftp_configs", target_config)
        print("使用配置文件{}导出实时持仓".format(target_config_path))
        ftp_config = json.load(open(target_config_path))
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
        request_from_trading_to_market(target_config_path)
                  
        
    def calculate_stocking(self, target_acc):
        self.export_stocking(target_acc)
        time.sleep(3)
        print("查看账户{}的实时持仓情况".format(target_acc))
        # 获取最新的持仓文件
        stocking = {}
        df = get_hold(target_acc)
        for temp in df.groupby('product'):
            breed = temp[0]
            value = temp[1]['stocking_delta'].apply(lambda x: float(str(x).replace(",", "")))
            stocking[breed] = value.sum()
        self.plt = stock_to_pie(stocking) 
        manager = self.plt.get_current_fig_manager()
        manager.window.showMaximized()
        self.plt.show()
        #plt.savefig(os.path.join(PLOT_PATH, "stocking.png"))
        
        

        



if __name__ == "__main__":
    """从交易服务器向行情服务器传送交易日志"""
    # 获取config文件所在的目录，找到对应交易员与账户
    df = get_hold("lq")
    stocking = {}
    for temp in df.groupby('product'):
        breed = temp[0]
        value = temp[1]['stocking_delta'].apply(lambda x: float(str(x).replace(",", "")))
        stocking[breed] = value.sum()
    plt = stock_to_pie(stocking)
    app = QApplication(sys.argv)
    window = QMainWindow()
    canvas = FigureCanvas(plt.figure())
    toolbar = NavigationToolbar(canvas, window)
    layout = QVBoxLayout()
    layout.addWidget(toolbar)
    layout.addWidget(canvas)
    widget = QWidget()
    widget.setLayout(layout)
    window.setCentralWidget(widget)
    canvas.draw()
    window.exec_()
        