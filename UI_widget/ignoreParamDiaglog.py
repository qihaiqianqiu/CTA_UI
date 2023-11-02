"""
    为账户选择哪些标的不交易
    1.维护一个ignore_info, 行索引为BASE的param.csv中的套利对, 列索引为账户, 初始化的时候创建一个全为False的表格, 存储在ignore_info中
    2.每次呼出功能时, 以最新BASE的参数表刷新ignore表, 之后以checkbox的形式修改igonore, 提交后存储
    3.每次分发账户表的时候, 以ignore表为准, 将ignore表中为True的标的从账户表中删除 -- 修改start.py中save函数的相应逻辑即可
"""
import re
import os
import pandas as pd
from UI_widget.checkboxModel import checkboxModel
from PyQt5.QtWidgets import QTableView, QDialog, QVBoxLayout, QDialogButtonBox, QMessageBox
from utils.const import PARAM_PATH, INFO_PATH

all = ["ignoreParamDialog"]

class ignoreParamDialog(QDialog):
    def __init__(self):
        super().__init__()
        # 读取最新BASE参数表，获取所有套利对
        self.ignore_info = self.refresh_ingore_table()
        # 初始化界面
        model =  checkboxModel(self.ignore_info)
        view = QTableView()
        view.setModel(model)
        # show
        self.layout = QVBoxLayout()
        self.layout.addWidget(view)
        self.setLayout(self.layout)
        self.setWindowTitle("选择不交易的标的")
        self.setGeometry(300, 300, 640, 1000)
        self.buttonBox = QDialogButtonBox(QDialogButtonBox.Ok | QDialogButtonBox.Cancel)
        self.buttonBox.accepted.connect(self.submit)
        self.buttonBox.rejected.connect(self.reject)
        self.layout.addWidget(self.buttonBox)
        
    def refresh_ingore_table(self):
        self.param_df = pd.read_csv(os.path.join(PARAM_PATH, 'BASE', "params.csv"))
        self.contract_pair_lst = self.param_df['pairs_id'].tolist()
        self.account_df = pd.read_excel(os.path.join(INFO_PATH, 'account_info.xlsx'), sheet_name="Sheet1")
        # 账户为id列
        self.acc_lst = self.account_df['id'].tolist()
        
        # 如果没有ingore_info存在，则初始化一个新的ignore_info
        if not os.path.exists(os.path.join(INFO_PATH, "ignore_info.xlsx")):
            print("进入创建分支")
            # 初始化ignore_info
            self.ignore_info = pd.DataFrame(columns=self.acc_lst, index=self.contract_pair_lst)
            # 值全部设置为False
            self.ignore_info = self.ignore_info.fillna(False)
            self.ignore_info.to_excel(os.path.join(INFO_PATH, "ignore_info.xlsx"))
            
        else:
            print("进入修改分支")
            self.ignore_info = pd.read_excel(os.path.join(INFO_PATH, 'ignore_info.xlsx'), sheet_name="Sheet1", index_col=0)
            # 与最新的BASE参数表行索引作对比，如果有新的套利对，则添加到ignore_info中
            for contract_pair in self.contract_pair_lst:
                if contract_pair not in self.ignore_info.index:
                    print("-----")
                    print(contract_pair)
                    self.ignore_info.loc[contract_pair] = False
            # 如果有参数表在BASE参数表被删除了，则删除ignore_info中的对应行
            for contract_pair in self.ignore_info.index:
                if contract_pair not in self.contract_pair_lst:
                    self.ignore_info.drop(contract_pair, inplace=True)
                    
            # 与最新的BASE参数表列索引做对比，如果有新的账户，则添加到ingore_info中
            for acc in self.acc_lst:
                if acc not in self.ignore_info.columns:
                    print("-----")
                    print(acc)
                    self.ignore_info[acc] = False
            # 如果有账户在账户表被删除了，则删除ignore_info中的对应列
            for acc in self.ignore_info.columns:
                if acc not in self.acc_lst:
                    self.ignore_info.drop(acc, axis=1, inplace=True)
                    
            self.ignore_info.to_excel(os.path.join(INFO_PATH, "ignore_info.xlsx"))
        print("标的交易控制表刷新完成")
        return self.ignore_info

    # 记录用户提交的交易标的勾选结果并存储
    def submit(self):
        # 获取用户提交的勾选结果
        model = self.layout.itemAt(0).widget().model()
        self.ignore_info = model.data
        # 存储
        self.ignore_info.to_excel(os.path.join(INFO_PATH, "ignore_info.xlsx"))
        print("标的交易控制表修改并存储完成")
        QMessageBox.information(self, "提示", "标的交易控制方案已提交")
        self.close()
        return 