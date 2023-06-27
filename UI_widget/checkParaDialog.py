"""
检查参数表
"""
from PyQt5 import QtGui
from PyQt5.QtCore import Qt, pyqtSignal
from PyQt5.QtWidgets import  QDialog, QHBoxLayout, QPushButton
import pandas as pd
import sys
import os
from utils.get_contract_pair import check_vaild_month
from utils.const import PARAM_PATH
from UI_widget.addParaDialog import addParaDialog
from .pandasModel import pandasModel, TableView

all = ["checkParamDialog"]
class checkParaDialog(QDialog):
    add_signal_outer = pyqtSignal(bool)
    def __init__(self):
        super().__init__()
        lost_pair = pd.DataFrame(columns=['warehouse_recipt', 'volume'], index=['pairs_id'])
        lost_pair.astype({"warehouse_recipt": 'bool'})
        candidate_pairs = check_vaild_month()
        param_df = pd.read_csv(os.path.join(PARAM_PATH, 'BASE', "params.csv"))
        for contract_pair in candidate_pairs['contract_pair'].tolist():
            if contract_pair not in param_df['pairs_id'].tolist():
                vol = candidate_pairs[candidate_pairs['contract_pair']==contract_pair]['volume'].tolist()[0]
                print("检测到未包含合约：", contract_pair)
                flag = candidate_pairs[candidate_pairs['contract_pair']==contract_pair]['flag'].tolist()[0]
                lost_pair = pd.concat([lost_pair, pd.DataFrame({'warehouse_recipt': flag, 'volume': vol}, index=[contract_pair])])
        lost_pair = lost_pair.sort_values(by=['warehouse_recipt', 'volume'], ascending=False)
        
        self.model = pandasModel(lost_pair)
        self.view = TableView(self.model)

        layout = QHBoxLayout()
        layout.addWidget(self.view)
        
        font = QtGui.QFont()
        font.setFamily("方正粗黑宋简体")
        font.setPointSize(18)
        button = QPushButton("添加")
        button.setFont(font)
        button.clicked.connect(self.add_param)
        button.setStyleSheet("QPushButton{background:qlineargradient(spread:reflect, x1:0, y1:1, x2:0, y2:0, "
                             "stop:0 #0a4a83, stop:0.5 #186e99, stop:1 #239cd8);"
                             "border:2px solid qlineargradient(spread:pad, x1:1, y1:1, x2:1, y2:0,"
                             "stop:0 #002aff, stop:0.5 #00aeff, stop:1 #00e6fc); border-radius:1px; "
                             "color:#d0f2f5} "
                             "QPushButton:hover:!pressed {border:1px solid #f8878f;}"
                             "QPushButton:pressed {padding-left:6px;padding-top:6px;border:1px solid #f8878f;}")
        layout.addWidget(button)
        
        self.setLayout(layout)
        self.setGeometry(300, 300, 640, 1000)
        self.setWindowTitle("可添加套利对")
        
        
    # 将信号传递给工具箱窗口
    def pass_add_signal(self, flag):
        print("sending signal to toolKit:", flag)
        self.add_signal_outer.emit(flag)
        if flag == True:
            self.close()



        
    def add_param(self):
        pairs_id = self.model._data[self.model._data['CheckBox'] == True].index.tolist()
        if len(pairs_id) > 0:
            self.add_dia = addParaDialog(pairs_id)
            self.add_dia.add_signal_inner.connect(self.pass_add_signal)
            self.add_dia.show() 