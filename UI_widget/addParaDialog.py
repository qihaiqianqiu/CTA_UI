"""
计算参数，更新参数表
"""
import re
import os
import pandas as pd
from PyQt5.QtGui import QDesktopServices
from PyQt5.QtCore import QUrl, pyqtSignal
from PyQt5.QtWidgets import QLineEdit, QLabel, QDialog, QDialogButtonBox, QGridLayout, QVBoxLayout, QWidget, QPushButton, QMessageBox, QCheckBox
from utils.plotfile_management import pairname_to_plotdir
from utils.const import PARAM_PATH, exchange_breed_dict, param_columns
from functools import partial
from utils.rename import rename_db_to_param
from utils.date_section_modification import get_date_section
from utils.get_contract_pair import get_contract_pair_rank, get_exchange_on

all = ["addParaDialog"]
class addParaDialog(QDialog):
    add_signal_inner = pyqtSignal(bool)
    def __init__(self, pairs_id_lst):
        super().__init__()
        self.setAutoFillBackground(True)
        self.layout = QVBoxLayout()
        self.setLayout(self.layout)
        self._contract_pair_label_lst = {}
        for contract_pair in pairs_id_lst:
            sub_layout = QGridLayout()
            panel = QWidget()
            sub_layout.addWidget(panel, 0, 0)
            panel.setStyleSheet("background-color:gray;")
            line_layout = QVBoxLayout(panel)
            btn = QPushButton(contract_pair)
            # 这个按钮直接链接到套利图上
            btn.clicked.connect(partial(QDesktopServices.openUrl, QUrl.fromLocalFile(pairname_to_plotdir(btn.text()))))  # 使用新的变量
            btn.setStyleSheet("QPushButton{background:qlineargradient(spread:reflect, x1:0, y1:1, x2:0, y2:0, "
                             "stop:0 #0a4a83, stop:0.5 #186e99, stop:1 #239cd8);"
                             "border:2px solid qlineargradient(spread:pad, x1:1, y1:1, x2:1, y2:0,"
                             "stop:0 #002aff, stop:0.5 #00aeff, stop:1 #00e6fc); border-radius:1px; "
                             "color:#d0f2f5} "
                             "QPushButton:hover:!pressed {border:1px solid #f8878f;}"
                             "QPushButton:pressed {padding-left:6px;padding-top:6px;border:1px solid #f8878f;}")
            line_layout.addWidget(btn)
            line_layout.addLayout(self.generate_line(contract_pair))
            self.layout.addLayout(sub_layout)

        self.buttonBox = QDialogButtonBox(QDialogButtonBox.Ok | QDialogButtonBox.Cancel)

        self.buttonBox.accepted.connect(self.add_param)
        self.buttonBox.rejected.connect(self.reject)
        self.sp_checkbox = QCheckBox("程序是否交易SP合约")
        self.layout.addWidget(self.sp_checkbox)
        self.layout.addWidget(self.buttonBox)
        self.add_signal_inner.emit(True)
        
            

    def generate_line(self, contract_pair:str):
        region_line_label = ["region_drift", "region_0", "region_1", "region_2", "region_3", "region_4", "region_5", "region_6", "region_7", "region_tick_lock", "region_unit_num"]
        suffix_line_label = ["today_fee", "wait_window", "favor_times", "unfavor_times", "abs_threshold", "wait2_windows", "after_tick", "night_type", "if_add", "limitcoef", "abs_threshold_after", "before_tick", "before_cancel_flag", "before_cancel_num", "max_position", "min_position", "If_SP", "SP_InstrumentID"]

        region_layout = QGridLayout()
        for label in region_line_label:
            region_layout.addWidget(QLabel(label), 0, region_line_label.index(label))
            region_layout.addWidget(QLineEdit(), 1, region_line_label.index(label))
            
        today_fee = QLineEdit("2")
        wait_window = QLineEdit("3000000")
        favor_times = QLineEdit("3")
        unfavor_times = QLineEdit("1")
        abs_threshold = QLineEdit("50")
        wait2_windows = QLineEdit("2000")
        after_tick = QLineEdit("40")
        night_type = QLineEdit("1")
        if_add = QLineEdit("1")
        limitcoef = QLineEdit("5")
        abs_threshold_after = QLineEdit("32")
        before_tick = QLineEdit("0")
        before_cancel_flag = QLineEdit("1")
        before_cancel_num = QLineEdit("1")
        max_position = QLineEdit()
        min_position = QLineEdit("0")
        If_SP = QLineEdit("0")
        SP_InstrumentID = QLineEdit("Null")
        
        suffix_line_lst = []
        suffix_line_lst.append(today_fee)
        suffix_line_lst.append(wait_window)
        suffix_line_lst.append(favor_times)
        suffix_line_lst.append(unfavor_times)
        suffix_line_lst.append(abs_threshold)
        suffix_line_lst.append(wait2_windows)
        suffix_line_lst.append(after_tick)
        suffix_line_lst.append(night_type)
        suffix_line_lst.append(if_add)
        suffix_line_lst.append(limitcoef)
        suffix_line_lst.append(abs_threshold_after)
        suffix_line_lst.append(before_tick)
        suffix_line_lst.append(before_cancel_flag)
        suffix_line_lst.append(before_cancel_num)
        suffix_line_lst.append(max_position)
        suffix_line_lst.append(min_position)
        suffix_line_lst.append(If_SP)
        suffix_line_lst.append(SP_InstrumentID)

        suffix_layout = QGridLayout()
        for label in suffix_line_label:
            suffix_layout.addWidget(QLabel(label), 0,  suffix_line_label.index(label))
            suffix_layout.addWidget(suffix_line_lst[suffix_line_label.index(label)], 1, suffix_line_label.index(label))
            
        line_layout = QVBoxLayout()
        line_layout.addLayout(region_layout)
        line_layout.addLayout(suffix_layout)
        self._contract_pair_label_lst[contract_pair] = line_layout

        return line_layout
    
        
    def add_param(self):
        # 获取全部已经填写的Layout
        date, section = get_date_section()
        old_param = pd.read_csv(os.path.join(PARAM_PATH, 'BASE', "params.csv"))
        for key, value in self._contract_pair_label_lst.items():
            param_dict = {}
            param_dict["pairs_id"] = key
            param_dict["kind"] = re.search("[a-zA-Z]+", key).group(0).upper()
            param_dict["indate_date"] = str(date) + '_' + str(section)
            # 前置列 还包含主被动腿的信息
            print("Looking at pair: ", key)
            # Sublayout 应该是两个QGridLayout， 一个是region， 一个是suffix
            for i in range(value.count()):
                region_layout = value.itemAt(0)
                suffix_layout = value.itemAt(1)
                for k in range(region_layout.count()):
                    subwidget = region_layout.itemAt(k).widget()
                    if isinstance(subwidget, QLineEdit):
                        if len(subwidget.text()) == 0:
                            self.add_signal_inner.emit(False)
                            QMessageBox.information(self, "区信息填写错误", "请检查空参数！")
                            print("Sent bad signal to checkPara")
                            return 0
                        else:
                            param_dict[region_layout.itemAt(k-1).widget().text()] = subwidget.text()

                for l in range(suffix_layout.count()):
                    subwidget = suffix_layout.itemAt(l).widget()
                    if isinstance(subwidget, QLineEdit):
                        if len(subwidget.text()) == 0:
                            self.add_signal_inner.emit(False)
                            QMessageBox.information(self, "附加信息填写错误", "请检查空参数！")
                            print("Sent bad signal to checkPara")
                            return 0
                        else:
                            param_dict[suffix_layout.itemAt(l-1).widget().text()] = subwidget.text()                   
            # 添加其他信息
            pairs = get_exchange_on(key)
            first = pairs[0]
            second = pairs[1]
            prime_ins = rename_db_to_param(get_contract_pair_rank([first, second])[0])
            if prime_ins == first.split('.')[0]:
                prime_ins = first
            else:
                prime_ins = second
            param_dict["first_instrument"] = first
            param_dict["second_instrument"] = second
            param_dict["prime_instrument"] = prime_ins
            up_boundary = ["boundary_tick_lock", "up_boundary_5", "up_boundary_4", "up_boundary_3", "up_boundary_2", "up_boundary_1"]
            down_boundary = ["down_boundary_1", "down_boundary_2", "down_boundary_3", "down_boundary_4", "down_boundary_5"]
            boundary_list = ["boundary_unit_num"]
            for label in boundary_list:
                param_dict[label] = "0"
            for label in up_boundary:
                param_dict[label] = "99999"
            for label in down_boundary:
                param_dict[label] = "-99999"
            param_line = pd.DataFrame(param_dict, index=[0])
            print(param_line.columns)
            print(param_line)
            for column in param_columns:
                if column not in param_line.columns:
                    print(column)
            order = param_columns
            param_line = param_line[order]
            if not self.sp_checkbox.isChecked():
                param_line.drop(["If_SP", "SP_InstrumentID"], axis=1, inplace=True)
            old_param = pd.concat([old_param, param_line], axis=0)
        old_param = old_param.sort_values(by=["pairs_id"])
        old_param.to_csv(os.path.join(PARAM_PATH, 'BASE', "params.csv"), index=False)
        print("Sent good signal to CheckParam")
        QMessageBox.information(self, "参数添加成功", "参数添加成功！")
        self.add_signal_inner.emit(True)
        print("Sent good signal to checkPara")
        # 关闭窗口
        self.close()
        return 1
    