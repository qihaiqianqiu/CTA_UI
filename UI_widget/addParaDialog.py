"""
计算参数，更新参数表
"""
from PyQt5.QtGui import QDesktopServices
from PyQt5.QtCore import QUrl
from PyQt5.QtWidgets import QLineEdit, QLabel, QDialog, QDialogButtonBox, QGridLayout, QVBoxLayout, QWidget, QPushButton
from utils.plotfile_management import pairname_to_plotdir
from functools import partial

all = ["addParaDialog"]
class addParaDialog(QDialog):
    def __init__(self, pairs_id_lst):
        super().__init__()
        self.setAutoFillBackground(True)
        self.layout = QVBoxLayout()
        self.setLayout(self.layout)
        self._contract_pair_label_lst = []
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

        self.buttonBox.accepted.connect(self.accept)
        self.buttonBox.rejected.connect(self.reject)
            

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
        return line_layout