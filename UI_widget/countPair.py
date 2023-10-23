from PyQt5.QtWidgets import QLabel, QLineEdit, QDialog, QDialogButtonBox, QGridLayout
from PyQt5.QtCore import *
import datetime as dt
all = ["countPair"]
class countPairDialog(QDialog):
    def __init__(self):
        super().__init__()
        self.pair = QLineEdit()
        self.pair_label = QLabel("套利对名称(eg.SC2308-2309):")
        self.acc = QLineEdit()
        self.acc_label = QLabel("账户名称:")

        self.buttonBox = QDialogButtonBox(QDialogButtonBox.Ok
                             | QDialogButtonBox.Cancel)

        self.buttonBox.accepted.connect(self.accept)
        self.buttonBox.rejected.connect(self.reject)

        # 默认参数
        layout = QGridLayout()
        layout.addWidget(self.pair_label)
        layout.addWidget(self.pair)
        layout.addWidget(self.acc_label)
        layout.addWidget(self.acc)
        layout.addWidget(self.buttonBox,2,1,Qt.Alignment(Qt.AlignCenter))
        self.setLayout(layout)
        self.setWindowTitle("导出套利对交易记录")