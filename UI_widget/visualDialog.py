from PyQt5.QtWidgets import QLabel, QLineEdit, QDialog, QDialogButtonBox, QGridLayout
from PyQt5.QtCore import *
import datetime as dt
all = ["visualDialog"]
class visualDialog(QDialog):
    def __init__(self):
        super().__init__()
        self.date = QLineEdit()
        self.date_label = QLabel("请输入日期:")
        self.back_period = QLineEdit()
        self.back_period_label = QLabel("请输入回看交易日天数:")
        self.back_period.setText("44")

        self.buttonBox = QDialogButtonBox(QDialogButtonBox.Ok
                             | QDialogButtonBox.Cancel)

        self.buttonBox.accepted.connect(self.accept)
        self.buttonBox.rejected.connect(self.reject)

        # 默认参数
        today = int(dt.date.today().strftime('%Y%m%d'))
        self.date.setText(str(today))

        layout = QGridLayout()
        layout.addWidget(self.date_label)
        layout.addWidget(self.date)
        layout.addWidget(self.back_period_label)
        layout.addWidget(self.back_period)
        layout.addWidget(self.buttonBox,2,1,Qt.Alignment(Qt.AlignCenter))
        self.setLayout(layout)
        self.setWindowTitle("导出套利图")