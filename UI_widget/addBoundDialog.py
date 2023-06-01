"""
计算参数，更新参数表
"""
from PyQt5 import Qt
from PyQt5.QtWidgets import QLineEdit, QLabel, QDialog, QDialogButtonBox, QGridLayout
from utils.date_section_modification import get_date_section
all = ["addBoundDialog"]
class addBoundDialog(QDialog):
    def __init__(self):
        super().__init__()
        self.end_date = QLineEdit()
        self.end_date_label = QLabel("预测日期")
        self.section = QLineEdit()
        self.section_label = QLabel("预测交易单元")
        self.q = QLineEdit()
        self.q_label = QLabel("分位数")
        self.step = QLineEdit()
        self.step_label = QLabel("回看周期")
        self.ticklock_ratio = QLineEdit()
        self.ticklock_ratio_label = QLabel("Ticklock系数")
        
        label_lst = [self.end_date_label, self.section_label, self.q_label, self.step_label, self.ticklock_ratio_label]
        edit_lst = [self.end_date, self.section, self.q, self.step, self.ticklock_ratio]

        self.buttonBox = QDialogButtonBox(QDialogButtonBox.Ok
                             | QDialogButtonBox.Cancel)

        self.buttonBox.accepted.connect(self.accept)
        self.buttonBox.rejected.connect(self.reject)

        # 默认参数
        date, section = get_date_section()
        self.end_date.setText(str(date))
        self.section.setText(str(section))
        self.q.setText("0.95")
        self.step.setText("6")
        self.ticklock_ratio.setText("0.9")

        layout = QGridLayout()
        for i in range(len(label_lst)):
            layout.addWidget(label_lst[i], 0, i)
            layout.addWidget(edit_lst[i], 1, i)
        layout.addWidget(self.buttonBox,2,1,Qt.Alignment(Qt.AlignCenter))
        self.setLayout(layout)
        self.setWindowTitle("生成参数表")