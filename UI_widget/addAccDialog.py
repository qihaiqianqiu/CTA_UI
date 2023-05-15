from PyQt5 import Qt
from PyQt5.QtWidgets import *
from PyQt5.QtCore import *
all=["addAccDialog"]
class addAccDialog(QDialog):
    def __init__(self):
        super().__init__()
        self.id = QLineEdit()
        self.id_label = QLabel("id")
        self.describe = QLineEdit()
        self.describe_label = QLabel("describe")
        self.account = QLineEdit()
        self.account_label = QLabel("account")
        self.status = QLineEdit()
        self.status_label = QLabel("status")
        self.region_budget = QLineEdit()
        self.region_budget_label = QLabel("region_budget")
        self.boundary_budget = QLineEdit()
        self.boundary_budget_label = QLabel("boundary_budget")
        self.is_cffex = QLineEdit()
        self.is_cffex_label = QLabel("is_cffex")
        self.is_back = QLineEdit()
        self.is_back_label = QLabel("is_back")
        self.is_boundary_3 = QLineEdit()
        self.is_boundary_3_label = QLabel("is_boundary_3")
        self.is_boundary_4 = QLineEdit()
        self.is_boundary_4_label = QLabel("is_boundary_4")
        label_lst = [self.id_label, self.describe_label, self.account_label, self.status_label, self.region_budget_label, \
            self.boundary_budget_label, self.is_cffex_label, self.is_back_label, self.is_boundary_3_label, self.is_boundary_4_label]
        edit_lst = [self.id, self.describe, self.status, self.account, self.region_budget, self.boundary_budget, self.is_cffex, self.is_back,\
            self.is_boundary_3, self.is_boundary_4]

        self.buttonBox = QDialogButtonBox(QDialogButtonBox.Ok
                             | QDialogButtonBox.Cancel)

        self.buttonBox.accepted.connect(self.accept)
        self.buttonBox.rejected.connect(self.reject)

        layout = QGridLayout()
        for i in range(len(label_lst)):
            layout.addWidget(label_lst[i], 0, i)
            layout.addWidget(edit_lst[i], 1, i)
        layout.addWidget(self.buttonBox,2,1,Qt.Alignment(Qt.AlignCenter))
        self.setLayout(layout)
        self.setWindowTitle("添加账户")