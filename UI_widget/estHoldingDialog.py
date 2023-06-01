from PyQt5 import Qt
from PyQt5.QtWidgets import QHBoxLayout, QDialogButtonBox, QDialog, QVBoxLayout, QLabel, QLineEdit, QScrollArea
from PyQt5.QtCore import *
from utils.const import breed_dict

all=["estHoldingDialog"]
class estHoldingDialog(QDialog):
    def __init__(self):
        super().__init__()
        self.dialog = self.create_dialog()
        scroll = self.add_scroll()
        layout = QHBoxLayout()
        layout.addWidget(scroll)
        
        self.buttonBox = QDialogButtonBox(QDialogButtonBox.Ok
                             | QDialogButtonBox.Cancel)

        self.buttonBox.accepted.connect(self.accept)
        self.buttonBox.rejected.connect(self.reject)
        layout.addWidget(self.buttonBox)

        self.setLayout(layout)
        self.setWindowTitle("持仓估算")

    def create_dialog(self):
        dialog = QDialog()
        breed_label = []
        breed_name = []
        breed_vol = []
        breed_price = []
        self.layout_breed_label = QVBoxLayout()
        self.layout_breed_name = QVBoxLayout()
        self.layout_breed_vol = QVBoxLayout()
        self.layout_breed_price = QVBoxLayout()
        for key, values in dict(sorted(breed_dict.items(), key=lambda item: item[1].upper())).items():
            breed_label.append(QLabel(values.upper()))
            breed_name.append(QLabel(key))
            breed_vol.append(QLineEdit())
            breed_price.append(QLineEdit())

        mainlayout = QHBoxLayout()
        layout_header = QHBoxLayout()
        layout_header.addWidget(QLabel("品种"))
        layout_header.addWidget(QLabel("名称"))
        layout_header.addWidget(QLabel("手数"))
        layout_header.addWidget(QLabel("均价"))

        for i in range(len(breed_label)):
            breed_vol[i].setText("0")
            breed_price[i].setText("0")
            self.layout_breed_label.addWidget(breed_label[i])
            self.layout_breed_name.addWidget(breed_name[i])
            self.layout_breed_vol.addWidget(breed_vol[i])
            self.layout_breed_price.addWidget(breed_price[i])

        mainlayout.addLayout(self.layout_breed_label)
        mainlayout.addLayout(self.layout_breed_name)
        mainlayout.addLayout(self.layout_breed_vol)
        mainlayout.addLayout(self.layout_breed_price)
        layout_button = QHBoxLayout()
        
        total_layout = QVBoxLayout()
        total_layout.addLayout(layout_header)
        total_layout.addLayout(mainlayout)
        total_layout.addLayout(layout_button)

        dialog.setLayout(total_layout)

        return dialog
    
    def add_scroll(self):
        scroll = QScrollArea()
        scroll.setMinimumSize(425,650)
        scroll.setWidget(self.dialog)
        return scroll