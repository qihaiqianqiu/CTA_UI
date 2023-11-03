from PyQt5 import QtGui
from PyQt5.QtWidgets import QHBoxLayout, QDialogButtonBox, QDialog, QVBoxLayout, QLabel, QCheckBox, QGridLayout, QSizePolicy
from PyQt5.QtCore import pyqtSignal
from utils.const import param_columns, default_columns

all=["columnSelection"]
class columnSelection(QDialog):
    data_signal = pyqtSignal(list)
    def __init__(self):
        super().__init__()
        self.dialog = self.create_dialog()
        layout = QHBoxLayout()
        layout.addWidget(self.dialog)
        self.buttonBox = QDialogButtonBox(QDialogButtonBox.Ok
                             | QDialogButtonBox.Cancel)

        self.buttonBox.accepted.connect(self.change_visible_columns)
        self.buttonBox.rejected.connect(self.reject)
        layout.addWidget(self.buttonBox)

        self.setLayout(layout)
        self.setWindowTitle("自定义参数界面")
        
        font = QtGui.QFont()
        font.setPointSize(14)  # 设置默认字体大小
        self.setFont(font)
        
    def create_dialog(self):
        dialog = QDialog()
    
        self.grid_layout = QGridLayout()
        header_labels = ["是否展示", "名称", "含义"]
        for col, header in enumerate(header_labels):
            header_label = QLabel(header)
            self.grid_layout.addWidget(header_label, 0, col)

        row = 1
        for col in default_columns:
            chkbox = QCheckBox()
            chkbox.setChecked(True)
            self.grid_layout.addWidget(chkbox, row, 0)
            self.grid_layout.addWidget(QLabel(col), row, 1)
            self.grid_layout.addWidget(QLabel("-"), row, 2)
            row += 1

        for col in param_columns:
            if col not in default_columns and col != 'pairs_id':
                chkbox = QCheckBox()
                chkbox.setChecked(False)
                self.grid_layout.addWidget(chkbox, row, 0)
                self.grid_layout.addWidget(QLabel(col), row, 1)
                self.grid_layout.addWidget(QLabel("-"), row, 2)
                row += 1

        
        layout_button = QHBoxLayout()
        
        total_layout = QVBoxLayout()
        total_layout.addLayout(self.grid_layout)
        total_layout.addLayout(layout_button)

        dialog.setLayout(total_layout)

        return dialog

    def change_visible_columns(self):
        # 根据当前勾选的checkbox，更新可见列
        self.visible_columns = []
        for row in range(1, self.grid_layout.rowCount()):
            chkbox = self.grid_layout.itemAtPosition(row, 0).widget()
            if chkbox.isChecked():
                self.visible_columns.append(self.grid_layout.itemAtPosition(row, 1).widget().text())
        # 将可见列传递给主程序
        self.data_signal.emit(self.visible_columns)
        print("可见列信号发送成功：", self.visible_columns)
        self.close()
    
class MyLabel(QLabel):
    def __init__(self, text):
        super().__init__(text)
        self.setSizePolicy(QSizePolicy.Preferred, QSizePolicy.Expanding)  # 设置默认大小策略