
# 开启/关闭反套
# 开启/关闭不可转抛合约
import sys, os
import pandas as pd
from PyQt5.QtWidgets import QSlider, QGridLayout, QLabel, QDialog, QApplication, QDialogButtonBox, QCheckBox, QMessageBox
from PyQt5.QtCore import Qt
from PyQt5.QtCore import pyqtSignal
from utils.const import PARAM_PATH
from utils.mute import mute_by_instruct

class muteMonth(QDialog):
    refresh_signal = pyqtSignal(bool)
    def __init__(self):
        super().__init__()
        self.layout = QGridLayout()
        self.mute = {}
        # 添加全部功能
        for i in range(12):
            self.generate_line(str(i+1) + "月", i)
        self.generate_line("反套", i+1)
        self.generate_line("不可转抛合约", i+2)
        self.generate_line("全合约", i+3)
        self.buttonBox = QDialogButtonBox(QDialogButtonBox.Ok | QDialogButtonBox.Cancel)
        self.buttonBox.accepted.connect(self.submit_mute)
        self.buttonBox.rejected.connect(self.reject)
        self.layout.addWidget(self.buttonBox, i+4, 0, 1, 2)
        self.setLayout(self.layout)
        self.title = "开启/关闭合约交易"
        

        
    def generate_line(self, instruct:str, position:int):
        label = QLabel(instruct)
        slider = QSlider(Qt.Horizontal)
        checker = QCheckBox()
        slider.setMinimum(0)
        slider.setMaximum(1)
        slider.setSliderPosition(1) # 初始状态为开启
        self.layout.addWidget(checker, position, 0)
        self.layout.addWidget(label, position, 1)
        self.layout.addWidget(slider, position, 2)
                
        
    def submit_mute(self):
        df = pd.read_csv(os.path.join(PARAM_PATH, 'BASE', "params.csv"))
        # 遍历被选中的操作
        for i in range(15):
            rowWidget = self.layout.itemAtPosition(i, 0).widget()
            if rowWidget.isChecked():
                instruct = self.layout.itemAtPosition(i, 1).widget().text()
                self.mute[instruct] = self.layout.itemAtPosition(i, 2).widget().value()
        # 根据mute指令修改df
        if(len(self.mute) == 0):
            QMessageBox.warning(self, "警告", "未选择任何操作")
        else:
            txt = ""
            for key, values in self.mute.items():
                if values == 0:
                    txt += key + " " + "合约关闭" + "\n"
                else:
                    txt += key + " " + "合约开启" + "\n"
                df = mute_by_instruct(key, values, df)        
            df.to_csv(os.path.join(PARAM_PATH, 'BASE', "params.csv"), index=False)
            self.mute = {}
            self.refresh_signal.emit(True)
            QMessageBox.information(self, "修改成功", txt)
            self.close()
    

if __name__ == "__main__":
    app = QApplication(sys.argv)
    dialog = muteMonth()
    dialog.exec_()