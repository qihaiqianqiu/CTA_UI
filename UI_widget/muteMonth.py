
# 开启/关闭反套
# 开启/关闭不可转抛合约
import sys
from PyQt5.QtWidgets import QSlider, QWidget, QGridLayout, QLabel, QDialog, QApplication
from PyQt5.QtCore import Qt

class muteMonth(QDialog):
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
        self.setLayout(self.layout)

    def on_slider_change(self, value):
        # 当用户移动滑块时触发此函数
        # 检查滑块所控制的范围
        slider = self.sender()  # 获取发射信号的对象
        row = self.layout.getItemPosition(self.layout.indexOf(slider))[0]  # 获取行
        # 获取Mute指令
        label = self.layout.itemAtPosition(row, 0).widget()  # 获取同一行的QLabel控件
        if value == 0:
            print("关闭")
        elif value == 1:
            print("开启")
        self.mute[label.text()] = self.layout.itemAtPosition(row, 1).widget().value()
        print(self.mute)
        
    def generate_line(self, instruct:str, position:int):
        label = QLabel(instruct)
        slider = QSlider(Qt.Horizontal)
        slider.setMinimum(0)
        slider.setMaximum(1)
        slider.setSliderPosition(1) # 初始状态为开启
        slider.valueChanged.connect(self.on_slider_change)
        self.layout.addWidget(label, position, 0)
        self.layout.addWidget(slider, position, 1)
        self.mute[label.text()] = slider.value()
        
    def mute_by_instruct(self, instruct:str):
        if instruct in ['1月', '2月', '3月', '4月', '5月', '6月', '7月', '8月', '9月', '10月', '11月', '12月']:
            pass
        
        
        
if __name__ == "__main__":
    app = QApplication(sys.argv)
    dialog = muteMonth()
    dialog.exec_()