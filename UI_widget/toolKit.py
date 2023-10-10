from PyQt5 import QtCore
from PyQt5.QtWidgets import QDialog, QPushButton, QGridLayout
from PyQt5.QtCore import pyqtSignal
from UI_widget.checkParaDialog import checkParaDialog
from UI_widget.muteMonth import muteMonth
from UI_widget.realtimeStocking import stockCounter


all = ["toolKit"]
class toolKit(QDialog):
    refresh_signal = pyqtSignal(bool)
    def __init__(self):
        super().__init__()
        btn1 = QPushButton("检查参数表")
        btn1.setFixedSize(150, 75)
        btn2 = QPushButton("开启/关闭合约组")
        btn2.setFixedSize(150, 75)
        btn3 = QPushButton("检查不可转抛/反套持仓")
        btn3.setFixedSize(150, 75)
        btn4 = QPushButton("实时持仓状态")
        btn4.setFixedSize(150, 75)
        
        self.layout = QGridLayout()
        self.layout.addWidget(btn1, 0, 0)
        self.layout.addWidget(btn2, 0, 1)
        self.layout.addWidget(btn3, 1, 0)
        self.layout.addWidget(btn4, 1, 1)
        self.setLayout(self.layout)
        self.setWindowTitle("工具箱")
        btn1.clicked.connect(self.check_param_pairs)
        btn2.clicked.connect(self.mute)
        btn4.clicked.connect(self.stock_counting)

        
        
    # 将信号传递给主窗口
    def pass_add_signal(self, flag):
        print("sending signal to mainUI:", flag)
        self.refresh_signal.emit(flag)
        if flag == True:
            self.close()
            
            
    @QtCore.pyqtSlot()
    def check_param_pairs(self):
        self.check_dialog = checkParaDialog()
        self.check_dialog.add_signal_outer.connect(self.pass_add_signal)
        self.check_dialog.show()
        
    
    @QtCore.pyqtSlot()
    def mute(self):
        self.mute_month_dialog = muteMonth()
        self.mute_month_dialog.refresh_signal.connect(self.pass_add_signal)
        self.mute_month_dialog.show()
    
    @QtCore.pyqtSlot()
    def stock_counting(self):
        self.stock_counting_dialog = stockCounter()
        self.stock_counting_dialog.show()
    
    @QtCore.pyqtSlot()
    def profit_cal(self):
        pass
