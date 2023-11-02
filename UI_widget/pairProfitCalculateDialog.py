from PyQt5.QtWidgets import QLabel, QLineEdit, QDialog, QDialogButtonBox, QGridLayout, QTableWidget, QMessageBox, QProgressBar, QHBoxLayout, QDesktopWidget
from PyQt5.QtCore import Qt
from UI_widget.pandasModel import pandasModel, TableView
from utils.const import Z_PATH, multiple_dict
import pandas as pd
import os, re
all = ["pairProfitCalculateDialog"]

class pairProfitCalculateDialog(QDialog):
    def __init__(self):
        super().__init__()
        self.acc = QLineEdit()
        self.acc_label = QLabel("请输入账户:")
        self.contract_pair = QLineEdit()
        self.contract_pair_label = QLabel("请输入回看套利对(eg.SC2308-2309):")
        self.trans_table = QTableWidget()
        self.model = pandasModel(pd.DataFrame(), barplot_flag=False, checkbox_flag=False)
        self.view = TableView(self.model)
        layout = QHBoxLayout()
        layout.addWidget(self.view)
        self.trans_table.setLayout(layout)
        self.buttonBox = QDialogButtonBox(QDialogButtonBox.Ok
                             | QDialogButtonBox.Cancel)

        self.buttonBox.accepted.connect(lambda: self.cal_profit(self.trans_table))
        self.buttonBox.rejected.connect(self.reject)

        # 默认参数

        layout = QGridLayout()
        layout.addWidget(self.acc_label)
        layout.addWidget(self.acc)
        layout.addWidget(self.contract_pair_label)
        layout.addWidget(self.contract_pair)
        layout.addWidget(self.buttonBox,2,1,Qt.Alignment(Qt.AlignCenter))
        self.setLayout(layout)
        self.setWindowTitle("导出套利图")
        

    def cal_profit(self, trans_table):
        """提取某账户下某合约对交易记录
        1. 读取交易记录
        2. 提取某合约对交易记录
        3. 统计盈亏
        """
        # 获取窗口的账户和套利对
        acc_name = self.acc.text()
        contract_pair = self.contract_pair.text()
        # 提取品种
        breed = re.findall(r'[a-zA-Z]+', contract_pair)[0]
        # 账户交易记录所在目录
        acc_dir = os.path.join(Z_PATH, "tradings", acc_name)
        trading_file = [os.path.join(acc_dir, f) for f in os.listdir(acc_dir) if f.endswith(".csv") and "_sorted" in f]
        if len(trading_file) == 0:
            QMessageBox.warning("错误", "该账户下无交易记录")
            return
        # 读取交易记录
        res = pd.DataFrame()
        progress = QProgressBar()
        # 设置进度条大小
        progress_width = 600
        progress_height = 50
        progress.setGeometry(0, 0, progress_width, progress_height)
        # 计算进度条位置并设置
        desktop = QDesktopWidget().screenGeometry()
        x = (desktop.width() - progress_width) / 2
        y = (desktop.height() - progress_height) / 2
        progress.move(int(x), int(y))
        progress.setAlignment(Qt.AlignCenter)
        progress.setWindowTitle('核算套利对交易记录中...')
        progress.show()
        progress.setMaximum(len(trading_file))
        pb_counter = 0
        for file in trading_file:
            progress.setValue(pb_counter)
            df = pd.read_csv(file, encoding="GBK")
            # 提取某合约对交易记录
            res = pd.concat([res, df[df['套利对'] == contract_pair]])
            pb_counter += 1
        # 计算盈亏
        res = res.set_index('套利对')
        # 将res转化为pandasModel
        self.model.updateData(res)
        res['成交单位'] = res.apply(lambda x: x['价格'] * x['手数']  * -1 if x['操作'] == '买' else x['价格'] * x['手数'], axis=1)
        res['累计净值'] = (res['成交单位'] * multiple_dict[breed]).cumsum()  
        self.trans_table.show()
        return res
