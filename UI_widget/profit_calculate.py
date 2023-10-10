import os
from utils.const import Z_PATH
import pandas as pd
from PyQt5.QtWidgets import QMessageBox, QProgressBar, QDesktopWidget
from PyQt5.QtCore import Qt

def accounting_contract_pair(contract_pair:str, acc_name:str):
    """提取某账户下某合约对交易记录
    1. 读取交易记录
    2. 提取某合约对交易记录
    3. 统计盈亏
    4. 保存至本地
    """
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
    progress.setWindowTitle('导出TmpValue中...')
    progress.show()
    progress.setMaximum(len(trading_file))
    pb_counter = 0
    for file in trading_file:
        progress.setValue(pb_counter)
        df = pd.read_csv(file)
        # 提取某合约对交易记录
        res = pd.concat(res, df[df['套利对'] == contract_pair])
        pb_counter += 1
        progressbar_value = progressbar_value / len(trading_file) * 100
        progress.setValue(progressbar_value)
    # 计算盈亏
    res['净值'] = res.apply(lambda x: x['价格'] * x['手数'] if x['操作'] == '买' else x['价格'] * x['手数'] * -1, axis=1)
    total = res['净值'].sum()
    return res, total
