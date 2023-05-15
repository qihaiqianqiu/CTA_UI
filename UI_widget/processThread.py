from PyQt5.QtWidgets import *
from PyQt5.QtCore import *
import tqdm
from utils.calculate_parameter import *
class ProcessThread(QThread):
    update_progress = pyqtSignal(int)
    
    def __init__(self, region_info):
        super().__init__()
        self.region_info = region_info

    def run(self):
        for i, row in tqdm(self.region_info.iterrows(), total=len(self.region_info)):
            # 在这里执行需要运行的函数
            predict_info(row, end_date, end_section, q, step, ratio, flag, cache_path)
            
            # 发送进度信号
            self.update_progress.emit(i+1)