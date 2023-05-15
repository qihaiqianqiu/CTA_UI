import pandas as pd
from PyQt5.QtWidgets import *
from PyQt5.QtCore import *
from utils.date_section_modification import *
from utils.cache_management import *
from utils.calculate_parameter import *
from utils.transform import transform
from . import layout_constants
all = ["paraTable"]
class paraTable(QTableWidget):
    def __init__(self):
        super().__init__(0,0)              
        # 主体：表格
        width = int(QDesktopWidget().screenGeometry().width() * 0.95)
        height = int(QDesktopWidget().screenGeometry().height() * 0.95)
        self.setFrameShape(QFrame.NoFrame)  ##设置无表格的外框
        self.setSelectionBehavior(QAbstractItemView.SelectRows) #设置 不可选择单个单元格，只可选择一行。
        self.setGeometry(layout_constants.X_OFFSET, layout_constants.Y_OFFSET, width, height)
        self.horizontalHeader().setSectionsClickable(False) #可以禁止点击表头的列
        self.setEditTriggers(QAbstractItemView.NoEditTriggers) #设置表格不可更改
        #self.sortItems(1,Qt.DescendingOrder) #设置按照第二列自动降序排序
        #self.setColumnHidden(1,True)#将第二列隐藏
        #self.setSortingEnabled(True)#设置表头可以自动排序
        # Init with Base(also export base)
        date, section = get_date_section()
        date, section = from_predict(date, section)
        region_df = pd.read_excel('./info/region_info.xlsx', sheet_name="Sheet1")
        pairs = region_df['pairs_id']
        default_args = (0.95, 6, 1.0)
        # 在启动时不要自动更新
        flag, filename = get_most_updated_cache('BASE', pairs, default_args)
        info_df = predict_info(region_df, date, section, 0.95, 6, 1.0, flag, filename)
        info_df = transform(info_df)
        cache_param('BASE', info_df, date, section, default_args)
        save_param('BASE', info_df)
        self.fill_para_table(info_df)
        self.show()
        print("PARA TABLE GENERATED")
    
    def fill_para_table(self, df):
        self.setRowCount(0)
        self.clearContents()
        self.setColumnCount(len(df.columns))
        self.columns = df.columns
        self.setHorizontalHeaderLabels(self.columns)#设置表头文字
        for i in range(len(self.columns)):
            self.horizontalHeader().resizeSection(i,150)
        new_content = df.values.tolist()
        for row in range(len(new_content)):
            self.setRowCount(row+1)
            for i in range(len(df.columns)):
                self.setItem(row,i,QTableWidgetItem(str(new_content[row][i])))
        print("Parameter table set")

    def update(self):
        date, section = get_date_section()
        date, section = from_predict(date, section)
        region_df = pd.read_excel('./info/region_info.xlsx', sheet_name="Sheet1")
        pairs = region_df['pairs_id']
        default_args = (0.95, 6, 1.0)
        # 更新
        flag, filename = check_cache('BASE', date, section, pairs, default_args)
        info_df = predict_info(region_df, date, section, 0.95, 6, 1.0, flag, filename)
        info_df = transform(info_df)
        cache_param('BASE', info_df, date, section, default_args)
        save_param('BASE', info_df)
        self.fill_para_table(info_df)