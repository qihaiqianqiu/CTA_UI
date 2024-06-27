import pandas as pd
from PyQt5.QtWidgets import QTableWidgetItem, QTableWidget, QApplication
from PyQt5.QtCore import *
import os, sys
from utils.date_section_modification import *
from utils.cache_management import *
from utils.calculate_parameter import *
from utils.transform import param_split
from utils.const import PARAM_PATH, default_args, INFO_PATH
from UI_widget.pandasModel import pandasModel, TableView

all = ["paraTable"]
class paraTable(QTableWidget):
    def __init__(self):
        super().__init__(0,0)              
        # 获取基参数表
        info_df = pd.read_csv(os.path.join(PARAM_PATH, 'BASE', 'params.csv')).reset_index(drop=True).set_index("pairs_id")
        # 使用pandasModel将info_df转化为表格
        region_info, boundary_info, suffix_info = param_split(info_df)
        self.model = pandasModel(info_df, checkbox_flag=True, toggle_flag=True)
        self.view = TableView(self.model)

        print("PARA TABLE GENERATED")

    # 刷新参数表并更新其在UI的显示
    # *************************************************************
    def update(self): 
        info_df = pd.read_csv(os.path.join(PARAM_PATH, 'BASE', 'params.csv')).reset_index(drop=True).set_index("pairs_id").sort_index()
        # 使用pandasModel将info_df转化为表格
        region_info, boundary_info, suffix_info = param_split(info_df)
        self.model.updateData(info_df)
        #self.view = TableView(self.model)
        
        print("PARA TABLE UPDATED")
        
        
    # 参数计算模块
    def fill_para_Table(self):
        """        
        date, section = get_date_section()
        date, section = from_predict(date, section)
        region_df = pd.read_excel('./info/region_info.xlsx', sheet_name="Sheet1")
        pairs = region_df['pairs_id']
        # 更新
        flag, filename = check_cache('BASE', date, section, pairs, default_args)
        info_df = predict_info(region_df, date, section, 0.95, 6, 1.0, flag, filename)
        # 重组为合规的Params表
        cache_param('BASE', info_df, date, section, default_args)
        save_param('BASE', info_df)
        self.model._data = info_df
        """
        pass
    
if __name__ == "__main__":
    app = QApplication(sys.argv)
    table = paraTable()
    print(table.model._data['region_unit_num'].dtype)
    sys.exit(app.exec_())
    