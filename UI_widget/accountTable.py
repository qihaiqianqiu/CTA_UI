from PyQt5.QtWidgets import QDesktopWidget, QTableWidget, QAbstractItemView, QFrame
from . import layout_constants
all = ["accountTable"]
class accountTable(QTableWidget):
    def __init__(self):
        super().__init__(0,layout_constants.ACC_TABLE_COL_NUMBER)               
        # 主体：表格
        width = int(QDesktopWidget().screenGeometry().width() * 0.95)     
        self.columns = ['√', 'id', 'describe' , 'status', 'account', 'region_budget', 'boundary_budget', 'is_cffex', 'is_back', 'is_boundary_3', 'is_boundary_4']
        self.setHorizontalHeaderLabels(self.columns)#设置表头文字
        self.setFrameShape(QFrame.NoFrame)  ##设置无表格的外框
        self.setSelectionBehavior(QAbstractItemView.SelectRows) #设置 不可选择单个单元格，只可选择一行。
        self.horizontalHeader().resizeSection(0,layout_constants.CHECKBOX_WID) #设置第一列的宽度为70
        for i in range(1, len(self.columns)):
            self.horizontalHeader().resizeSection(i,layout_constants.ACC_TABLE_COL_WID)
        self.setGeometry(layout_constants.X_OFFSET, layout_constants.Y_OFFSET, width, layout_constants.ACC_TABLE_HEI)
        self.horizontalHeader().setSectionsClickable(False) #可以禁止点击表头的列
        self.setEditTriggers(QAbstractItemView.NoEditTriggers) #设置表格不可更改
        self.show()
        print("ACC TABLE GENERATED")
