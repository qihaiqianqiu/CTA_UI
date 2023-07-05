from PyQt5 import Qt
from PyQt5.QtGui import QIcon, QDesktopServices, QMouseEvent
from PyQt5.QtWidgets import QTableView, QAbstractItemView, QAbstractScrollArea, QHeaderView
from PyQt5.QtCore import QUrl, QModelIndex, QAbstractTableModel, Qt
from PyQt5.QtGui import QIcon, QDesktopServices, QMouseEvent
import os
from utils.plotfile_management import pairname_to_plotdir
from utils.const import ROOT_PATH
import pandas as pd

#显示所有列
pd.set_option('display.max_columns', None)
#显示所有行
pd.set_option('display.max_rows', None)
#设置value的显示长度为100，默认为50
pd.set_option('max_colwidth',100)


# 待实现的功能：点击套利对名称即可自动打开对应时序套利图
# PandasModel对于属于其中的df比较索引依赖，因此需要将df的index重置为套利对名称

all = ["pandasModel", "TableView"]

class pandasModel(QAbstractTableModel):
    def __init__(self, data, barplot_flag=True, checkbox_flag=True):
        QAbstractTableModel.__init__(self)
        self.barplot_flag = barplot_flag
        self.checkbox_flag = checkbox_flag
        self._data = self.addColumnToDf(data, barplot_flag, checkbox_flag)
        # 在.0时不显示小数
        def convert_decimal_to_integer(x):
            if isinstance(x, float) and x.is_integer():  # 检查元素是否为浮点数且小数部分为 0
                return int(x)
            else:
                return x
        self._data = self._data.applymap(convert_decimal_to_integer)
        
          
            
    def rowCount(self, parent=None):
        return self._data.shape[0]


    def columnCount(self, parent=None):
        return self._data.shape[1]
   
   
    def data(self, index, role=Qt.DisplayRole):
        if index.isValid():
            row = index.row()
            col = index.column()
            if role == Qt.DisplayRole and col >= self.checkbox_flag + self.barplot_flag:
                value = self._data.iloc[row, col]
                return str(value)
            elif self.checkbox_flag and role == Qt.CheckStateRole and col == 0:
                checked = self._data.iloc[row, col]
                return Qt.Checked if checked else Qt.Unchecked
            elif self.barplot_flag and role == Qt.DecorationRole and col == self.checkbox_flag:
                icon_path = os.path.join(ROOT_PATH, "src", "box-plot.png")
                if icon_path:
                    icon = QIcon(icon_path)
                    return icon
            elif self.barplot_flag and role == Qt.UserRole and col == self.checkbox_flag:
                image_path = self._data.iloc[row, col]
                #print(image_path)
                return image_path
            """
            elif role == Qt.MouseButtonPressRole and col == 1:
                # Open file on mouse button press in image column
                image_path = pairname_to_plotdir(self._data.iloc[row, col-1])
                if image_path:
                    QDesktopServices.openUrl(QUrl.fromLocalFile(image_path))
            """
        return None
    
    
    def headerData(self, section, orientation, role=Qt.DisplayRole):
        if role != Qt.DisplayRole:
            return None
        if orientation == Qt.Horizontal:
            return str(self._data.columns[section])
        else:
            return str(self._data.index[section])


    def setData(self, index, value, role):
        if role == Qt.CheckStateRole and index.column() == 0:
            row = index.row()
            checked = value == Qt.Checked
            self._data.iloc[row, index.column()] = checked
            self.dataChanged.emit(index, index)
            return True
        if role == Qt.EditRole:
            row = index.row()
            col = index.column()
            self._data.iloc[row, col] = value
            self.dataChanged.emit(index, index)
            return True
        return False


    def flags(self, index):
        if self.checkbox_flag and index.column() == 0:
            return Qt.ItemIsEnabled | Qt.ItemIsUserCheckable
        elif self.barplot_flag and index.column() == self.checkbox_flag:
            return Qt.ItemIsEnabled | Qt.ItemIsSelectable | Qt.ItemIsEditable
        elif index.column() >= self.barplot_flag + self.checkbox_flag:
            return Qt.ItemIsEnabled | Qt.ItemIsSelectable | Qt.ItemIsEditable
        else:
            return Qt.ItemIsEnabled
    
    
    def mousePressEvent(self, event):
        index = self.indexAt(event.pos())
        if event.button() == Qt.LeftButton:
            if index.row() != -1:
                self._selected_rows = {index.row()}
            elif index.column() != -1:
                self._selected_cols = {index.column()}
            else:
                self._selected_rows = set()
                self._selected_cols = set()
            self.dataChanged.emit(QModelIndex(), QModelIndex())
        elif event.button() == Qt.RightButton:
            self._selected_rows = set()
            self._selected_cols = set()
            self.dataChanged.emit(QModelIndex(), QModelIndex())
        else:
            super().mousePressEvent(event)
            
            
    def mouseMoveEvent(self, event):
        index = self.indexAt(event.pos())
        if event.buttons() & Qt.LeftButton:
            if index.row() != -1:
                self._selected_rows.add(index.row())
            elif index.column() != -1:
                self._selected_cols.add(index.column())
            self.dataChanged.emit(QModelIndex(), QModelIndex())
        else:
            super().mouseMoveEvent(event)
            
            
    def addColumnToDf(self, data, barplot_flag, checkbox_flag):
        if barplot_flag:
            data.insert(0, 'BarPlot', data.apply(lambda x: pairname_to_plotdir(x.name), axis=1))
        if checkbox_flag:
            data.insert(0, 'CheckBox', False)
        return data
    
            

class TableView(QTableView):
    def __init__(self, model):
        super(TableView, self).__init__()
        self.setModel(model)
        self.barplot_flag = model.barplot_flag
        self.checkbox_flag = model.checkbox_flag
        self.updateHeaderSize()
        self.setEditTriggers(QAbstractItemView.DoubleClicked)
        self.setSizeAdjustPolicy(QAbstractScrollArea.AdjustToContents)
        for i in range(1, self.model().columnCount()):
            self.setColumnWidth(i, 100) 

    def updateHeaderSize(self):
        for row in range(self.model().rowCount()):
            index = self.model().index(row, 0)
            sizeHint = self.sizeHintForIndex(index)
            self.verticalHeader().resizeSection(row, sizeHint.height())
            
        for col in range(self.model().columnCount()):
            index = self.model().index(0, col)
            text = self.model().headerData(col, Qt.Horizontal)
            self.horizontalHeader().resizeSection(col, len(text) * 8)
            
            
    def mousePressEvent(self, event: QMouseEvent) -> None:
        index = self.indexAt(event.pos())
        if self.barplot_flag and index.column() == self.checkbox_flag:  # 图片列
            file_path = self.model().data(index, Qt.UserRole)
            print(file_path)
            #print(os.path.exists(file_path))
            if file_path:
                url = QUrl.fromLocalFile(file_path)
                url.setScheme("file")
                QDesktopServices.openUrl(url)
            else:
                print("BarPlot not exist")
        else:
            super().mousePressEvent(event)
