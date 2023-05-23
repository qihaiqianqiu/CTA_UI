from PyQt5 import Qt
from PyQt5.QtWidgets import *
from PyQt5.QtCore import *
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

all = ["pandasModel", "TableView"]
class pandasModel(QAbstractTableModel):
    # https://stackoverflow.com/questions/31475965/fastest-way-to-populate-qtableview-from-pandas-data-frame
    def __init__(self, data):
        QAbstractTableModel.__init__(self)
        original_columns = data.columns.tolist()
        data.insert(0, 'BarPlot', data.apply(lambda x: pairname_to_plotdir(x.name), axis=1))
        data.insert(0, 'CheckBox', False)
        print(data.columns)
        self._data = data

    def rowCount(self, parent=None):
        return self._data.shape[0]

    def columnCount(self, parent=None):
        return self._data.shape[1]
   
    def data(self, index, role=Qt.DisplayRole):
        if index.isValid():
            row = index.row()
            col = index.column()
            if role == Qt.DisplayRole and col >= 2:
                value = self._data.iloc[row, col]
                return str(value)
            elif role == Qt.CheckStateRole and col == 0:
                checked = self._data.iloc[row, col]
                return Qt.Checked if checked else Qt.Unchecked
            elif role == Qt.DecorationRole and col == 1:
                icon_path = os.path.join(ROOT_PATH, "src", "box-plot.png")
                if icon_path:
                    icon = QIcon(icon_path)
                    return icon
            elif role == Qt.UserRole and col == 1:
                image_path = self._data.iloc[row, col]
                print(image_path)
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
        return False


    def flags(self, index):
        if index.column() == 0:
            return Qt.ItemIsEnabled | Qt.ItemIsUserCheckable
        if index.column() == 1:
            return Qt.ItemIsEnabled | Qt.ItemIsSelectable | Qt.ItemIsEditable
        elif index.column() >= 2:
            return Qt.ItemIsEnabled | Qt.ItemIsSelectable
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
            

class TableView(QTableView):
    def __init__(self, model):
        super(TableView, self).__init__()
        self.setModel(model)

    def mousePressEvent(self, event: QMouseEvent) -> None:
        index = self.indexAt(event.pos())
        if index.column() == 1:  # 图片列
            file_path = self.model().data(index, Qt.UserRole)
            print(file_path)
            print(os.path.exists(file_path))
            if file_path:
                url = QUrl.fromLocalFile(file_path)
                url.setScheme("file")
                QDesktopServices.openUrl(url)
            else:
                print("BarPlot not exist")
        else:
            super().mousePressEvent(event)
