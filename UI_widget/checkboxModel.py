import pandas as pd
from PyQt5.QtCore import Qt
from PyQt5.QtCore import QAbstractTableModel

all = ["checkboxModel"]

class checkboxModel(QAbstractTableModel):
    def __init__(self, data):
        super().__init__()
        self.data = data.astype(bool)  # Convert all values to booleans

    def rowCount(self, parent=None):
        return len(self.data)

    def columnCount(self, parent=None):
        return len(self.data.columns)

    def data(self, index, role=Qt.DisplayRole):
        if role == Qt.CheckStateRole:
            value = self.data.iloc[index.row(), index.column()]
            return Qt.Checked if value else Qt.Unchecked
        return None
    
    def headerData(self, section, orientation, role=Qt.DisplayRole):
        if role != Qt.DisplayRole:
            return None
        if orientation == Qt.Horizontal:
            return str(self.data.columns[section])
        else:
            return str(self.data.index[section])
        
    def setData(self, index, value, role):
        # 在用户点击checkbox后实时更新dataframe中存储的值
        if role == Qt.CheckStateRole:
            # 修改dataframe中的值为对应的bool值
            self.data.iloc[index.row(), index.column()] = bool(value)
            return True
        return False
    
    def flags(self, index):
        return Qt.ItemIsEnabled | Qt.ItemIsUserCheckable

