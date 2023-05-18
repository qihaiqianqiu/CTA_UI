from PyQt5 import Qt
from PyQt5.QtWidgets import *
from PyQt5.QtCore import *
all = ["pandasModel"]
class pandasModel(QAbstractTableModel):
    # https://stackoverflow.com/questions/31475965/fastest-way-to-populate-qtableview-from-pandas-data-frame
    def __init__(self, data):
        QAbstractTableModel.__init__(self)
        self._data = data

    def rowCount(self, parent=None):
        return self._data.shape[0]

    def columnCount(self, parent=None):
        return self._data.shape[1]
   
    def data(self, index, role=Qt.DisplayRole):
        print(index.row(), index.column())
        print("++++++")
        if index.isValid():
            if role == Qt.DisplayRole:
                value = self._data.iloc[index.row(), index.column()]
                print(index.row(), index.column(), value)
                return str(value)
            elif role == Qt.BackgroundRole:
                #return QColor(Qt.white)
                pass
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
            if len(self._data.index.names) == 1:
                self._data.iloc[index.row()] = [value == Qt.Checked] + list(self._data.iloc[index.row(), 1:])
            else:
                row_values = list(self._data.index[index.row()])
                for i in range(len(self._data.index.names)):
                    if self._data.index.names[i] == index.internalPointer().name():
                        row_values[i] = value == Qt.Checked
                self._data.set_value(tuple(row_values), self._data.columns[index.column()], value == Qt.Checked)
            self.dataChanged.emit(index, index)
            return True
        return False

    def flags(self, index):
        if index.column() == 0:
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