#!/usr/bin/python3
# -*- coding: utf-8 -*-
"""
Author: Han Hao
Last edited: December 2022
"""
import sys, traceback
import pandas as pd
from PyQt5 import QtCore, QtGui, QtWidgets, Qt
from PyQt5.QtWidgets import QProgressBar, QApplication, QMainWindow, QDesktopWidget, QAbstractItemView, QAction, QPushButton, QFileDialog, QMessageBox, QCheckBox, QHBoxLayout, QVBoxLayout, QWidget, QTableWidgetItem, QDialog, QSizePolicy 
from PyQt5.QtCore import Qt, QSize
import matplotlib.pyplot as plt
import matplotlib
matplotlib.use('Qt5Agg')
from matplotlib.backends.backend_qt5agg import FigureCanvasQTAgg as FigureCanvas
import qdarkstyle
import datetime
import concurrent.futures
from tqdm import tqdm
import os
from utils import *
from UI_widget import *

class Arbitrator(QMainWindow):    
    def __init__(self):
        super().__init__()
        # in_buffer: 账户表当前内容
        self.in_buffer = []
        self.edit_buffer = []
        self.param_edit_buffer = pd.DataFrame()
        # 账户表
        self.table = accountTable()
        # 参数表
        self.param = paraTable()
        self.editable = False
        self.setStyleSheet(qdarkstyle.load_stylesheet_pyqt5())
        # layout management
        self.container = QtWidgets.QWidget()
        self.param_layout = QtWidgets.QHBoxLayout()
        self.initUI()

    # 布局：菜单 - 读取（打开）套利对CSV
    #       添加套利对 - 批量导入
    #                 - 手动录入
    #       删除套利对 - 手动删除
    #       提交增删结果
    #       启动可视化

    def initUI(self):
        width = int(QDesktopWidget().screenGeometry().width() * 0.95)
        height = int(QDesktopWidget().screenGeometry().height() * 0.95)
        # File tag
        # SFTP
        appendAct = QAction('SFTP', self)
        appendAct.setStatusTip('SFTP文件传输')
        appendAct.triggered.connect(self.open)

        # 重载
        appendAct = QAction('Reload', self)
        appendAct.setShortcut('Ctrl+O')
        appendAct.setStatusTip('重载账户表')
        appendAct.triggered.connect(self.open)
        
        # 退出
        exitAct = QAction('Exit', self)
        exitAct.setShortcut('Ctrl+L')
        exitAct.setStatusTip('退出')
        exitAct.triggered.connect(self.close)

        # 保存
        saveAct = QAction('Save', self)
        saveAct.setShortcut('Ctrl+S')
        saveAct.setStatusTip('保存当前账户参数信息')
        saveAct.triggered.connect(self.save)
        
        # 期货信息表
        dictAct = QAction('Show Dict', self)
        dictAct.setShortcut('Ctrl+M')
        dictAct.triggered.connect(self.showDict)

        # 删除参数表中的勾选行
        deletAct = QAction('Delete Param', self)
        deletAct.setShortcut('Ctrl+D')
        deletAct.triggered.connect(self.delete_param)
        
        # 撤回参数表修改
        undoAct_param = QAction('Undo Param', self)
        undoAct_param.setShortcut('Ctrl+Z')
        undoAct_param.triggered.connect(self.undo_delete_param)
        
        # 撤回账户表修改
        undoAct_acc = QAction('Undo Account', self)
        undoAct_acc.setShortcut('Ctrl+X')
        undoAct_acc.triggered.connect(self.undo)

        # 全选
        selectAllAct = QAction('Click all checkbox', self)
        selectAllAct.setShortcut('Ctrl+A')
        selectAllAct.triggered.connect(self.update_check_all_state)

        # 菜单栏
        menuBar = self.menuBar()
        self.fileMenu = menuBar.addMenu('&File')
        self.fileMenu.addAction(appendAct)
        self.fileMenu.addAction(exitAct)


        self.dictMenu = menuBar.addMenu('&Dict')
        self.dictMenu.addAction(dictAct)
        
        
        self.editMenu = menuBar.addMenu('&Edit')
        self.editMenu.addAction(saveAct)
        self.editMenu.addAction(selectAllAct)
        self.editMenu.addAction(deletAct)
        self.editMenu.addAction(undoAct_param)
        self.editMenu.addAction(undoAct_acc)
        
        # ToolBar
        toolBar = self.addToolBar("")

        calculate = QAction(QtGui.QIcon("./src/reload.png"), "calculate parameter", self)
        calculate.triggered.connect(self.update_base_info)
        toolBar.addAction(calculate)

        addAccount = QAction(QtGui.QIcon("./src/add-user.png"), "add account", self)
        addAccount.triggered.connect(self.add)
        toolBar.addAction(addAccount)

        delAccount = QAction(QtGui.QIcon("./src/delete-user.png"), "delete account", self)
        delAccount.triggered.connect(self.delete_user)
        toolBar.addAction(delAccount)

        editAccount = QAction(QtGui.QIcon("./src/edit.png"), "edit account", self)
        editAccount.triggered.connect(self.set_editable)
        toolBar.addAction(editAccount)

        up = QAction(QtGui.QIcon("./src/up-arrows.png"), "up", self)
        up.triggered.connect(self.move_row_up)
        toolBar.addAction(up)

        down = QAction(QtGui.QIcon("./src/down-arrows.png"), "down", self)
        down.triggered.connect(self.move_row_down)
        toolBar.addAction(down)

        toolBar.setIconSize(QSize(40,40))


        # statusbar
        self.status = self.statusBar()
        self.status.show()
        self.status.showMessage('就绪')


        self.setCentralWidget(self.container)
        self.layout = QtWidgets.QGridLayout(self.container)

        # Button
        button_layout_line1 = QtWidgets.QHBoxLayout()
        button_layout_line2 = QtWidgets.QHBoxLayout()
        button_layout_line3 = QtWidgets.QHBoxLayout()
        button_layout_line4 = QtWidgets.QHBoxLayout()

        font = QtGui.QFont()
        font.setFamily("方正粗黑宋简体")
        font.setPointSize(18)

        # 计算界参数
        btn4 = QPushButton("导出参数", self)
        btn4.setFont(font)
        btn4.setObjectName("pushButton")
        btn4.setStyleSheet("QPushButton{background:qlineargradient(spread:reflect, x1:0, y1:1, x2:0, y2:0, "
                             "stop:0 #0a4a83, stop:0.5 #186e99, stop:1 #239cd8);"
                             "border:2px solid qlineargradient(spread:pad, x1:1, y1:1, x2:1, y2:0,"
                             "stop:0 #002aff, stop:0.5 #00aeff, stop:1 #00e6fc); border-radius:1px; "
                             "color:#d0f2f5} "
                             "QPushButton:hover:!pressed {border:1px solid #f8878f;}"
                             "QPushButton:pressed {padding-left:6px;padding-top:6px;border:1px solid #f8878f;}")
        btn4.clicked.connect(self.export_param)
        
        btn5 = QPushButton("导出图像", self)
        btn5.setFont(font)
        btn5.setObjectName("pushButton")
        btn5.setStyleSheet("QPushButton{background:qlineargradient(spread:reflect, x1:0, y1:1, x2:0, y2:0, "
                             "stop:0 #0a4a83, stop:0.5 #186e99, stop:1 #239cd8);"
                             "border:2px solid qlineargradient(spread:pad, x1:1, y1:1, x2:1, y2:0,"
                             "stop:0 #002aff, stop:0.5 #00aeff, stop:1 #00e6fc); border-radius:1px; "
                             "color:#d0f2f5} "
                             "QPushButton:hover:!pressed {border:1px solid #f8878f;}"
                             "QPushButton:pressed {padding-left:6px;padding-top:6px;border:1px solid #f8878f;}")
        btn5.clicked.connect(self.visualize)

        btn6 = QPushButton("导出持仓对比", self)
        btn6.setFont(font)
        btn6.setObjectName("pushButton")
        btn6.setStyleSheet("QPushButton{background:qlineargradient(spread:reflect, x1:0, y1:1, x2:0, y2:0, "
                             "stop:0 #0a4a83, stop:0.5 #186e99, stop:1 #239cd8);"
                             "border:2px solid qlineargradient(spread:pad, x1:1, y1:1, x2:1, y2:0,"
                             "stop:0 #002aff, stop:0.5 #00aeff, stop:1 #00e6fc); border-radius:1px; "
                             "color:#d0f2f5} "
                             "QPushButton:hover:!pressed {border:1px solid #f8878f;}"
                             "QPushButton:pressed {padding-left:6px;padding-top:6px;border:1px solid #f8878f;}")
        btn6.clicked.connect(self.audit)

        btn7 = QPushButton("导出TmpValue", self)
        btn7.setFont(font)
        btn7.setObjectName("pushButton")
        btn7.setStyleSheet("QPushButton{background:qlineargradient(spread:reflect, x1:0, y1:1, x2:0, y2:0, "
                             "stop:0 #0a4a83, stop:0.5 #186e99, stop:1 #239cd8);"
                             "border:2px solid qlineargradient(spread:pad, x1:1, y1:1, x2:1, y2:0,"
                             "stop:0 #002aff, stop:0.5 #00aeff, stop:1 #00e6fc); border-radius:1px; "
                             "color:#d0f2f5} "
                             "QPushButton:hover:!pressed {border:1px solid #f8878f;}"
                             "QPushButton:pressed {padding-left:6px;padding-top:6px;border:1px solid #f8878f;}")
        btn7.clicked.connect(self.fixTmpValue)

        btn8 = QPushButton("导出交易记录", self)
        btn8.setFont(font)
        btn8.setObjectName("pushButton")
        btn8.setStyleSheet("QPushButton{background:qlineargradient(spread:reflect, x1:0, y1:1, x2:0, y2:0, "
                             "stop:0 #0a4a83, stop:0.5 #186e99, stop:1 #239cd8);"
                             "border:2px solid qlineargradient(spread:pad, x1:1, y1:1, x2:1, y2:0,"
                             "stop:0 #002aff, stop:0.5 #00aeff, stop:1 #00e6fc); border-radius:1px; "
                             "color:#d0f2f5} "
                             "QPushButton:hover:!pressed {border:1px solid #f8878f;}"
                             "QPushButton:pressed {padding-left:6px;padding-top:6px;border:1px solid #f8878f;}")
        btn8.clicked.connect(self.fixTradeRecord)

        btn9 = QPushButton("导出成交对比", self)
        btn9.setFont(font)
        btn9.setObjectName("pushButton")
        btn9.setStyleSheet("QPushButton{background:qlineargradient(spread:reflect, x1:0, y1:1, x2:0, y2:0, "
                             "stop:0 #0a4a83, stop:0.5 #186e99, stop:1 #239cd8);"
                             "border:2px solid qlineargradient(spread:pad, x1:1, y1:1, x2:1, y2:0,"
                             "stop:0 #002aff, stop:0.5 #00aeff, stop:1 #00e6fc); border-radius:1px; "
                             "color:#d0f2f5} "
                             "QPushButton:hover:!pressed {border:1px solid #f8878f;}"
                             "QPushButton:pressed {padding-left:6px;padding-top:6px;border:1px solid #f8878f;}")
        btn9.clicked.connect(self.export_trading_compare_table)

        btn_check = QPushButton("工具箱", self)
        btn_check.setFont(font)
        btn_check.setObjectName("pushButton")
        btn_check.setStyleSheet("QPushButton{background:qlineargradient(spread:reflect, x1:0, y1:1, x2:0, y2:0, "
                             "stop:0 #0a4a83, stop:0.5 #186e99, stop:1 #239cd8);"
                             "border:2px solid qlineargradient(spread:pad, x1:1, y1:1, x2:1, y2:0,"
                             "stop:0 #002aff, stop:0.5 #00aeff, stop:1 #00e6fc); border-radius:1px; "
                             "color:#d0f2f5} "
                             "QPushButton:hover:!pressed {border:1px solid #f8878f;}"
                             "QPushButton:pressed {padding-left:6px;padding-top:6px;border:1px solid #f8878f;}")
        btn_check.clicked.connect(self.open_toolkit)
        
        btn_up = QPushButton("上传参数表", self)
        btn_up.setFont(font)
        btn_up.setObjectName("pushButton")
        btn_up.setStyleSheet("QPushButton{background:qlineargradient(spread:reflect, x1:0, y1:1, x2:0, y2:0, "
                             "stop:0 #0a4a83, stop:0.5 #186e99, stop:1 #239cd8);"
                             "border:2px solid qlineargradient(spread:pad, x1:1, y1:1, x2:1, y2:0,"
                             "stop:0 #002aff, stop:0.5 #00aeff, stop:1 #00e6fc); border-radius:1px; "
                             "color:#d0f2f5} "
                             "QPushButton:hover:!pressed {border:1px solid #f8878f;}"
                             "QPushButton:pressed {padding-left:6px;padding-top:6px;border:1px solid #f8878f;}")
        btn_up.clicked.connect(self.upload_param)

        btn_down = QPushButton("下载三大表", self)
        btn_down.setFont(font)
        btn_down.setObjectName("pushButton")
        btn_down.setStyleSheet("QPushButton{background:qlineargradient(spread:reflect, x1:0, y1:1, x2:0, y2:0, "
                             "stop:0 #0a4a83, stop:0.5 #186e99, stop:1 #239cd8);"
                             "border:2px solid qlineargradient(spread:pad, x1:1, y1:1, x2:1, y2:0,"
                             "stop:0 #002aff, stop:0.5 #00aeff, stop:1 #00e6fc); border-radius:1px; "
                             "color:#d0f2f5} "
                             "QPushButton:hover:!pressed {border:1px solid #f8878f;}"
                             "QPushButton:pressed {padding-left:6px;padding-top:6px;border:1px solid #f8878f;}")
        btn_down.clicked.connect(self.download_holdings)

        button_layout_line2.addWidget(btn4)
        button_layout_line2.addWidget(btn7)
        button_layout_line2.addWidget(btn8)
        button_layout_line3.addWidget(btn5)
        button_layout_line3.addWidget(btn6)
        button_layout_line3.addWidget(btn9)
        button_layout_line4.addWidget(btn_check)
        button_layout_line4.addWidget(btn_up)
        button_layout_line4.addWidget(btn_down)

        # Table
        table_layout = QtWidgets.QHBoxLayout()
        table_layout.addWidget(self.table)

        # Parameter table
        self.param_layout.addWidget(self.param.view)

        # Global setting
        self.layout.addLayout(table_layout, 0, 0)
        self.layout.addLayout(button_layout_line1, 1, 0)
        self.layout.addLayout(button_layout_line2, 2, 0)
        self.layout.addLayout(button_layout_line3, 3, 0)
        self.layout.addLayout(button_layout_line4, 4, 0)
        self.layout.addLayout(self.param_layout, 5, 0)
        self.layout.setRowStretch(0, 0)
        self.layout.setRowStretch(1, 0)
        self.layout.setRowStretch(2, 0)
        self.layout.setRowStretch(3, 0)
        self.layout.setRowStretch(4, 0)
        self.layout.setRowStretch(5, 6)
        self.setGeometry(X_OFFSET, Y_OFFSET, width, height)
        self.setWindowTitle('Arbitrager')
        self.openDefaultFile()
        self.show()
        
        
    @QtCore.pyqtSlot()
    def openDefaultFile(self):
        df = pd.read_excel('./info/account_info.xlsx', sheet_name="Sheet1")
        for line in df.values.tolist():
            self.addLine(line)


    @QtCore.pyqtSlot()
    def open(self):
        openfile_path = QFileDialog.getOpenFileName(self,'选择文件','','XLSX files(*.xlsx)')
        for pair in pd.read_excel(openfile_path[0], sheet_name='Sheet1').values.tolist():
            self.addLine(pair)
        self.status.showMessage("账户配置导入完成")
    
    
    # 未启用：SFTP传输模块
    @QtCore.pyqtSlot()
    def sftp_transfer(self):
        openfile_path = QFileDialog.getOpenFileName(self,'选择文件','','XLSX files(*.xlsx)')
        for pair in pd.read_excel(openfile_path[0], sheet_name='Sheet1').values.tolist():
            self.addLine(pair)
        self.status.showMessage("账户配置导入完成")

    @QtCore.pyqtSlot()
    def estimate_holding(self, dialog):
        b = dialog.layout_breed_label
        p = dialog.layout_breed_price
        v = dialog.layout_breed_vol
        holding_dict = {}
        sum = 0
        for i in range(b.count()):
            breed = b.itemAt(i).widget().text()
            price = p.itemAt(i).widget().text()
            volume = v.itemAt(i).widget().text()
            secure = const.secury_deposit_d1_dict[breed]
            multiple = const.multiple_dict[breed]
            value = float(price) * int(volume) * float(secure) * int(multiple)
            holding_dict[breed] = value
            sum += value
        txt = "总仓: " + str(round(sum)) + '\n'
        for key, values in holding_dict.items():
            if values > 0:
                line = key + " " + str(round(values)) + '\n'
                txt += line
        QMessageBox.information(self, "持仓估算完成", txt)
        

    @QtCore.pyqtSlot()
    def update_inbuffer(self):
        current_content = []
        for i in range(self.table.rowCount(), 0, -1):
            temp = [self.table.item(i-1,j).text() for j in range(1, self.table.columnCount())]
            current_content.insert(0, temp)
        self.in_buffer = current_content


    @QtCore.pyqtSlot()
    def addLine(self, line:list):
        row = self.table.rowCount()
        self.table.setRowCount(row + 1)
        ckBox = QCheckBox()
        h = QHBoxLayout()
        h.setAlignment(Qt.AlignCenter)
        h.addWidget(ckBox)
        w = QWidget()
        w.setLayout(h)
        self.table.setCellWidget(row,0,w)
        for i in range(len(line)):
            self.table.setItem(row,i+1,QTableWidgetItem(str(line[i])))
        self.update_inbuffer()


    # 添加账户的对话框
    @QtCore.pyqtSlot()
    def add_dialog(self, dialog):
        print("Add ",type(dialog.accepted))
        id = dialog.id.text()
        describe = dialog.describe.text()
        status = dialog.status.text()
        account = dialog.account.text()
        region_budget = dialog.region_budget.text()
        boundary_budget = dialog.boundary_budget.text()
        is_cffex = dialog.is_cffex.text()
        is_back = dialog.is_back.text()
        is_boundary_3 = dialog.is_boundary_3.text()
        is_boundary_4 = dialog.is_boundary_4.text()

        self.addLine([id, describe, status, account, region_budget, boundary_budget, is_cffex, is_back, is_boundary_3, is_boundary_4])
        self.table.scrollToBottom()
        self.status.showMessage("添加账户:"+ str(id))


    # 以下为更新参数表模块
    @QtCore.pyqtSlot()
    def open_toolkit(self):
        self.toolkit = toolKit()
        self.toolkit.show()
        self.toolkit.refresh_signal.connect(lambda flag: self.refresh(flag))
        
        
    @QtCore.pyqtSlot()
    def update_progressbar(self, progressbar, value):
        progressbar.setValue(value)


    @QtCore.pyqtSlot()
    def run_predict_info(self, region_info, end_date, end_section, q, step, ratio, flag, cache_path, progressbar):
        result = calculate_parameter.predict_info(region_info, end_date, end_section, q, step, ratio, flag, cache_path)
        progressbar.setValue(100)
        return result
    
    
    @QtCore.pyqtSlot()
    def upload_param(self):
        fail_counter = 0
        config_file_lst = os.listdir(os.path.join(const.ROOT_PATH, "sftp_configs"))
        for config in config_file_lst:
            try:
                flink.pull_from_UI_to_cloud(config)
            except Exception as e:
                fail_counter += 1
                print("文件上传至云端失败:", config)
                QMessageBox.information(self, "上传失败", "参数表上传至云端失败:" + config)
                error_info = traceback.format_exc()
                with open("error_log.txt", "a+", encoding='utf-8') as err_file:
                    err_file.write(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S') + '\n')
                    err_file.write(error_info + '\n')
            try:
                flink.pull_from_UI_to_market(config)
            except Exception as e:
                fail_counter += 1
                print("文件上传至行情端失败:", config)
                QMessageBox.information(self, "上传失败", "参数表上传至行情端失败:" + config)
                error_info = traceback.format_exc()
                with open("error_log.txt", "a+", encoding='utf-8') as err_file:
                    err_file.write(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S') + '\n')
                    err_file.write(error_info + '\n')
        if fail_counter == 0:
            QMessageBox.information(self, "上传完成", "参数表已上传至云端&行情端")


    @QtCore.pyqtSlot()
    def download_holdings(self):
        conn = flink.sftp_conn("bridge")
        acc_lst = flink.get_acc_lst()
        for account in acc_lst:
            flink.sftp_transfer(conn, account, "planned_times")


    @QtCore.pyqtSlot()
    def start_prediction(self, region_info, end_date, end_section, q, step, ratio, flag, cache_path, progressbar):
        with concurrent.futures.ThreadPoolExecutor() as executor:
            future = executor.submit(self.run_predict_info, region_info, end_date, end_section, q, step, ratio, flag, cache_path, progressbar)
            while not future.done():
                # update progress bar in main thread
                self.update_progressbar(progressbar, int(future.running() / len(region_info) * 100))
                QtWidgets.QApplication.processEvents()  # 允许 GUI 线程响应事件
        result = future.result()
        return result
    
    
    @QtCore.pyqtSlot()
    def get_param(self, dialog):
        end_date = int(dialog.end_date.text())
        section = int(dialog.section.text())
        q = float(dialog.q.text())
        step = int(dialog.step.text())
        ratio = float(dialog.ticklock_ratio.text())
        # 总是读取最新的region_info.xlsx
        region_df = pd.read_excel('./info/region_info.xlsx', sheet_name="Sheet1")
        pairs = region_df['pairs_id']
        end_date, section = date_section_modification.from_predict(end_date, section)
        self.status.showMessage("开始预测")
        for i in range(self.table.rowCount(), 0, -1):
            if self.table.cellWidget(i-1, 0).findChild(type(QCheckBox())).isChecked():
                acc_info = [self.table.item(i-1,j).text() for j in range(1, self.table.columnCount())]
                acc_name = acc_info[0]
                region_coef = float(acc_info[4])
                boundary_coef = float(acc_info[5])
                cffex_flag = bool(int(acc_info[6]))
                back_flag = bool(int(acc_info[7]))
                boundary_3_flag = bool(int(acc_info[8]))
                boundary_4_flag = bool(int(acc_info[9]))
                # 查看是否已经缓存
                flag, filename = cache_management.check_cache(acc_name, end_date, section, pairs, (q, step, ratio))
                ####### 加个进度条
                progressbar = QtWidgets.QProgressBar(self)
                progressbar.setGeometry(10, 10, 200, 20)
                progressbar.setMinimum(0)
                progressbar.setMaximum(100)
                progressbar.show()
                info_df = self.start_prediction(region_df, end_date, section, q, step, ratio, flag, filename, progressbar)
                progressbar.close()
                info_df = transform(info_df)
                # 根据各账户系数/flag修改值
                info_df['boundary_unit_num'] = info_df['boundary_unit_num'].apply(lambda x: round(int(x) * boundary_coef + 0.01))
                info_df['region_unit_num'] = info_df['region_unit_num'].apply(lambda x: round(int(x) * region_coef + 0.01))
                cache_management.cache_param(acc_name, info_df, end_date, section, (q, step, ratio))
                # is_cffex, is_back
                if not boundary_3_flag:
                    info_df['up_boundary_3'] = "99999"
                    info_df['down_boundary_3'] = "-99999"
                if not boundary_4_flag:
                    info_df['up_boundary_4'] = "99999"
                    info_df['down_boundary_4'] = "-99999"
                if not back_flag:
                    info_df[['up_boundary_4', 'up_boundary_3', 'up_boundary_2', 'up_boundary_1']] = "99999"
                if not cffex_flag:
                    info_df = info_df[(info_df['kind']!="IF") & (info_df['kind']!="IC") & (info_df['kind']!="IM") & (info_df['kind']!="IH")]
                self.param.fill_para_table(info_df)
                print("账号：", acc_name)
                cache_management.save_param(acc_name, info_df)
                
        self.status.showMessage("区界信息已生成")
    

        
    @QtCore.pyqtSlot()
    def visualization(self, dialog):
        date = int(dialog.date.text())
        back_period = int(dialog.back_period.text())
        self.status.showMessage("开始生成图像")
        self.status.showMessage("绘制成交量切分套利图...")
        visualize.plot_volume_split()
        self.status.showMessage("成交量切分套利图生成完成（barplot目录）")
        self.status.showMessage("绘制品种连续合约套利图...")
        visualize.plot_continuous_contract()
        self.status.showMessage("品种连续合约套利图生成完成（barplot目录）")
        self.status.showMessage("绘制固定套利对时序分析图...")
        visualize.plot_time_series(date=date, back_period=back_period)
        self.status.showMessage("固定套利对时序分析图生成完成（barplot目录）")


    @QtCore.pyqtSlot()
    def export_param(self):
        dialog = addBoundDialog()
        dialog.accepted.connect(lambda: self.get_param(dialog))
        dialog.exec()


    @QtCore.pyqtSlot()
    def visualize(self):
        dialog = visualDialog()
        dialog.accepted.connect(lambda: self.visualization(dialog))
        dialog.exec()
    
    
    @QtCore.pyqtSlot()
    def update_base_info(self):
        self.param.update()
        QMessageBox.information(self, "刷新完成", "BASE参数表更新完成")


    @QtCore.pyqtSlot()
    # 使UI主界面参数表与存储的参数表同步
    def refresh(self, flag):
        if flag:
            self.param.update()
            self.param_layout = QtWidgets.QHBoxLayout()
            self.layout.addLayout(self.param_layout, 5, 0)
            self.param_layout.addWidget(self.param.view)
            QMessageBox.information(self, "刷新完成", "BASE参数表更新完成")


    @QtCore.pyqtSlot()
    def add(self):
        dialog = addAccDialog()
        dialog.accepted.connect(lambda: self.add_dialog(dialog))
        dialog.exec()
        
        
    @QtCore.pyqtSlot()
    def delete_user(self):
        for i in range(self.table.rowCount(), 0, -1):
            if self.table.cellWidget(i-1, 0).findChild(type(QCheckBox())).isChecked():
                temp = [self.table.item(i-1,j).text() for j in range(1, self.table.columnCount())]
                self.table.removeRow(i-1)
                if temp not in self.edit_buffer:
                    self.edit_buffer.append(temp)
        self.update_inbuffer()
        self.status.showMessage("已删除勾选套利对")
        
        
    @QtCore.pyqtSlot()
    def delete_param(self):
        param_df = self.param.model._data
        delete_line = param_df[param_df['CheckBox'] == True]
        delete_line = delete_line.loc[:, ~param_df.columns.isin(['CheckBox', 'BarPlot'])]
        print(delete_line)
        self.param_edit_buffer = pd.concat([self.param_edit_buffer, delete_line])
        keep_line = param_df[param_df['CheckBox'] == False]
        keep_line = keep_line.loc[:, ~param_df.columns.isin(['CheckBox', 'BarPlot'])]
        keep_line.to_csv(os.path.join(const.PARAM_PATH, 'BASE', 'params.csv'))
        self.refresh(True)
    
    
    @QtCore.pyqtSlot()
    def undo_delete_param(self):
        param_df = self.param.model._data
        param_df = param_df.loc[:, ~param_df.columns.isin(['CheckBox', 'BarPlot'])]
        param_df = pd.concat([param_df, self.param_edit_buffer])
        param_df.to_csv(os.path.join(const.PARAM_PATH, 'BASE', 'params.csv'))
        self.refresh(True)
            

    @QtCore.pyqtSlot()
    def move_row_down(self):
        for i in range(self.table.rowCount()-1, 0, -1):
            if self.table.cellWidget(i-1, 0).findChild(type(QCheckBox())).isChecked():
                current_item = [self.table.item(i, column).clone() for column in range(1, self.table.columnCount())]
                for column in range(1, self.table.columnCount()):
                    self.table.setItem(i, column, self.table.item(i-1, column).clone())
                    self.table.setItem(i-1, column, current_item[column-1])
                self.table.cellWidget(i-1, 0).findChild(type(QCheckBox())).setChecked(False)
                self.table.cellWidget(i, 0).findChild(type(QCheckBox())).setChecked(True)
        

    @QtCore.pyqtSlot()
    def move_row_up(self):
        for i in range(self.table.rowCount()-1, 0, -1):
            if self.table.cellWidget(i, 0).findChild(type(QCheckBox())).isChecked():
                current_item = [self.table.item(i, column).clone() for column in range(1, self.table.columnCount())]
                for column in range(1, self.table.columnCount()):
                    self.table.setItem(i, column, self.table.item(i-1, column).clone())
                    self.table.setItem(i-1, column, current_item[column-1])
                self.table.cellWidget(i, 0).findChild(type(QCheckBox())).setChecked(False)
                self.table.cellWidget(i-1, 0).findChild(type(QCheckBox())).setChecked(True)

    @QtCore.pyqtSlot()
    def undo(self):
        for undo_item in self.edit_buffer:
            self.addLine(undo_item)
        self.edit_buffer = []
        self.status.showMessage("已恢复被删除的套利对")

    @QtCore.pyqtSlot()
    def update_check_all_state(self):
        all_checked = True
        # 更新全选复选框的状态以反映所有行的复选框的状态
        for row in range(self.table.rowCount()):
            item = self.table.cellWidget(row, 0).findChild(type(QCheckBox()))
            if item is not None:
                if item.isChecked() == False:
                    all_checked = False
                    break
        if all_checked:
            # 全关
            for row in range(self.table.rowCount()):
                item = self.table.cellWidget(row, 0).findChild(type(QCheckBox()))
                if item is not None:
                    item.setChecked(False)
        else:
            for row in range(self.table.rowCount()):
                item = self.table.cellWidget(row, 0).findChild(type(QCheckBox()))
                if item is not None:
                    item.setChecked(True)

    @QtCore.pyqtSlot()        
    def set_editable(self):
        if self.editable == True:
            self.table.setEditTriggers(QAbstractItemView.NoEditTriggers)
            self.status.showMessage("禁用编辑功能,不再修改账户表")
            self.editable = False
        else:
            self.table.setEditTriggers(QAbstractItemView.AllEditTriggers)
            self.status.showMessage("开启编辑功能,可修改账户表")
            self.editable = True


    @QtCore.pyqtSlot()
    def save(self):
        self.update_inbuffer()
        df = pd.DataFrame(self.in_buffer, columns=self.table.columns[1:])
        df.to_excel(os.path.join(const.INFO_PATH, "account_info.xlsx"), index=False, sheet_name='Sheet1')
        param_df = self.param.model._data
        param_df = param_df.loc[:, ~param_df.columns.isin(['CheckBox', 'BarPlot'])]
        region_info, boundary_info, suffix_info = transform.param_split(param_df)
        param_df.to_csv(os.path.join(const.PARAM_PATH, 'BASE', 'params.csv'))
        # 再从基表分发给各个账户表
        for acc in self.in_buffer:
            id = acc[0]
            region_budget = float(acc[4])
            boundary_budget = float(acc[5])
            acc_param_df = param_df.copy()
            acc_param_df['region_unit_num'] = acc_param_df.apply(lambda x: int(round(x['region_unit_num'] * region_budget,0)), axis=1)
            acc_param_df['boundary_unit_num'] = acc_param_df.apply(lambda x: int(round(x['boundary_unit_num'] * boundary_budget,0)), axis=1)
            acc_param_dir = os.path.join(const.PARAM_PATH, id)
            if not os.path.exists(acc_param_dir):
                os.mkdir(acc_param_dir)
            acc_param_df.to_csv(os.path.join(const.PARAM_PATH, id, 'params.csv'))
            print("已分发参数表：", id)
        self.status.showMessage("账户参数表保存成功")


    @QtCore.pyqtSlot()
    def audit(self):
        acc_lst = []
        for i in range(self.table.rowCount(), 0, -1):
            if self.table.cellWidget(i-1, 0).findChild(type(QCheckBox())).isChecked():
                acc_info = [self.table.item(i-1,j).text() for j in range(1, self.table.columnCount())]
                acc_name = acc_info[0]
                acc_lst.append(acc_name)
        try:
            res = compare.export_holdings_compare(acc_lst)
            res_trade_export = res.copy()
            dialog = QDialog()
            model = pandasModel(res, checkbox_flag=False)
            view = TableView(model)

            layout = QVBoxLayout()
            layout.addWidget(view)
            dialog.setLayout(layout)
            dialog.setWindowTitle("持仓对比")
            dialog.exec_()
            # 导出对比文件
            res_trade_export.to_csv(os.path.join(ROOT_PATH, "holding_compare", str(datetime.date.today()) + '_holding_compare.csv'))
            QMessageBox.information(self, "导出持仓对比完成", "持仓对比结果存储在hold_compare目录下")
        except FileNotFoundError as e1:
            print(e1)
            QMessageBox.information(self, "读取持仓失败--" + acc_name, "TmpValue文件未生成在对应账户目录")
        except PermissionError as e2:
            print(e2)
            QMessageBox.information(self, "导出持仓对比失败", "持仓对比结果文件已打开，阻碍写入")
        
    @QtCore.pyqtSlot()
    def export_trading_compare_table(self):
        acc_lst = []
        for i in range(self.table.rowCount(), 0, -1):
            if self.table.cellWidget(i-1, 0).findChild(type(QCheckBox())).isChecked():
                acc_info = [self.table.item(i-1,j).text() for j in range(1, self.table.columnCount())]
                acc_name = acc_info[0]
                acc_lst.append(acc_name)
        try:
            res = compare.export_trading_compare(acc_lst)
            res_hold_export = res.copy()
            dialog = QDialog()
            model = pandasModel(res, checkbox_flag=False)
            view = TableView(model)
            # 导出对比文件
            res_hold_export.to_csv(os.path.join(ROOT_PATH, "trading_compare", str(datetime.date.today()) + '_day_compare.csv'), encoding='GBK')

            layout = QVBoxLayout()
            layout.addWidget(view)
            dialog.setLayout(layout)
            dialog.setWindowTitle("日成交对比")
            dialog.exec_()
            QMessageBox.information(self, "导出持仓对比完成", "持仓对比结果存储在day_compare目录下")
            
        except FileNotFoundError as e1:
            print(e1)
            QMessageBox.information(self, "读取日成交失败--" + acc_name, "TmpValue文件未生成在对应账户目录")
        except PermissionError as e2:
            print(e2)
            QMessageBox.information(self, "导出日成交对比失败", "持仓对比结果文件已打开，阻碍写入")

    # 导出TmpValue
    @QtCore.pyqtSlot()
    def fixTmpValue(self):
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
        counter = 0
        for i in range(self.table.rowCount(), 0, -1):
            if self.table.cellWidget(i-1, 0).findChild(type(QCheckBox())).isChecked():
                counter += 1
        progress.setMaximum(counter)
        pb_counter = 0
        for i in range(self.table.rowCount(), 0, -1):
            if self.table.cellWidget(i-1, 0).findChild(type(QCheckBox())).isChecked():
                counter += 1
                progress.setValue(pb_counter)
                QtWidgets.QApplication.processEvents()
                pb_counter += 1
                acc_info = [self.table.item(i-1,j).text() for j in range(1, self.table.columnCount())]
                acc_name = acc_info[0]
                try:
                    gap = report_to_tmpvalue.to_tmpvalue(acc_name).reset_index()
                    if len(gap) > 0:
                        gap_info = "{异常}\n以下持仓未进入TmpValue："
                        for k in range(len(gap)):
                            gap_info += '\n' + str(gap.iloc[k]['code']) + "  ||  " + str(gap.iloc[k]['gap'])
                        print(gap_info)
                        QMessageBox.information(self, "导出完成--" + acc_name, gap_info)
                except FileNotFoundError as e1:
                    print(e1)
                    QMessageBox.information(self, "导出失败--" + acc_name, "TmpValue导出失败,report, params文件未放置在对应账户目录")
                except PermissionError as e2:
                    print(e2)
                    QMessageBox.information(self, "导出失败--" + acc_name, "TmpValue导出失败,生成文件已打开，阻碍写入")
        QMessageBox.information(self, "导出完成", "TmpValue全部导出完成(TmpValue目录)")
        

    # 导出成交记录Sorted
    @QtCore.pyqtSlot()
    def fixTradeRecord(self):
        try:
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
            progress.setWindowTitle('解析成交记录中...')
            progress.show()
            counter = 0
            for i in range(self.table.rowCount(), 0, -1):
                if self.table.cellWidget(i-1, 0).findChild(type(QCheckBox())).isChecked():
                    counter += 1
            progress.setMaximum(counter)
            pb_counter = 0
            for i in range(self.table.rowCount(), 0, -1):
                if self.table.cellWidget(i-1, 0).findChild(type(QCheckBox())).isChecked():
                    progress.setValue(pb_counter)
                    QtWidgets.QApplication.processEvents()
                    pb_counter += 1
                    acc_info = [self.table.item(i-1,j).text() for j in range(1, self.table.columnCount())]
                    acc_name = acc_info[0]
                    trade_to_trading.fix_trade_record(acc_name)
            QMessageBox.information(self, "导出完成", "成交记录导出完成(tradings目录)")
        except FileNotFoundError as e1:
            print(e1)
            QMessageBox.information(self, "导出失败--" + acc_name, "成交记录导出失败,文件未放置在tradings目录对应账户下")
        except PermissionError as e2:
            print(e2)
            QMessageBox.information(self, "导出失败--" + acc_name, "成交记录导出失败,生成文件已打开，阻碍写入")

    @QtCore.pyqtSlot()
    def showDict(self):
        dict_dialog = estHoldingDialog()
        dict_dialog.accepted.connect(lambda: self.estimate_holding(dict_dialog))
        dict_dialog.exec()

if __name__ == '__main__':
    try:
        app = QApplication(sys.argv)
        ex = Arbitrator()
        sys.exit(app.exec_())
        
    except Exception as e:
        error_info = traceback.format_exc()
        with open("error_log.txt", "a+", encoding='utf-8') as err_file:
            err_file.write(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S') + '\n')
            err_file.write(error_info + '\n')
#展示已建立的套利对
#添加新套利对
#删除旧套利对