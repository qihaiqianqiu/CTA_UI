"""
检查参数表
"""
from PyQt5.QtWidgets import  QDialog, QHBoxLayout
import pandas as pd
import os
from utils.get_contract_pair import check_vaild_month
from utils.const import PARAM_PATH
from .pandasModel import pandasModel, TableView

all = ["checkParamDialog"]
class checkParaDialog(QDialog):
    def __init__(self):
        super().__init__()
        
        lost_pair = pd.DataFrame(columns=['pairs_id', 'warehouse_recipt', 'volume'])
        candidate_pairs = check_vaild_month()
        param_df = pd.read_csv(os.path.join(PARAM_PATH, 'BASE', "params.csv"))
        for contract_pair in candidate_pairs['contract_pair'].tolist():
            if contract_pair not in param_df['pairs_id'].tolist():
                vol = candidate_pairs[candidate_pairs['contract_pair']==contract_pair]['volume'].tolist()[0]
                print("检测到未包含合约：", contract_pair)
                flag = candidate_pairs[candidate_pairs['contract_pair']==contract_pair]['flag'].tolist()[0]
                lost_pair = lost_pair.append({'pairs_id': contract_pair, 'warehouse_recipt': flag, 'volume': vol}, ignore_index=True)

                lost_pair = lost_pair.reset_index(drop=True).set_index('pairs_id')
                lost_pair = lost_pair.sort_values(by=['warehouse_recipt', 'volume'], ascending=False)
                
        model = pandasModel(lost_pair)
        view = TableView(model)

        layout = QHBoxLayout()
        layout.addWidget(view)
        self.setLayout(layout)
        self.setWindowTitle("可添加套利对")
        self.exec_()
        
