import pandas as pd
import os
from utils.const import ROOT_PATH
df = pd.read_excel(os.path.join(ROOT_PATH, "info", "account_info.xlsx"), sheet_name="Sheet1")
acc_lst = df['id'].values.tolist()
print(acc_lst)