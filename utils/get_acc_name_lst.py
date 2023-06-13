import pandas as pd

df = pd.read_excel(r'../info/account_info.xlsx', sheet_name="Sheet1")
acc_lst = df['id'].values.tolist()
print(acc_lst)