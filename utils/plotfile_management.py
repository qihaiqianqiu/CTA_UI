import os
from utils.const import PLOT_PATH

all = ["pairname_to_plotdir"]

def pairname_to_plotdir(contract_pair_name) -> str:
    """
    将套利对名称转换为图像存储路径
    :param contract_pair_name: 套利对名称
    :return: 图像存储路径
    """
    updated_date = max(os.listdir(PLOT_PATH))
    plot_name = updated_date + '-' + contract_pair_name + ".png"
    plot_dir = os.path.join(PLOT_PATH, updated_date, plot_name)
    
    return plot_dir