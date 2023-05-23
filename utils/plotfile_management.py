import os
from utils.const import PLOT_PATH

all = ["pairname_to_plotdir"]

def pairname_to_plotdir(contract_pair_name) -> str:
    """
    将套利对名称转换为图像存储路径
    :param contract_pair_name: 套利对名称
    :return: 图像存储路径
    """
    date_lst = os.listdir(PLOT_PATH)
    date_lst.sort(reverse=True)
    for date in date_lst[:3]:
        plotname = date + "-" + contract_pair_name + ".png"
        plot_dir = os.path.join(PLOT_PATH, date, plotname)
        if os.path.exists(plot_dir):
            return plot_dir
    return "无效套利对"

if __name__ == "__main__":
    print(pairname_to_plotdir("AU2402-2404"))