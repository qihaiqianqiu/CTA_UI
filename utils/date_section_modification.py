import datetime
import time
from . import const
# 日期/交易单元 轮换模块
"""
获取当前日期 + 交易单元
交易单元取自然时间，即夜盘为0
"""
__all__ = ["get_date_section", "from_predict", "to_predict", "to_trading_day_backwards"]

def to_trading_day_backwards(date:int):
    date = datetime.datetime.strptime(str(date), "%Y%m%d")
    while int(date.strftime('%Y%m%d')) not in const.trade_day:
        date += datetime.timedelta(days=1)
    return int(date.strftime('%Y%m%d'))


# Get current date + section in a predict format
def get_date_section():
    today = int(datetime.date.today().strftime('%Y%m%d'))
    print(today)
    today = to_trading_day_backwards(today)
    # Choose trading section to upload
    HMtime = time.strftime("%H:%M", time.localtime())
    print("Current time:", HMtime)
    # Morning Market
    if (HMtime > '02:30') & (HMtime < '11:30'):
        section = 1
    # Afternoon Market
    elif (HMtime > '11:30') & (HMtime < '15:00'):
        section = 2
    # Evening Market
    else:
        today = const.trade_day[const.trade_day.index(today) + 1]
        section = 0
    return today, section


"""
前置一个交易单元
场景：获取当前所处的交易单元后，前推一个交易单元可以得到最新的以入库日期+单元
用以最新的已入库时间节点进一步进行模型预测
"""
def from_predict(predict_date:int, predict_section:int):
    if predict_section == 1 or predict_section == 2:
        section = predict_section - 1
        date = predict_date
    else:
        section = 2
        date = const.trade_day[const.trade_day.index(predict_date) - 1]
    return date, section

"""
后置一个交易单元
场景：检查当前交易单元参数表是否已缓存的时候，把前置过得日期+单元数据后推回来，检查文件名是否存在
"""
def to_predict(date:int, section:int):
    if section == 0 or section == 1:
        predict_section = section + 1
        predict_date = date
    if section == 2:
        predict_section = 0
        predict_date = const.trade_day[const.trade_day.index(date) + 1]
    return predict_date, predict_section