from clickhouse_driver import Client
import json
from utils.const import ROOT_PATH
import os

all = ['init_clickhouse_client', 'db_para']
# 数据库配置
DB_CONFIG_PATH = os.path.join(ROOT_PATH, "DB_config.json")
db_para = json.load(open(DB_CONFIG_PATH))

def init_clickhouse_client():
    # Clickhouse config
    client = Client(host=db_para['ip'], port=db_para['port'])
    client.execute('use ' + db_para['db_to'])
    return client