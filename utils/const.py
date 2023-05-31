from clickhouse_driver import Client
import json
import os

all = ["ROOT_PATH", "BOUNDARY_PATH", "PLOT_PATH", "client", "ssh", "trade_day", "breed_dict", 
       "param_columns", "boundary_dict", "exchange_breed_dict", "secury_deposit_d1_dict", "default_args"]
# 路径
# Where parameter stored 
# ROOT_PATH = r"Z:\300_Group\HFT\Program\CTA_UI"
ROOT_PATH = r"D:\local_repo\CTA_UI"
PARAM_PATH = os.path.join(ROOT_PATH, "params")
INFO_PATH = os.path.join(ROOT_PATH, "info")
BOUNDARY_PATH = os.path.join(INFO_PATH, "boundary_info")
PLOT_PATH = os.path.join(ROOT_PATH, "barplot")
DB_CONFIG_PATH = os.path.join(ROOT_PATH, "DB_config.json")
# 数据库配置
db_para = json.load(open(DB_CONFIG_PATH))

# Clickhouse config
client = Client(host=db_para['ip'], port=db_para['port'])
client.execute('use ' + db_para['db_to'])

# 界计算模块参数
default_args = (0.95, 6, 1.0)

# 交易日列表
trade_day = [20220809, 20220810, 20220811, 20220812, 20220815, 20220816, 20220817, 20220818, 20220819, 20220822, 20220823, 20220824, 
             20220825, 20220826, 20220829, 20220830, 20220831, 20220901, 20220902, 20220905, 20220906, 20220907, 20220908, 20220909, 
             20220913, 20220914, 20220915, 20220916, 20220919, 20220920, 20220921, 20220922, 20220923, 20220926, 20220927, 20220928,
             20220929, 20220930, 20221010, 20221011, 20221012, 20221013, 20221014, 20221017, 20221018, 20221019, 20221020, 20221021, 
             20221024, 20221025, 20221026, 20221027, 20221028, 20221031, 20221101, 20221102, 20221103, 20221104, 20221107, 20221108, 
             20221109, 20221110, 20221111, 20221114, 20221115, 20221116, 20221117, 20221118, 20221121, 20221122, 20221123, 20221124, 
             20221125, 20221128, 20221129, 20221130, 20221201, 20221202, 20221205, 20221206, 20221207, 20221208, 20221209, 20221212, 
             20221213, 20221214, 20221215, 20221216, 20221219, 20221220, 20221221, 20221222, 20221223, 20221226, 20221227, 20221228, 
             20221229, 20221230, 20230103, 20230104, 20230105, 20230106, 20230109, 20230110, 20230111, 20230112, 20230113, 20230116, 20230117, 20230118, 20230119, 20230120, 20230130, 20230131, 20230201, 20230202, 20230203, 20230206, 20230207, 20230208, 20230209, 20230210, 20230213, 20230214, 20230215, 20230216, 20230217, 20230220, 20230221, 20230222, 20230223, 20230224, 20230227, 20230228, 20230301, 20230302, 20230303, 20230306, 20230307, 20230308, 20230309, 20230310, 20230313, 20230314, 20230315, 20230316, 20230317, 20230320, 20230321, 20230322, 20230323, 20230324, 20230327, 20230328, 20230329, 20230330, 20230331, 20230403, 20230404, 20230406, 20230407, 20230410, 20230411, 20230412, 20230413, 20230414, 20230417, 20230418, 20230419, 20230420, 20230421, 20230424, 20230425, 20230426, 20230427, 20230428, 20230502, 20230503, 20230504, 20230505, 20230508, 20230509, 20230510, 20230511, 20230512, 20230515, 20230516, 20230517, 20230518, 20230519, 20230522, 20230523, 20230524, 20230525, 20230526, 20230529, 20230530, 20230531, 20230601, 20230602, 20230605, 20230606, 20230607, 20230608, 20230609, 20230612, 20230613, 20230614, 20230615, 20230616, 20230619, 20230620, 20230621, 20230623, 20230626, 20230627, 20230628, 20230629, 20230630, 20230703, 20230704, 20230705, 20230706, 20230707, 20230710, 20230711, 20230712, 20230713, 20230714, 20230717, 20230718, 20230719, 20230720, 20230721, 20230724, 20230725, 20230726, 20230727, 20230728, 20230731, 20230801, 20230802, 20230803, 20230804, 20230807, 20230808, 20230809, 20230810, 20230811, 20230814, 20230815, 20230816, 20230817, 20230818, 20230821, 20230822, 20230823, 20230824, 20230825, 20230828, 20230829, 20230830, 20230831, 20230901, 20230904, 20230905, 20230906, 20230907, 20230908, 20230911, 20230912, 20230913, 20230914, 20230915, 20230918, 20230919, 20230920, 20230921, 20230922, 20230925, 20230926, 20230927, 20230928, 20231006, 20231009, 20231010, 20231011, 20231012, 20231013, 20231016, 20231017, 20231018, 20231019, 20231020, 20231023, 20231024, 20231025, 20231026, 20231027, 20231030, 20231031, 20231101, 20231102, 20231103, 20231106, 20231107, 20231108, 20231109, 20231110, 20231113, 20231114, 20231115, 20231116, 20231117, 20231120, 20231121, 20231122, 20231123, 20231124, 20231127, 20231128, 20231129, 20231130, 20231201, 20231204, 20231205, 20231206, 20231207, 20231208, 20231211, 20231212, 20231213, 20231214, 20231215, 20231218, 20231219, 20231220, 20231221, 20231222, 20231225, 20231226, 20231227, 20231228, 20231229]

breed_dict = {'原油': 'sc', '燃料油': 'fu', '低硫燃料油': 'lu', '沥青': 'bu', '精对苯二甲酸': 'TA', '乙二醇':'eg', '短纤': 'PF',\
           '棉花': 'CF', '聚乙烯':'l', '聚氯乙烯':'v', '聚丙烯': 'pp', '橡胶': 'ru', '20号胶': 'nr', '液化石油气': 'pg', '甲醇': 'MA',\
           '苯乙烯': 'eb', '纯碱': 'SA', '玻璃': 'FG', '纸浆': 'sp', '尿素': 'UR', '豆一': 'a', '玉米': 'c', '玉米淀粉': 'cs',\
           '豆粕': 'm', '菜粕': 'RM', '白糖': 'SR', '菜油': 'OI', '豆油': 'y', '棕榈油': 'p', '苹果': 'AP', '红枣': 'CJ',\
           '鸡蛋': 'jd', '硅铁': 'SF', '锰硅': 'SM', '螺纹': 'rb', '铁矿': 'i', '热卷': 'hc', '焦炭': 'j', '焦煤': 'jm',\
           '动力煤': 'ZC', '白银':'ag', '黄金': 'au', '铜': 'cu', '锌': 'zn', '铝': 'al', '铅': 'pb', '不锈钢': 'ss', '镍': 'ni',\
           '锡': 'sn', 'IF': 'IF', 'IH': 'IH', 'IC': 'IC', 'IM': 'IM', '棉纱': 'CY', "线材": 'WR', "国际铜": 'bc', "豆二" : "b", "胶合板" : "bb", \
            "纤维板" : 'FB', "生猪": "LH", "粳米" : "RR", "粳稻" : 'JR', "晚籼" : 'LR', "油菜籽" : 'RS', "强麦" : "WH"}

exchange_breed_dict = {
    "XSGE": ["CU", "AL", "FU", "ZN", "RB", "RU", "PB", "AG", "AU", "BU", "HC", "NI", "SN", "SP", "SS"],
    "XDCE": ["M", "Y", "C", "L", "P", "V", "J", "JM", "JD", "A", "B", "I", "FB", "PP", "CS", "EG", "RR", "EB", "PG", "LH"],
    "XZCE": ["PM", "MA", "CF", "WH", "TA", "OI", "RI", "SR", "FG", "RS", "RM", "ZC", "JR", "LR", "SF", "SM", "CY", "AP", "CJ", "UR", "SA", "PF", "PK"],
    "CCFX": ["IF", "IH", "IC", "IM"],
    "XINE": ["NR", "LU", "BC", "SC"]
}

param_columns = ['pairs_id','indate_date', 'first_instrument', 'second_instrument', 'prime_instrument', 'boundary_unit_num', \
        'region_drift', 'region_0', 'region_1', 'region_2', 'region_3', 'region_4', 'region_5', 'region_6', 'region_7', \
        'up_boundary_5', 'up_boundary_4', 'up_boundary_3', 'up_boundary_2', 'up_boundary_1', 'down_boundary_1', \
        'down_boundary_2', 'down_boundary_3', 'down_boundary_4', 'down_boundary_5', 'today_fee', 'wait_window', \
        'favor_times', 'unfavor_times', 'abs_threshold', 'boundary_tick_lock', 'wait2_windows', 'after_tick', 'night_type',\
        'if_add', 'limitcoef', 'abs_threshold_after', 'kind', 'before_tick', 'before_cancel_flag', 'before_cancel_num',\
        'max_position', 'min_position', 'region_tick_lock', 'region_unit_num']

multiple_dict = {
    "AG" : 15,
    "AL" : 5,
    "AU" : 1000,
    "BU" : 10,
    "CU" : 5,
    "FU" : 10,
    "HC" : 10,
    "NI" : 1,
    "PB" : 5,
    "RB" : 10,
    "RU" : 10,
    "SN" : 1,
    "SP" : 10,
    "SS" : 5,
    "WR" : 10,
    "ZN" : 5,
    "BC" : 5,
    "LU" : 10,
    "SC" : 1000,
    "NR" : 10,
    "A" : 10,
    "B" : 10,
    "BB" : 500,
    "C" : 10,
    "CS" : 10,
    "EB" : 5,
    "EG" : 10,
    "FB" : 10,
    "I" : 100,
    "J" : 100,
    "JD" : 10,
    "JM" : 60,
    "L" : 5,
    "LH" : 16,
    "M" : 10,
    "P" : 10,
    "PG" : 20,
    "PP" : 5,
    "RR" : 10,
    "V" : 5,
    "Y" : 10,
    "AP" : 10,
    "CF" : 5,
    "CJ" : 5,
    "CY" : 5,
    "FG" : 20,
    "JR" : 20,
    "LR" : 20,
    "MA" : 10,
    "OI" : 10,
    "PF" : 5,
    "PK" : 5,
    "PM" : 50,
    "RI" : 20,
    "RM" : 10,
    "RS" : 10,
    "SA" : 20,
    "SF" : 5,
    "SM" : 5,
    "SR" : 10,
    "TA" : 5,
    "UR" : 20,
    "WH" : 20,
    "ZC" : 100,
    "IC" : 200,
    "IF" : 300,
    "IH" : 300,
    "IM" : 200
    }


secury_deposit_d1_dict = {
    "AG" : 0.12,
    "AL" : 0.12,
    "AU" : 0.10,
    "BU" : 0.15,
    "CU" : 0.12,
    "FU" : 0.15,
    "HC" : 0.13,
    "NI" : 0.19,
    "PB" : 0.14,
    "RB" : 0.13,
    "RU" : 0.10,
    "SN" : 0.14,
    "SP" : 0.15,
    "SS" : 0.14,
    "WR" : 0.13,
    "ZN" : 0.14,
    "BC" : 0.12,
    "LU" : 0.15,
    "SC" : 0.15,
    "NR" : 0.10,
    "A" : 0.12,
    "B" : 0.09,
    "BB" : 0.40,
    "C" : 0.12,
    "CS" : 0.09,
    "EB" : 0.12,
    "EG" : 0.12,
    "FB" : 0.10,
    "I" : 0.13,
    "J" : 0.20,
    "JD" : 0.09,
    "JM" : 0.20,
    "L" : 0.11,
    "LH" : 0.12,
    "M" : 0.10,
    "P" : 0.12,
    "PG" : 0.13,
    "PP" : 0.11,
    "RR" : 0.06,
    "V" : 0.11,
    "Y" : 0.09,
    "AP" : 0.10,
    "CF" : 0.07,
    "CJ" : 0.12,
    "CY" : 0.07,
    "FG" : 0.09,
    "JR" : 0.15,
    "LR" : 0.15,
    "MA" : 0.08,
    "OI" : 0.09,
    "PF" : 0.08,
    "PK" : 0.08,
    "PM" : 0.15,
    "RI" : 0.15,
    "RM" : 0.09,
    "RS" : 0.20,
    "SA" : 0.09,
    "SF" : 0.15,
    "SM" : 0.12,
    "SR" : 0.07,
    "TA" : 0.07,
    "UR" : 0.08,
    "WH" : 0.15,
    "ZC" : 0.50,
    "IC" : 0.14,
    "IF" : 0.12,
    "IH" : 0.12,
    "IM" : 0.15
    }

# 最小变动
boundary_dict = {'SC': 0.1,
                'FU': 1,
                'LU': 1,
                'BU': 1,
                'TA': 2,
                'EG': 1,
                'PF': 2,
                'CF': 5,
                'L': 1,
                'V': 1,
                'PP': 1,
                'RU': 5,
                'NR': 5,
                'PG': 1,
                'MA': 1,
                'EB': 1,
                'SA': 1,
                'FG': 1,
                'SP': 2,
                'UR': 1,
                'A': 1,
                'C': 1,
                'CS': 1,
                'M': 1,
                'RM': 1,
                'SR': 1,
                'OI': 1,
                'Y': 2,
                'P': 2,
                'AP': 1,
                'CJ': 5,
                'JD': 1,
                'SF': 2,
                'SM': 2,
                'RB': 1,
                'I': 0.5,
                'HC': 1,
                'J': 0.5,
                'JM': 0.5,
                'ZC': 0.2,
                'AG': 1,
                'AU': 0.02,
                'CU': 10,
                'ZN': 5,
                'AL': 5,
                'PB': 5,
                'SS': 5,
                'NI': 10,
                'SN': 10,
                'IF': 0.2,
                'IH': 0.2,
                'IC': 0.2,
                'IM' : 0.2,
                'CY': 5}
                

invalid_month_dict = {
    'FU':[12],
    'LU':[12],
    'BU':[10],
    'TA':[9],
    'EG':[3],
    'PF':[1,5,9],
    'CF':[11],
    'L':[3],
    'v':[3],
    'PP':[3],
    'RU':[11],
    'PG':[3,9],
    'MA':[5,11],
    'EB':[1,2,3,4,5,6,7,8,9,10,11,12],
    'SA':[1,3,5,7,9,11],
    'FG':[1,3,5,7,9,11],
    'SP':[12],
    'UR':[2,6,10],
    'A':[3],
    'B':[3],
    'CS':[3,7,11],
    'M':[3,7,11],
    'RM':[3,7,11],
    'SR':[11],
    'OI':[5],
    'Y':[3],
    'P':[1,2,3,4,5,6,7,8,9,10,11,12],
    'AP':[1,5,7],
    'CJ':[9],
    'JD':[1,2,3,4,5,6,7,8,9,10,11,12],
    'SF':[2,6,10],
    'SM':[10],
    'RB':[6],
    'I':[3,9],
    'J':[3],
    'JM':[1,2,3,4,5,6,7,8,9,10,11,12],
    'ZC':[5,11],
    'CY':[2,4,6,8,10,12],
    'RR':[1,2,3,4,5,6,7,8,9,10,11,12]
}