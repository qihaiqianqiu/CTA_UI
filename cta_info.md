## Z:\300_Group\HFT\Program\CDT\cta_lq_ch8\params.csv

| feature        | 描述                      |  备注                   |
| -------------- | ------------------------- |  ---------------------- |
| pairs_id | 套利对名称 | |
| first_instrument | 近期合约 | |
| second_instrument | 远期合约 | |
| prime_instrument | 主合约 | 交易量更高的合约，被动腿，first/second中的一个，另一个则为主动腿 |
| unit_num | 持仓单位 | 持仓量变动的单位数量 |
| region_drift | 区漂移项 |  |
| region_0 | 8区数据 |  |
| region_1 | | |
| region_2 | | |
| region_3 | | |
| region_4 | | |
| region_5 | | |
| region_6 | | |
| region_7 | | |
| up_boundary_5 | 上5界 |  |
| up_boundary_4 | | |
| up_boundary_3 | | |
| up_boundary_2 | | |
| up_boundary_1 | | |
| down_boundary_1 | 下1界 |  |
| down_boundary_2 | 下2界 |  |
| down_boundary_3 | 下3界 |  |
| down_boundary_4 | 下4界 |  |
| down_boundary_5 | 下5界 |  |
| today_fee | 是否平今 | 0-只平不开（适用于平今多收手续费），1-只开不平（适用于平今少收手续费），2-优先平仓 |
| wait_window | 主动腿撤单等待时间 | 超过对应毫秒自动撤单 |
| favor_times | 撤单的有利Tick数 | |
| unfavor_times | 撤单的不利Tick数 | |
| abs_threshold | 主动腿abs阈值 | 超过阈值后不挂单 |
| tick_lock | tick锁 | 每一个区界的盈利期望 |
| wait_2_windows | 被动腿撤单等待时间 | 单位毫秒 |
| after_tick | 被动腿加价tick数 | |
| night_type | 夜盘对应时间段类型 | 1-常规时间 2-凌晨2：30收 3-凌晨1：00收 |
| if_add | 是否加仓 | |
| limit_coef | 被动腿交易量限制 | 下主动腿时检验被动腿盘口挂单量，看是否打穿 |
| abs_threshold_after | 被动腿abs阈值 | abs = ask_1_price - bid_1_price，超过阈值后不挂单 |
| kind | 期货种类 | |
| before_tick | 主动腿加价tick数 | |
| before_cancel_flag | 含义暂不知 | |
| before_cancel_num | 含义暂不知 | |
| max_position | 最大持仓量 | 超过最大持仓量后停止加仓，风控参数 |

## Z:\300_Group\HFT\Program\CDT\cta_lq_ch8\TmpValue.csv

| 列       | 描述         | 备注 |
| -------- | ------------ | ---- |
| Columns1 | 期货对名称   |      |
| Columns2 | 持仓手数     |      |
| Columns3 | 冗余字段     |      |
| Columns4 | 冗余字段     |      |
| Columns5 | 最新更新时间 |      |

## Z:\300_Group\HFT\Program\CDT\cta_lq_ch8\configure.xml

| 字段                    | 描述                   | 默认值 |
| ----------------------- | ---------------------- | ------ |
| MaxOpenPosition         | 最大开仓量             | 500000 |
| MaxClosePosition        | 最大平仓量             | 500000 |
| MaxStepOpenPosition     | 最大单次开仓量         | 6      |
| MaxStepClosePosition    | 最大单次平仓量         | 6      |
| HandleThreadNum         | 处理线程数             | 1      |
| BrokerID                | 经纪商代码             | 9000   |
| InvesterID              | 投资者账户名           | -      |
| InvesterPassword        | 投资者密码             | -      |
| AuthCode                | 认证码                 | -      |
| AppID                   | 唯一标识码             | -      |
| MdFrontAddr             | 行情前置地址           | -      |
| TradeFronAddr           | 交易前置地址           | -      |
| PauseTradeSec           | 休市N秒前中止交易      | 120    |
| MaxOverNum              | 单品种额外下单笔数     | 40     |
| MaxRiskRate             | 最大风险率10000倍      | 3800   |
| MaxAfterOrderNum        | 最大撤补次数           | 40     |
| LastKindLongPosition    | 品种多头最后N手        | 4      |
| LastKindLongTradeVolume | 品种多头成交最后N手    | 3      |
| ParamsResetMilliSeconds | Params文件重加载间隔ms | 5000   |

