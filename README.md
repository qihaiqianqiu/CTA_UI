# CTA_UI 使用说明
- 更新日志
- 20230530：
- （已实现）修改主界面参数表展示
- （已实现）完善基表子表使用逻辑：Boundary Info目录下留界缓存，Param目录下每次更新/计算参数表之后留下带交易单元时间信息后缀的缓存。但是直接使用的是账号目录下的params.csv 
- （已实现）每当用户修改参数表并保存时，应该同步更新info目录下的region_info.xlsx，保证其实时为最新
- （已实现）修改Ctrl+S保存功能，使其同样保存参数表
- 实现修改参数表数据的功能-增删套利对-增：检查参数表最好重新做一个class
- 完善一些进度条
- 参数表适配SP指令（新增SP模块）-get_sp_instruction
- 根据账户系数(budget)修改参数表的功能没有实装（参数计算模块需要整体做相应的适配）
- （问题不大）UI目前使用起来有一些卡顿
- （已实现）pandasModel的列宽有些不够，应该根据内容的长短适应性改变尺寸