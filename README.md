# CTA_UI 使用说明
- 更新日志
- 20230530：
- （已实现）修改主界面参数表展示
- （已实现）完善基表子表使用逻辑：Boundary Info目录下留界缓存，Param目录下每次更新/计算参数表之后留下带交易单元时间信息后缀的缓存。但是直接使用的是账号目录下的params.csv 
- （已实现）每当用户修改参数表并保存时，应该同步更新info目录下的region_info.xlsx，保证其实时为最新
- （已实现）修改Ctrl+S保存功能，使其同样保存参数表
- （已实现）实现修改参数表数据的功能-增删套利对-增：检查参数表最好重新做一个class, 直接检查基表就行
1. (已实现)添加参数的时候窗口锁死，干脆直接把图片链接加进来不就完事了吗
2. (已实现)异常处理，空值
3. (已实现)添加删除套利对的快捷键，最好ctrl+z能撤回
- 工具箱
1. （已实现）检查参数表，用于增加套利对
2. 查看不可转抛、反套持仓
3. （已实现）Mute月份合约
4. 持仓(TmpValue & DB实时数据 -> 持仓盈亏 / 持仓构成) -- estHoldingDialog
- 完善一些进度条, 窗口大小设置
- 参数表适配SP指令（新增SP模块）- get_sp_instruction
- （已实现）根据账户系数(budget)修改参数表的功能没有实装（参数计算模块需要整体做相应的适配）
- （问题不大）UI目前使用起来有一些卡顿
- （已实现）pandasModel的列宽有些不够，应该根据内容的长短适应性改变尺寸

## 文件传输、监控链路
- 异常处理 任务日志加一下
1. 链路 SSH实现，一个链路对应一个终点RDP对应一个Config
2. UI中链路可视化 链路编辑 设置默认链路组合
3. 每次传输的时候走Default链路组合
4. 参数表修改日志，在云服务器运行计算
5. 使用rsync进行与中枢服务器保存的日志文件同步到本地：
`rsync_example = "rsync -avPz --port 8730 --password-file=/cygdrive/C/Users/Han.Hao/AppData/Local/cwrsync/bin/cta_password.txt root@39.97.106.35::cta/ /cygdrive/C/Users/Han.Hao/test"`
`rsync_pwd_path` & `rsync dest path`

- 方案1： 不做内网穿透，云服务器做中转中枢


- 方案2： 做内网穿透，云服务器做备份
- 方案3： 用云服务器做反向SSH的内网穿透【已通过paramiko实现】
  
1. 在云服务器上，确保 SSH 服务已启动，并且允许反向隧道连接。你可以编辑 `/etc/ssh/sshd_config` 文件，确保以下配置项未被注释或设置为正确的值：

   ```
   GatewayPorts yes
   AllowTcpForwarding yes
   ```
   之后记得重启服务器的SSHD: systemctl restart sshd

2. 在本地主机上，使用以下命令建立反向 SSH 隧道，将本地主机的某个端口（例如 8888）转发到云服务器上的 SSH 端口（默认为 22）：

   ```bash
   ssh -i <私钥文件路径> -R 8888:localhost:22 <云服务器用户名>@<云服务器IP地址>
   ```
   以某行情服务器为例，在行情服务器上运行 ssh -R 9876:localhost:22 root@39.97.106.35
   为了确保本地主机上的反向 SSH 隧道长时间存在并保持连接，使用 SSH 的 KeepAlive 机制：
   使用 SSH 的 TCPKeepAlive 选项：在云服务器上的 SSH 配置文件 `/etc/ssh/sshd_config` 中，你可以设置 `TCPKeepAlive yes`，以确保 SSH 服务器也发送 KeepAlive 消息。这样可以防止服务器端因为长时间没有活动而关闭连接。
   请注意，修改 SSH 服务器配置后，你需要重启 SSH 服务以使更改生效。
   在建立反向 SSH 隧道时，可以通过 `-o ServerAliveInterval=<秒数>` 参数设置 SSH 的 KeepAlive 间隔时间。例如：-o ServerAliveInterval=60

   ```bash
   ssh -i <私钥文件路径> -o ServerAliveInterval=60 -R 8888:localhost:22 <云服务器用户名>@<云服务器IP地址>
   ```
   UI端推送参数表的时候，一方面给云服务器推送。另一方面给行情服务器直接推送。
   推送时使用Paramiko建立SSH连接，等价命令: SSH -p 9876 adminstrator[行情服务器用户]@39.97.106.35[云服务器公网IP]

   请注意替换 `<私钥文件路径>`、`<云服务器用户名>` 和 `<云服务器IP地址>` 为你实际的值。

3. 在云服务器上，安装并配置 inotify 工具。例如，在 Ubuntu 上，你可以使用以下命令安装 inotify-tools：

   ```bash
   sudo apt-get install inotify-tools
   ```

4. 创建一个 Bash 脚本，用于监控云服务器上的文件变化并触发同步操作。以下是一个示例脚本：

   ```bash
   #!/bin/bash

   SOURCE_DIR="<云服务器文件目录>"
   DEST_DIR="<本地主机文件目录>"

   inotifywait -m -r -e modify,create,delete "$SOURCE_DIR" |
   while read path action file; do
       rsync --inplace --update -e "ssh -p 8888" "$SOURCE_DIR/$file" "$DEST_DIR/$file"
   done
   ```

   请注意替换 `<云服务器文件目录>` 和 `<本地主机文件目录>` 为你实际的值。

   上述脚本使用 inotifywait 监控指定的云服务器文件目录，当文件发生修改、创建或删除时，将触发 rsync 命令进行同步。rsync 命令使用反向 SSH 隧道的端口（8888）连接到本地主机。

5. 在云服务器上运行上述脚本：

   ```bash
   bash sync_script.sh
   ```

   脚本将开始监控云服务器上的文件变化，并在文件发生更改时触发同步操作。

