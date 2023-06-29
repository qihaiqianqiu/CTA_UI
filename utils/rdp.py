import os
from utils.const import ROOT_PATH
from utils.path_exp_switch import linux_to_windows

def rdp_transmission_demo():
    local_file_path = os.path.join(ROOT_PATH, "rdp_test_local", "params.csv")  # 本地文件路径
    remote_file_path = os.path.join(r"C:\Users\Administrator\Desktop", "rdp")  # 远程文件路径
    rdp_clip_exe = r"C:/Windows/System32/rdpclip.exe"  # rdpclip.exe路径

    # 启动rdpclip.exe进程，这将启动系统剪贴板功能
    os.startfile(rdp_clip_exe)

    # 等待rdpclip.exe进程建立并开启系统剪贴板服务
    import time
    time.sleep(1)

    # 复制本地文件到系统剪贴板【例如params.csv中的内容】
    import win32clipboard
    with open(local_file_path, 'rb') as f:
        file_data = f.read()
    win32clipboard.OpenClipboard()
    win32clipboard.EmptyClipboard()
    win32clipboard.SetClipboardData(win32clipboard.CF_UNICODETEXT, file_data)
    win32clipboard.CloseClipboard()

    # 在远程计算机中创建文件夹（如果不存在）
    import subprocess
    remote_file_path = linux_to_windows(remote_file_path)
    #subprocess.run(f"cmd /c mkdir {remote_file_path}", shell=True)

    # 在远程计算机上粘贴文件
    subprocess.run(f"cmd /c echo.|set /p=\"{remote_file_path}\"|clip", shell=True)
    subprocess.run(f"cmd /c start {rdp_clip_exe} \\tsclient", shell=True)
    time.sleep(1)
    print("3", win32clipboard.GetClipboardData(win32clipboard.CF_UNICODETEXT))
    print("4", win32clipboard.GetClipboardData(win32clipboard.CF_UNICODETEXT))



if __name__ == "__main__":
    rdp_transmission_demo()