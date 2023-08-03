import shutil

all = ['copy_file']

def copy_file(source_path, destination_path):
    try:
        shutil.copy2(source_path, destination_path)
        print("文件复制成功！")
    except FileNotFoundError:
        print("文件不存在！{} -> {}".format(source_path, destination_path))
    except IsADirectoryError:
        print("源路径是一个目录！{} -> {}".format(source_path, destination_path))
    except PermissionError:
        print("没有足够的权限进行复制！{} -> {}".format(source_path, destination_path))