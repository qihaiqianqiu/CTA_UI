all = ["windows_to_linux", "linux_to_windows"]

def windows_to_linux(path):
    return path.replace("\\", "/")

def linux_to_windows(path):
    return path.replace("/", "\\")