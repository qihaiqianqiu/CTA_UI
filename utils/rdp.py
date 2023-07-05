import os
from utils.const import ROOT_PATH
from utils.path_exp_switch import linux_to_windows

def rdp_transmission_demo():
    demo_scp_prompt = r"scp D:\local_repo\CTA_UI\params\BASE\params.csv administrator@172.31.113.251:C:\Users\Administrator\Desktop\rdp"
    pwd = "Abc@123"
    os.system(demo_scp_prompt)
    os.system(pwd)



if __name__ == "__main__":
    rdp_transmission_demo()