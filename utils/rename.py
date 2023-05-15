import re
from . import const
all = ["rename"]
# Contract name from upper case and %ccdddd
def rename(contract:str):
    code = re.search("[a-zA-Z]+", contract).group(0)
    figure = re.search("[0-9]+", contract).group(0)
    code_mapping = const.breed_dict
    counter = 0
    for key, values in code_mapping.items():
        if values == code:
            if code not in ["IF","IH","IC","IM"]:
                figure = figure[1:]
        else:
            counter += 1
    if counter == len(code_mapping):
        code = code.lower()
    
    return code + figure