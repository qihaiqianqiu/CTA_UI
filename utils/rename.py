import re
from utils.const import breed_dict
all = ["rename"]
# Contract name from upper case and %ccdddd [params.csv] to lower case and %cdddd [breed_dict] 
def rename(contract:str):
    code = re.search("[a-zA-Z]+", contract).group(0)
    figure = re.search("[0-9]+", contract).group(0)
    code_mapping = breed_dict
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

# reverse contract string from database format into param format
def rename_db_to_param(contract:str):
    code = re.search("[a-zA-Z]+", contract).group(0)
    figure = re.search("[0-9]+", contract).group(0)
    code = code.upper()
    if len(figure) == 3:
        figure = "2" + figure
        
    return code + figure

if __name__ == "__main__":
    print(rename_db_to_param(""))