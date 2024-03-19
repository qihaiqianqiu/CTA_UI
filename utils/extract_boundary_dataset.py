import numpy as np
# Dependencies
import os, numpy as np, pandas as pd
from pylab import mpl
mpl.rcParams['font.sans-serif'] = ['SimHei']
mpl.rcParams['axes.unicode_minus'] = False
from utils.rename import rename_db_to_param
import pandas as pd
import os
from utils.cache_management import *
from utils.calculate_parameter import *
import os
from utils.database_api import db_para
from utils.const import exchange_breed_dict, trade_day, BOUNDARY_PATH
from utils.date_section_modification import get_date_section, from_predict
from utils.database_api import init_clickhouse_client
import traceback
import time
from multiprocessing import Pool


# 如果需要新的套利对字典，可以用utils.get_contract_pair中的get_db_contract_pair()重新生成
contract_pair_dict = {'AP': [['AP210', 'AP211'], ['AP211', 'AP212'], ['AP212', 'AP301'], ['AP301', 'AP303'], ['AP303', 'AP304'], ['AP304', 'AP305'], ['AP305', 'AP310'], ['AP310', 'AP311'], ['AP311', 'AP312'], ['AP312', 'AP401'], ['AP401', 'AP403'], ['AP403', 'AP404'], ['AP404', 'AP405']], 'CF': [['CF209', 'CF211'], ['CF211', 'CF301'], ['CF301', 'CF303'], ['CF303', 'CF305'], ['CF305', 'CF307'], ['CF307', 'CF309'], ['CF309', 'CF311'], ['CF311', 'CF401'], ['CF401', 'CF403'], ['CF403', 'CF405'], ['CF405', 'CF407']], 'CJ': [['CJ209', 'CJ212'], ['CJ212', 'CJ301'], ['CJ301', 'CJ303'], ['CJ303', 'CJ305'], ['CJ305', 'CJ307'], ['CJ307', 'CJ309'], ['CJ309', 'CJ312'], ['CJ312', 'CJ401'], ['CJ401', 'CJ403'], ['CJ403', 'CJ405'], ['CJ405', 'CJ407']], 'CY': [['CY209', 'CY210'], ['CY210', 'CY211'], ['CY211', 'CY212'], ['CY212', 'CY301'], ['CY301', 'CY302'], ['CY302', 'CY303'], ['CY303', 'CY304'], ['CY304', 'CY305'], ['CY305', 'CY306'], ['CY306', 'CY307'], ['CY307', 'CY308'], ['CY308', 'CY309'], ['CY309', 'CY310'], ['CY310', 'CY311'], ['CY311', 'CY312'], ['CY312', 'CY401'], ['CY401', 'CY402'], ['CY402', 'CY403'], ['CY403', 'CY404'], ['CY404', 'CY405'], ['CY405', 'CY406'], ['CY406', 'CY407'], ['CY407', 'CY408']], 'FG': [['FG209', 'FG210'], ['FG210', 'FG211'], ['FG211', 'FG212'], ['FG212', 'FG301'], ['FG301', 'FG302'], ['FG302', 'FG303'], ['FG303', 'FG304'], ['FG304', 'FG305'], ['FG305', 'FG306'], ['FG306', 'FG307'], ['FG307', 'FG308'], ['FG308', 'FG309'], ['FG309', 'FG310'], ['FG310', 'FG311'], ['FG311', 'FG312'], ['FG312', 'FG401'], ['FG401', 'FG402'], ['FG402', 'FG403'], ['FG403', 'FG404'], ['FG404', 'FG405'], ['FG405', 'FG406'], ['FG406', 'FG407'], ['FG407', 'FG408']], 'IC': [['IC2209', 'IC2210'], ['IC2210', 'IC2211'], ['IC2211', 'IC2212'], ['IC2212', 'IC2301'], ['IC2301', 'IC2302'], ['IC2302', 'IC2303'], ['IC2303', 'IC2304'], ['IC2304', 'IC2305'], ['IC2305', 'IC2306'], ['IC2306', 'IC2307'], 
['IC2307', 'IC2308'], ['IC2308', 'IC2309'], ['IC2309', 'IC2310'], ['IC2310', 'IC2312'], ['IC2312', 'IC2403']], 'IF': [['IF2209', 'IF2210'], ['IF2210', 'IF2211'], ['IF2211', 'IF2212'], ['IF2212', 'IF2301'], ['IF2301', 'IF2302'], ['IF2302', 'IF2303'], ['IF2303', 'IF2304'], ['IF2304', 'IF2305'], ['IF2305', 'IF2306'], ['IF2306', 'IF2307'], ['IF2307', 'IF2308'], ['IF2308', 'IF2309'], ['IF2309', 'IF2310'], ['IF2310', 'IF2312'], ['IF2312', 'IF2403']], 'IH': [['IH2209', 'IH2210'], ['IH2210', 'IH2211'], ['IH2211', 'IH2212'], ['IH2212', 'IH2301'], ['IH2301', 'IH2302'], ['IH2302', 'IH2303'], ['IH2303', 'IH2304'], ['IH2304', 'IH2305'], ['IH2305', 'IH2306'], ['IH2306', 'IH2307'], ['IH2307', 'IH2308'], ['IH2308', 'IH2309'], ['IH2309', 'IH2310'], ['IH2310', 'IH2312'], ['IH2312', 'IH2403']], 'IM': [['IM2209', 'IM2210'], ['IM2210', 'IM2211'], ['IM2211', 'IM2212'], ['IM2212', 'IM2301'], ['IM2301', 'IM2302'], ['IM2302', 'IM2303'], ['IM2303', 'IM2304'], ['IM2304', 'IM2305'], ['IM2305', 'IM2306'], ['IM2306', 'IM2307'], ['IM2307', 'IM2308'], ['IM2308', 'IM2309'], ['IM2309', 'IM2310'], ['IM2310', 'IM2312'], ['IM2312', 'IM2403']], 'JR': [['JR209', 'JR211'], ['JR211', 'JR301'], ['JR301', 'JR303'], ['JR303', 'JR305'], ['JR305', 'JR307'], ['JR307', 'JR309'], ['JR309', 'JR311'], ['JR311', 'JR401'], ['JR401', 'JR403'], ['JR403', 'JR405'], ['JR405', 'JR407']], 'LR': [['LR209', 'LR211'], ['LR211', 'LR301'], ['LR301', 'LR303'], ['LR303', 'LR305'], ['LR305', 'LR307'], ['LR307', 'LR309'], ['LR309', 'LR311'], ['LR311', 'LR401'], ['LR401', 'LR403'], ['LR403', 'LR405'], ['LR405', 'LR407']], 'MA': [['MA209', 'MA210'], ['MA210', 'MA211'], ['MA211', 'MA212'], ['MA212', 'MA301'], ['MA301', 'MA302'], ['MA302', 'MA303'], ['MA303', 'MA304'], ['MA304', 'MA305'], ['MA305', 'MA306'], ['MA306', 'MA307'], ['MA307', 'MA308'], ['MA308', 'MA309'], ['MA309', 'MA310'], ['MA310', 'MA311'], ['MA311', 'MA312'], 
['MA312', 'MA401'], ['MA401', 'MA402'], ['MA402', 'MA403'], ['MA403', 'MA404'], ['MA404', 'MA405'], ['MA405', 'MA406'], ['MA406', 'MA407'], ['MA407', 'MA408']], 'OI': [['OI209', 'OI211'], ['OI211', 'OI301'], ['OI301', 'OI303'], ['OI303', 'OI305'], ['OI305', 'OI307'], ['OI307', 'OI309'], ['OI309', 'OI311'], ['OI311', 'OI401'], ['OI401', 'OI403'], ['OI403', 'OI405'], ['OI405', 'OI407']], 'PF': [['PF209', 'PF210'], ['PF210', 'PF211'], ['PF211', 'PF212'], ['PF212', 'PF301'], ['PF301', 'PF302'], ['PF302', 'PF303'], ['PF303', 'PF304'], ['PF304', 'PF305'], ['PF305', 'PF306'], ['PF306', 'PF307'], ['PF307', 'PF308'], ['PF308', 'PF309'], ['PF309', 'PF310'], ['PF310', 'PF311'], ['PF311', 'PF312'], ['PF312', 'PF401'], ['PF401', 'PF402'], ['PF402', 'PF403'], ['PF403', 'PF404'], ['PF404', 'PF405'], ['PF405', 'PF406'], ['PF406', 'PF407'], ['PF407', 'PF408']], 'PK': [['PK210', 'PK211'], ['PK211', 'PK212'], ['PK212', 'PK301'], ['PK301', 'PK303'], 
['PK303', 'PK304'], ['PK304', 'PK310'], ['PK310', 'PK311'], ['PK311', 'PK312'], ['PK312', 'PK401'], ['PK401', 'PK403'], ['PK403', 'PK404']], 'PM': [['PM209', 'PM211'], ['PM211', 'PM301'], ['PM301', 'PM303'], ['PM303', 'PM305'], ['PM305', 'PM307'], ['PM307', 'PM309'], ['PM309', 'PM311'], ['PM311', 'PM401'], ['PM401', 'PM403'], ['PM403', 'PM405'], ['PM405', 'PM407']], 'RI': [['RI209', 'RI211'], ['RI211', 'RI301'], ['RI301', 'RI303'], ['RI303', 'RI305'], ['RI305', 'RI307'], ['RI307', 'RI309'], ['RI309', 'RI311'], ['RI311', 'RI401'], ['RI401', 'RI403'], ['RI403', 'RI405'], ['RI405', 'RI407']], 'RM': [['RM209', 'RM211'], ['RM211', 'RM301'], ['RM301', 'RM303'], ['RM303', 'RM305'], ['RM305', 'RM307'], ['RM307', 'RM308'], ['RM308', 'RM309'], ['RM309', 'RM311'], ['RM311', 'RM401'], ['RM401', 'RM403'], ['RM403', 'RM405'], ['RM405', 'RM407'], ['RM407', 'RM408']], 'RS': [['RS209', 'RS211'], ['RS211', 'RS307'], ['RS307', 'RS308'], ['RS308', 'RS309'], ['RS309', 'RS311'], ['RS311', 'RS407'], ['RS407', 'RS408']], 'SA': [['SA209', 'SA210'], ['SA210', 'SA211'], ['SA211', 'SA212'], ['SA212', 'SA301'], ['SA301', 'SA302'], ['SA302', 'SA303'], ['SA303', 'SA304'], ['SA304', 'SA305'], 
['SA305', 'SA306'], ['SA306', 'SA307'], ['SA307', 'SA308'], ['SA308', 'SA309'], ['SA309', 'SA310'], ['SA310', 'SA311'], ['SA311', 'SA312'], ['SA312', 'SA401'], ['SA401', 'SA402'], ['SA402', 'SA403'], ['SA403', 'SA404'], ['SA404', 'SA405'], ['SA405', 'SA406'], ['SA406', 'SA407'], ['SA407', 'SA408']], 'SF': [['SF209', 'SF210'], ['SF210', 'SF211'], ['SF211', 'SF212'], ['SF212', 'SF301'], ['SF301', 'SF302'], ['SF302', 'SF303'], ['SF303', 'SF304'], ['SF304', 'SF305'], ['SF305', 'SF306'], ['SF306', 'SF307'], ['SF307', 'SF308'], ['SF308', 'SF309'], ['SF309', 'SF310'], ['SF310', 'SF311'], ['SF311', 'SF312'], ['SF312', 'SF401'], ['SF401', 'SF402'], ['SF402', 'SF403'], ['SF403', 'SF404'], ['SF404', 'SF405'], 
['SF405', 'SF406'], ['SF406', 'SF407'], ['SF407', 'SF408']], 'SM': [['SM209', 'SM210'], ['SM210', 'SM211'], ['SM211', 'SM212'], ['SM212', 'SM301'], ['SM301', 'SM302'], ['SM302', 'SM303'], ['SM303', 'SM304'], ['SM304', 'SM305'], ['SM305', 'SM306'], ['SM306', 'SM307'], ['SM307', 'SM308'], ['SM308', 'SM309'], ['SM309', 'SM310'], ['SM310', 'SM311'], ['SM311', 'SM312'], ['SM312', 'SM401'], ['SM401', 'SM402'], ['SM402', 'SM403'], ['SM403', 'SM404'], ['SM404', 'SM405'], ['SM405', 'SM406'], ['SM406', 'SM407'], ['SM407', 'SM408']], 'SR': [['SR209', 'SR211'], ['SR211', 'SR301'], ['SR301', 'SR303'], ['SR303', 'SR305'], ['SR305', 'SR307'], ['SR307', 'SR309'], ['SR309', 'SR311'], ['SR311', 'SR401'], ['SR401', 'SR403'], ['SR403', 'SR405'], ['SR405', 'SR407']], 'TA': [['TA209', 'TA210'], ['TA210', 'TA211'], ['TA211', 'TA212'], ['TA212', 'TA301'], ['TA301', 'TA302'], ['TA302', 'TA303'], ['TA303', 'TA304'], ['TA304', 'TA305'], ['TA305', 'TA306'], 
['TA306', 'TA307'], ['TA307', 'TA308'], ['TA308', 'TA309'], ['TA309', 'TA310'], ['TA310', 'TA311'], ['TA311', 'TA312'], ['TA312', 'TA401'], ['TA401', 'TA402'], ['TA402', 'TA403'], ['TA403', 'TA404'], ['TA404', 'TA405'], ['TA405', 'TA406'], ['TA406', 'TA407'], ['TA407', 'TA408']], 'UR': [['UR209', 'UR210'], ['UR210', 'UR211'], ['UR211', 'UR212'], ['UR212', 'UR301'], ['UR301', 'UR302'], ['UR302', 'UR303'], ['UR303', 'UR304'], ['UR304', 'UR305'], ['UR305', 'UR306'], ['UR306', 'UR307'], ['UR307', 'UR308'], ['UR308', 'UR309'], ['UR309', 'UR310'], ['UR310', 'UR311'], ['UR311', 'UR312'], ['UR312', 'UR401'], ['UR401', 'UR402'], ['UR402', 'UR403'], ['UR403', 'UR404'], ['UR404', 'UR405'], ['UR405', 'UR406'], 
['UR406', 'UR407'], ['UR407', 'UR408']], 'WH': [['WH209', 'WH211'], ['WH211', 'WH301'], ['WH301', 'WH303'], ['WH303', 'WH305'], ['WH305', 'WH307'], ['WH307', 'WH309'], ['WH309', 'WH311'], ['WH311', 'WH401'], ['WH401', 'WH403'], ['WH403', 'WH405'], ['WH405', 'WH407']], 'ZC': [['ZC210', 'ZC211'], ['ZC211', 'ZC212'], ['ZC212', 'ZC301'], ['ZC301', 'ZC302'], ['ZC302', 'ZC303'], ['ZC303', 'ZC304'], ['ZC304', 'ZC305'], ['ZC305', 'ZC306'], ['ZC306', 'ZC307'], ['ZC307', 'ZC308'], ['ZC308', 'ZC309'], ['ZC309', 'ZC310'], ['ZC310', 'ZC311'], ['ZC311', 'ZC312'], ['ZC312', 'ZC401'], ['ZC401', 'ZC402'], ['ZC402', 'ZC403'], ['ZC403', 'ZC404'], ['ZC404', 'ZC405'], ['ZC405', 'ZC406'], ['ZC406', 'ZC407'], ['ZC407', 'ZC408']], 'a': [['a2209', 'a2211'], ['a2211', 'a2301'], ['a2301', 'a2303'], ['a2303', 'a2305'], ['a2305', 'a2307'], ['a2307', 'a2309'], ['a2309', 'a2311'], ['a2311', 'a2401'], ['a2401', 'a2403'], ['a2403', 'a2405'], ['a2405', 'a2407']], 
'ag': [['ag2209', 'ag2210'], ['ag2210', 'ag2211'], ['ag2211', 'ag2212'], ['ag2212', 'ag2301'], ['ag2301', 'ag2302'], ['ag2302', 'ag2303'], ['ag2303', 'ag2304'], ['ag2304', 'ag2305'], ['ag2305', 'ag2306'], ['ag2306', 'ag2307'], ['ag2307', 'ag2308'], ['ag2308', 'ag2309'], ['ag2309', 'ag2310'], ['ag2310', 'ag2311'], ['ag2311', 'ag2312'], ['ag2312', 'ag2401'], ['ag2401', 'ag2402'], ['ag2402', 'ag2403'], ['ag2403', 'ag2404'], ['ag2404', 'ag2405'], ['ag2405', 'ag2406'], ['ag2406', 'ag2407'], ['ag2407', 'ag2408']], 'al': [['al2209', 'al2210'], ['al2210', 'al2211'], ['al2211', 'al2212'], ['al2212', 'al2301'], ['al2301', 'al2302'], ['al2302', 'al2303'], ['al2303', 'al2304'], ['al2304', 'al2305'], ['al2305', 
'al2306'], ['al2306', 'al2307'], ['al2307', 'al2308'], ['al2308', 'al2309'], ['al2309', 'al2310'], ['al2310', 'al2311'], ['al2311', 'al2312'], ['al2312', 'al2401'], ['al2401', 'al2402'], ['al2402', 'al2403'], ['al2403', 'al2404'], ['al2404', 'al2405'], ['al2405', 'al2406'], ['al2406', 'al2407'], ['al2407', 'al2408']], 'au': [['au2209', 'au2210'], ['au2210', 'au2211'], ['au2211', 'au2212'], ['au2212', 'au2301'], ['au2301', 'au2302'], ['au2302', 'au2303'], ['au2303', 'au2304'], ['au2304', 'au2305'], ['au2305', 'au2306'], ['au2306', 'au2307'], ['au2307', 'au2308'], ['au2308', 'au2309'], ['au2309', 'au2310'], ['au2310', 'au2311'], ['au2311', 'au2312'], ['au2312', 'au2402'], ['au2402', 'au2404'], ['au2404', 'au2406'], ['au2406', 'au2408']], 'b': [['b2209', 'b2210'], ['b2210', 'b2211'], ['b2211', 'b2212'], ['b2212', 'b2301'], ['b2301', 'b2302'], ['b2302', 'b2303'], ['b2303', 'b2304'], ['b2304', 'b2305'], ['b2305', 'b2306'], ['b2306', 'b2307'], ['b2307', 'b2308'], ['b2308', 'b2309'], ['b2309', 'b2310'], ['b2310', 'b2311'], ['b2311', 'b2312'], ['b2312', 'b2401'], ['b2401', 'b2402'], ['b2402', 'b2403'], ['b2403', 'b2404'], ['b2404', 'b2405'], ['b2405', 'b2406'], ['b2406', 'b2407'], ['b2407', 'b2408']], 'bc': [['bc2209', 'bc2210'], ['bc2210', 'bc2211'], ['bc2211', 'bc2212'], ['bc2212', 'bc2301'], ['bc2301', 'bc2302'], ['bc2302', 'bc2303'], ['bc2303', 'bc2304'], ['bc2304', 'bc2305'], ['bc2305', 'bc2306'], ['bc2306', 'bc2307'], ['bc2307', 'bc2308'], ['bc2308', 'bc2309'], ['bc2309', 'bc2310'], ['bc2310', 'bc2311'], ['bc2311', 'bc2312'], ['bc2312', 'bc2401'], ['bc2401', 'bc2402'], ['bc2402', 'bc2403'], ['bc2403', 'bc2404'], ['bc2404', 'bc2405'], ['bc2405', 'bc2406'], ['bc2406', 'bc2407'], ['bc2407', 'bc2408']], 'br': [['br2401', 'br2402'], ['br2402', 'br2403'], ['br2403', 'br2404'], ['br2404', 'br2405'], ['br2405', 'br2406'], ['br2406', 'br2407'], ['br2407', 'br2408']], 'bu': [['bu2209', 'bu2210'], ['bu2210', 'bu2211'], ['bu2211', 'bu2212'], ['bu2212', 'bu2301'], ['bu2301', 'bu2302'], ['bu2302', 'bu2303'], ['bu2303', 'bu2304'], ['bu2304', 'bu2305'], ['bu2305', 'bu2306'], ['bu2306', 'bu2307'], ['bu2307', 'bu2308'], ['bu2308', 'bu2309'], ['bu2309', 'bu2310'], ['bu2310', 'bu2311'], ['bu2311', 'bu2312'], ['bu2312', 'bu2401'], ['bu2401', 'bu2402'], ['bu2402', 'bu2403'], ['bu2403', 'bu2404'], ['bu2404', 'bu2405'], ['bu2405', 'bu2406'], ['bu2406', 'bu2407'], ['bu2407', 'bu2408'], ['bu2408', 'bu2409'], ['bu2409', 'bu2412'], ['bu2412', 'bu2503'], ['bu2503', 'bu2506']], 'c': [['c2209', 'c2211'], ['c2211', 'c2301'], ['c2301', 'c2303'], ['c2303', 'c2305'], ['c2305', 'c2307'], ['c2307', 'c2309'], ['c2309', 'c2311'], ['c2311', 'c2401'], ['c2401', 'c2403'], ['c2403', 'c2405'], ['c2405', 'c2407']], 'cs': [['cs2209', 'cs2211'], ['cs2211', 'cs2301'], ['cs2301', 'cs2303'], ['cs2303', 'cs2305'], ['cs2305', 'cs2307'], ['cs2307', 'cs2309'], ['cs2309', 'cs2311'], ['cs2311', 'cs2401'], ['cs2401', 'cs2403'], ['cs2403', 'cs2405'], ['cs2405', 'cs2407']], 'cu': [['cu2209', 'cu2210'], ['cu2210', 'cu2211'], ['cu2211', 'cu2212'], ['cu2212', 'cu2301'], ['cu2301', 'cu2302'], ['cu2302', 'cu2303'], ['cu2303', 'cu2304'], ['cu2304', 'cu2305'], ['cu2305', 'cu2306'], ['cu2306', 'cu2307'], ['cu2307', 'cu2308'], ['cu2308', 'cu2309'], ['cu2309', 'cu2310'], ['cu2310', 'cu2311'], ['cu2311', 'cu2312'], ['cu2312', 'cu2401'], ['cu2401', 'cu2402'], ['cu2402', 'cu2403'], ['cu2403', 'cu2404'], ['cu2404', 'cu2405'], ['cu2405', 'cu2406'], ['cu2406', 'cu2407'], ['cu2407', 'cu2408']], 'eb': [['eb2209', 'eb2210'], ['eb2210', 'eb2211'], ['eb2211', 'eb2212'], ['eb2212', 'eb2301'], ['eb2301', 'eb2302'], ['eb2302', 'eb2303'], ['eb2303', 'eb2304'], ['eb2304', 'eb2305'], ['eb2305', 'eb2306'], ['eb2306', 'eb2307'], ['eb2307', 'eb2308'], ['eb2308', 'eb2309'], ['eb2309', 'eb2310'], ['eb2310', 'eb2311'], ['eb2311', 'eb2312'], ['eb2312', 'eb2401'], ['eb2401', 'eb2402'], ['eb2402', 'eb2403'], ['eb2403', 'eb2404'], ['eb2404', 'eb2405'], ['eb2405', 'eb2406'], ['eb2406', 'eb2407'], ['eb2407', 'eb2408']], 'eg': [['eg2209', 'eg2210'], ['eg2210', 'eg2211'], ['eg2211', 'eg2212'], ['eg2212', 'eg2301'], ['eg2301', 'eg2302'], ['eg2302', 'eg2303'], ['eg2303', 'eg2304'], ['eg2304', 'eg2305'], ['eg2305', 'eg2306'], ['eg2306', 'eg2307'], ['eg2307', 'eg2308'], ['eg2308', 'eg2309'], ['eg2309', 'eg2310'], ['eg2310', 'eg2311'], ['eg2311', 'eg2312'], ['eg2312', 'eg2401'], ['eg2401', 'eg2402'], ['eg2402', 'eg2403'], ['eg2403', 'eg2404'], ['eg2404', 'eg2405'], ['eg2405', 'eg2406'], ['eg2406', 'eg2407'], ['eg2407', 'eg2408']], 'fb': [['fb2209', 'fb2210'], ['fb2210', 'fb2211'], ['fb2211', 'fb2212'], ['fb2212', 'fb2301'], ['fb2301', 'fb2302'], ['fb2302', 'fb2303'], ['fb2303', 'fb2304'], ['fb2304', 'fb2305'], ['fb2305', 'fb2306'], ['fb2306', 'fb2307'], ['fb2307', 'fb2308'], ['fb2308', 'fb2309'], ['fb2309', 'fb2310'], ['fb2310', 'fb2311'], ['fb2311', 'fb2312'], ['fb2312', 'fb2401'], ['fb2401', 'fb2402'], ['fb2402', 'fb2403'], ['fb2403', 'fb2404'], ['fb2404', 'fb2405'], ['fb2405', 'fb2406'], ['fb2406', 'fb2407'], ['fb2407', 'fb2408']], 'fu': [['fu2210', 'fu2211'], ['fu2211', 'fu2212'], ['fu2212', 'fu2301'], ['fu2301', 'fu2302'], ['fu2302', 'fu2303'], ['fu2303', 'fu2304'], ['fu2304', 'fu2305'], ['fu2305', 'fu2306'], ['fu2306', 'fu2307'], ['fu2307', 'fu2308'], ['fu2308', 'fu2309'], ['fu2309', 'fu2310'], ['fu2310', 'fu2311'], ['fu2311', 'fu2312'], ['fu2312', 'fu2401'], ['fu2401', 'fu2402'], ['fu2402', 'fu2403'], ['fu2403', 'fu2404'], ['fu2404', 'fu2405'], ['fu2405', 'fu2406'], ['fu2406', 'fu2407'], ['fu2407', 'fu2408']], 'hc': [['hc2209', 'hc2210'], ['hc2210', 'hc2211'], ['hc2211', 'hc2212'], ['hc2212', 'hc2301'], ['hc2301', 'hc2302'], ['hc2302', 'hc2303'], ['hc2303', 'hc2304'], ['hc2304', 'hc2305'], ['hc2305', 'hc2306'], ['hc2306', 'hc2307'], ['hc2307', 'hc2308'], ['hc2308', 'hc2309'], ['hc2309', 'hc2310'], ['hc2310', 'hc2311'], ['hc2311', 'hc2312'], ['hc2312', 'hc2401'], ['hc2401', 'hc2402'], ['hc2402', 'hc2403'], ['hc2403', 'hc2404'], ['hc2404', 'hc2405'], ['hc2405', 'hc2406'], ['hc2406', 'hc2407'], ['hc2407', 'hc2408']], 'i': [['i2209', 'i2210'], ['i2210', 'i2211'], ['i2211', 'i2212'], ['i2212', 'i2301'], ['i2301', 'i2302'], 
['i2302', 'i2303'], ['i2303', 'i2304'], ['i2304', 'i2305'], ['i2305', 'i2306'], ['i2306', 'i2307'], ['i2307', 'i2308'], ['i2308', 'i2309'], ['i2309', 'i2310'], ['i2310', 'i2311'], ['i2311', 'i2312'], ['i2312', 'i2401'], ['i2401', 'i2402'], ['i2402', 'i2403'], ['i2403', 'i2404'], ['i2404', 'i2405'], ['i2405', 'i2406'], ['i2406', 'i2407'], ['i2407', 'i2408']], 'j': [['j2209', 'j2210'], ['j2210', 'j2211'], ['j2211', 'j2212'], ['j2212', 'j2301'], ['j2301', 'j2302'], ['j2302', 'j2303'], ['j2303', 'j2304'], ['j2304', 'j2305'], ['j2305', 'j2306'], ['j2306', 'j2307'], ['j2307', 'j2308'], ['j2308', 'j2309'], ['j2309', 'j2310'], ['j2310', 'j2311'], ['j2311', 'j2312'], ['j2312', 'j2401'], ['j2401', 'j2402'], ['j2402', 'j2403'], ['j2403', 'j2404'], ['j2404', 'j2405'], ['j2405', 'j2406'], ['j2406', 'j2407'], ['j2407', 'j2408']], 'jd': [['jd2209', 'jd2210'], ['jd2210', 'jd2211'], ['jd2211', 'jd2212'], ['jd2212', 'jd2301'], ['jd2301', 'jd2302'], ['jd2302', 'jd2303'], ['jd2303', 'jd2304'], ['jd2304', 'jd2305'], ['jd2305', 'jd2306'], ['jd2306', 'jd2307'], ['jd2307', 'jd2308'], ['jd2308', 'jd2309'], ['jd2309', 'jd2310'], ['jd2310', 'jd2311'], ['jd2311', 'jd2312'], ['jd2312', 'jd2401'], ['jd2401', 'jd2402'], ['jd2402', 'jd2403'], ['jd2403', 'jd2404'], ['jd2404', 'jd2405'], ['jd2405', 'jd2406'], ['jd2406', 'jd2407'], ['jd2407', 'jd2408']], 'jm': [['jm2209', 'jm2210'], ['jm2210', 'jm2211'], ['jm2211', 'jm2212'], ['jm2212', 'jm2301'], ['jm2301', 'jm2302'], ['jm2302', 'jm2303'], ['jm2303', 'jm2304'], ['jm2304', 'jm2305'], ['jm2305', 'jm2306'], ['jm2306', 'jm2307'], ['jm2307', 'jm2308'], ['jm2308', 'jm2309'], ['jm2309', 'jm2310'], ['jm2310', 'jm2311'], ['jm2311', 'jm2312'], ['jm2312', 'jm2401'], ['jm2401', 'jm2402'], ['jm2402', 'jm2403'], ['jm2403', 'jm2404'], ['jm2404', 'jm2405'], ['jm2405', 'jm2406'], ['jm2406', 'jm2407'], ['jm2407', 'jm2408']], 'l': [['l2209', 'l2210'], ['l2210', 'l2211'], ['l2211', 'l2212'], ['l2212', 'l2301'], ['l2301', 'l2302'], ['l2302', 'l2303'], ['l2303', 'l2304'], ['l2304', 'l2305'], ['l2305', 'l2306'], ['l2306', 'l2307'], ['l2307', 'l2308'], ['l2308', 'l2309'], ['l2309', 'l2310'], ['l2310', 'l2311'], ['l2311', 'l2312'], ['l2312', 'l2401'], ['l2401', 'l2402'], ['l2402', 'l2403'], ['l2403', 'l2404'], ['l2404', 'l2405'], ['l2405', 'l2406'], ['l2406', 'l2407'], ['l2407', 'l2408']], 'lh': [['lh2209', 'lh2211'], ['lh2211', 'lh2301'], ['lh2301', 'lh2303'], ['lh2303', 'lh2305'], ['lh2305', 'lh2307'], ['lh2307', 'lh2309'], ['lh2309', 'lh2311'], ['lh2311', 'lh2401'], ['lh2401', 'lh2403'], ['lh2403', 'lh2405'], ['lh2405', 'lh2407']], 'lu': [['lu2210', 'lu2211'], ['lu2211', 'lu2212'], ['lu2212', 'lu2301'], ['lu2301', 'lu2302'], ['lu2302', 'lu2303'], ['lu2303', 'lu2304'], ['lu2304', 'lu2305'], ['lu2305', 'lu2306'], ['lu2306', 'lu2307'], ['lu2307', 'lu2308'], ['lu2308', 'lu2309'], ['lu2309', 'lu2310'], ['lu2310', 'lu2311'], ['lu2311', 'lu2312'], ['lu2312', 'lu2401'], ['lu2401', 'lu2402'], ['lu2402', 'lu2403'], ['lu2403', 'lu2404'], ['lu2404', 'lu2405'], ['lu2405', 'lu2406'], ['lu2406', 'lu2407'], ['lu2407', 'lu2408']], 'm': [['m2209', 'm2211'], ['m2211', 'm2212'], ['m2212', 'm2301'], ['m2301', 'm2303'], ['m2303', 'm2305'], ['m2305', 'm2307'], ['m2307', 'm2308'], ['m2308', 'm2309'], ['m2309', 'm2311'], ['m2311', 'm2312'], ['m2312', 'm2401'], ['m2401', 'm2403'], ['m2403', 'm2405'], ['m2405', 'm2407'], ['m2407', 'm2408']], 'ni': [['ni2209', 'ni2210'], ['ni2210', 'ni2211'], ['ni2211', 'ni2212'], ['ni2212', 'ni2301'], ['ni2301', 'ni2302'], ['ni2302', 'ni2303'], ['ni2303', 'ni2304'], ['ni2304', 'ni2305'], ['ni2305', 'ni2306'], ['ni2306', 'ni2307'], ['ni2307', 'ni2308'], ['ni2308', 'ni2309'], ['ni2309', 'ni2310'], ['ni2310', 'ni2311'], ['ni2311', 'ni2312'], ['ni2312', 'ni2401'], ['ni2401', 'ni2402'], ['ni2402', 'ni2403'], ['ni2403', 'ni2404'], ['ni2404', 'ni2405'], ['ni2405', 'ni2406'], ['ni2406', 'ni2407'], ['ni2407', 'ni2408']], 'nr': [['nr2209', 'nr2210'], ['nr2210', 'nr2211'], ['nr2211', 'nr2212'], ['nr2212', 'nr2301'], ['nr2301', 'nr2302'], ['nr2302', 'nr2303'], ['nr2303', 'nr2304'], ['nr2304', 'nr2305'], ['nr2305', 'nr2306'], ['nr2306', 'nr2307'], ['nr2307', 'nr2308'], ['nr2308', 'nr2309'], ['nr2309', 'nr2310'], ['nr2310', 'nr2311'], ['nr2311', 'nr2312'], ['nr2312', 'nr2401'], ['nr2401', 'nr2402'], ['nr2402', 'nr2403'], ['nr2403', 'nr2404'], ['nr2404', 'nr2405'], ['nr2405', 'nr2406'], ['nr2406', 'nr2407'], ['nr2407', 'nr2408']], 'p': [['p2209', 'p2210'], ['p2210', 'p2211'], ['p2211', 'p2212'], ['p2212', 'p2301'], ['p2301', 'p2302'], ['p2302', 'p2303'], ['p2303', 'p2304'], ['p2304', 'p2305'], ['p2305', 'p2306'], ['p2306', 'p2307'], ['p2307', 'p2308'], ['p2308', 'p2309'], ['p2309', 'p2310'], ['p2310', 'p2311'], ['p2311', 'p2312'], ['p2312', 'p2401'], ['p2401', 'p2402'], ['p2402', 'p2403'], ['p2403', 'p2404'], ['p2404', 'p2405'], ['p2405', 'p2406'], ['p2406', 'p2407'], ['p2407', 'p2408']], 'pb': [['pb2209', 'pb2210'], ['pb2210', 'pb2211'], ['pb2211', 'pb2212'], ['pb2212', 'pb2301'], ['pb2301', 'pb2302'], ['pb2302', 'pb2303'], ['pb2303', 'pb2304'], ['pb2304', 'pb2305'], ['pb2305', 'pb2306'], ['pb2306', 'pb2307'], ['pb2307', 'pb2308'], ['pb2308', 'pb2309'], ['pb2309', 'pb2310'], ['pb2310', 'pb2311'], ['pb2311', 'pb2312'], ['pb2312', 'pb2401'], ['pb2401', 'pb2402'], ['pb2402', 'pb2403'], ['pb2403', 'pb2404'], ['pb2404', 'pb2405'], ['pb2405', 'pb2406'], ['pb2406', 'pb2407'], ['pb2407', 'pb2408']], 'pg': [['pg2209', 'pg2210'], ['pg2210', 'pg2211'], ['pg2211', 'pg2212'], ['pg2212', 'pg2301'], ['pg2301', 'pg2302'], ['pg2302', 'pg2303'], ['pg2303', 'pg2304'], ['pg2304', 'pg2305'], ['pg2305', 'pg2306'], ['pg2306', 'pg2307'], ['pg2307', 'pg2308'], ['pg2308', 'pg2309'], ['pg2309', 'pg2310'], ['pg2310', 'pg2311'], ['pg2311', 'pg2312'], ['pg2312', 'pg2401'], ['pg2401', 'pg2402'], ['pg2402', 'pg2403'], ['pg2403', 'pg2404'], ['pg2404', 'pg2405'], ['pg2405', 'pg2406'], ['pg2406', 'pg2407'], ['pg2407', 'pg2408']], 'pp': [['pp2209', 'pp2210'], ['pp2210', 'pp2211'], ['pp2211', 'pp2212'], ['pp2212', 'pp2301'], ['pp2301', 'pp2302'], ['pp2302', 'pp2303'], ['pp2303', 'pp2304'], ['pp2304', 'pp2305'], ['pp2305', 'pp2306'], ['pp2306', 'pp2307'], ['pp2307', 'pp2308'], ['pp2308', 'pp2309'], ['pp2309', 'pp2310'], ['pp2310', 'pp2311'], ['pp2311', 'pp2312'], ['pp2312', 'pp2401'], ['pp2401', 'pp2402'], ['pp2402', 'pp2403'], ['pp2403', 'pp2404'], ['pp2404', 'pp2405'], ['pp2405', 'pp2406'], ['pp2406', 'pp2407'], ['pp2407', 'pp2408']], 'rb': [['rb2209', 'rb2210'], ['rb2210', 'rb2211'], ['rb2211', 'rb2212'], ['rb2212', 'rb2301'], ['rb2301', 'rb2302'], ['rb2302', 'rb2303'], ['rb2303', 'rb2304'], ['rb2304', 'rb2305'], ['rb2305', 'rb2306'], ['rb2306', 'rb2307'], ['rb2307', 'rb2308'], ['rb2308', 'rb2309'], ['rb2309', 'rb2310'], ['rb2310', 'rb2311'], ['rb2311', 'rb2312'], ['rb2312', 'rb2401'], ['rb2401', 'rb2402'], ['rb2402', 'rb2403'], ['rb2403', 'rb2404'], ['rb2404', 'rb2405'], ['rb2405', 'rb2406'], ['rb2406', 'rb2407'], ['rb2407', 'rb2408']], 'rr': [['rr2209', 'rr2210'], ['rr2210', 'rr2211'], ['rr2211', 'rr2212'], ['rr2212', 'rr2301'], ['rr2301', 'rr2302'], ['rr2302', 'rr2303'], ['rr2303', 'rr2304'], ['rr2304', 'rr2305'], ['rr2305', 'rr2306'], ['rr2306', 'rr2307'], ['rr2307', 'rr2308'], ['rr2308', 'rr2309'], ['rr2309', 'rr2310'], ['rr2310', 'rr2311'], ['rr2311', 'rr2312'], ['rr2312', 'rr2401'], ['rr2401', 'rr2402'], ['rr2402', 'rr2403'], ['rr2403', 'rr2404'], ['rr2404', 'rr2405'], ['rr2405', 'rr2406'], ['rr2406', 'rr2407'], ['rr2407', 'rr2408']], 'ru': [['ru2209', 'ru2210'], ['ru2210', 'ru2211'], ['ru2211', 'ru2301'], ['ru2301', 'ru2303'], ['ru2303', 'ru2304'], ['ru2304', 'ru2305'], ['ru2305', 'ru2306'], ['ru2306', 'ru2307'], ['ru2307', 'ru2308'], ['ru2308', 'ru2309'], ['ru2309', 'ru2310'], ['ru2310', 'ru2311'], ['ru2311', 'ru2401'], ['ru2401', 'ru2403'], ['ru2403', 'ru2404'], ['ru2404', 'ru2405'], ['ru2405', 'ru2406'], ['ru2406', 'ru2407'], ['ru2407', 'ru2408']], 'sc': [['sc2210', 'sc2211'], ['sc2211', 'sc2212'], ['sc2212', 'sc2301'], ['sc2301', 'sc2302'], ['sc2302', 'sc2303'], ['sc2303', 'sc2304'], ['sc2304', 'sc2305'], 
['sc2305', 'sc2306'], ['sc2306', 'sc2307'], ['sc2307', 'sc2308'], ['sc2308', 'sc2309'], ['sc2309', 'sc2310'], ['sc2310', 'sc2311'], ['sc2311', 'sc2312'], ['sc2312', 'sc2401'], ['sc2401', 'sc2402'], ['sc2402', 'sc2403'], ['sc2403', 'sc2404'], ['sc2404', 'sc2405'], ['sc2405', 'sc2406'], ['sc2406', 'sc2407'], ['sc2407', 'sc2408'], ['sc2408', 'sc2409'], ['sc2409', 'sc2412'], ['sc2412', 'sc2503'], ['sc2503', 'sc2506'], ['sc2506', 'sc2509'], ['sc2509', 'sc2512'], ['sc2512', 'sc2603'], ['sc2603', 'sc2606']], 'si': [['si2308', 'si2309'], ['si2309', 'si2310'], ['si2310', 'si2311'], ['si2311', 'si2312'], ['si2312', 'si2401'], ['si2401', 'si2402'], ['si2402', 'si2403'], ['si2403', 'si2404']], 'sn': [['sn2209', 'sn2210'], ['sn2210', 'sn2211'], ['sn2211', 'sn2212'], ['sn2212', 'sn2301'], ['sn2301', 'sn2302'], ['sn2302', 'sn2303'], ['sn2303', 'sn2304'], ['sn2304', 'sn2305'], ['sn2305', 'sn2306'], ['sn2306', 'sn2307'], ['sn2307', 'sn2308'], ['sn2308', 'sn2309'], ['sn2309', 'sn2310'], ['sn2310', 'sn2311'], ['sn2311', 'sn2312'], ['sn2312', 'sn2401'], ['sn2401', 'sn2402'], ['sn2402', 'sn2403'], ['sn2403', 'sn2404'], ['sn2404', 'sn2405'], ['sn2405', 'sn2406'], ['sn2406', 'sn2407'], ['sn2407', 'sn2408']], 'sp': [['sp2209', 'sp2210'], ['sp2210', 'sp2211'], ['sp2211', 'sp2212'], ['sp2212', 'sp2301'], ['sp2301', 'sp2302'], ['sp2302', 'sp2303'], ['sp2303', 'sp2304'], ['sp2304', 'sp2305'], ['sp2305', 'sp2306'], ['sp2306', 'sp2307'], ['sp2307', 'sp2308'], ['sp2308', 'sp2309'], ['sp2309', 'sp2310'], ['sp2310', 'sp2311'], ['sp2311', 'sp2312'], ['sp2312', 'sp2401'], ['sp2401', 'sp2402'], ['sp2402', 'sp2403'], ['sp2403', 'sp2404'], ['sp2404', 'sp2405'], 
['sp2405', 'sp2406'], ['sp2406', 'sp2407'], ['sp2407', 'sp2408']], 'ss': [['ss2209', 'ss2210'], ['ss2210', 'ss2211'], ['ss2211', 'ss2212'], ['ss2212', 'ss2301'], ['ss2301', 'ss2302'], ['ss2302', 'ss2303'], ['ss2303', 'ss2304'], ['ss2304', 'ss2305'], ['ss2305', 'ss2306'], ['ss2306', 'ss2307'], ['ss2307', 'ss2308'], ['ss2308', 'ss2309'], ['ss2309', 'ss2310'], ['ss2310', 'ss2311'], ['ss2311', 'ss2312'], ['ss2312', 'ss2401'], ['ss2401', 'ss2402'], ['ss2402', 'ss2403'], ['ss2403', 'ss2404'], ['ss2404', 'ss2405'], ['ss2405', 'ss2406'], ['ss2406', 'ss2407'], ['ss2407', 'ss2408']], 'v': [['v2209', 'v2210'], ['v2210', 'v2211'], ['v2211', 'v2212'], ['v2212', 'v2301'], ['v2301', 'v2302'], ['v2302', 'v2303'], ['v2303', 'v2304'], ['v2304', 'v2305'], ['v2305', 'v2306'], ['v2306', 'v2307'], ['v2307', 'v2308'], ['v2308', 'v2309'], ['v2309', 'v2310'], ['v2310', 'v2311'], ['v2311', 'v2312'], ['v2312', 'v2401'], ['v2401', 'v2402'], ['v2402', 'v2403'], ['v2403', 'v2404'], ['v2404', 'v2405'], ['v2405', 'v2406'], ['v2406', 'v2407'], ['v2407', 'v2408']], 'y': [['y2209', 'y2211'], ['y2211', 'y2212'], ['y2212', 'y2301'], ['y2301', 'y2303'], ['y2303', 'y2305'], ['y2305', 'y2307'], ['y2307', 'y2308'], ['y2308', 'y2309'], ['y2309', 'y2311'], ['y2311', 'y2312'], ['y2312', 'y2401'], ['y2401', 'y2403'], ['y2403', 'y2405'], ['y2405', 'y2407'], ['y2407', 'y2408']], 'zn': [['zn2209', 'zn2210'], ['zn2210', 'zn2211'], ['zn2211', 'zn2212'], ['zn2212', 'zn2301'], ['zn2301', 'zn2302'], ['zn2302', 'zn2303'], ['zn2303', 'zn2304'], ['zn2304', 'zn2305'], ['zn2305', 'zn2306'], ['zn2306', 'zn2307'], ['zn2307', 'zn2308'], ['zn2308', 'zn2309'], ['zn2309', 'zn2310'], ['zn2310', 'zn2311'], ['zn2311', 'zn2312'], ['zn2312', 'zn2401'], ['zn2401', 'zn2402'], ['zn2402', 'zn2403'], ['zn2403', 'zn2404'], ['zn2404', 'zn2405'], ['zn2405', 'zn2406'], ['zn2406', 'zn2407'], ['zn2407', 'zn2408']]}

def get_param_contract_pair():
    breed_lst = []
    for key, values in exchange_breed_dict.items():
        breed_lst += exchange_breed_dict[key]
    breed_lst += [x.lower() for x in breed_lst]
    cta_table = db_para['tb_to']
    SQL = "SELECT distinct contract, breed from " + cta_table + " where breed in " + str(tuple(breed_lst))
    print(SQL)
    if not ("clickhouse_client" in locals() or "clickhouse_client" in globals()):
        clickhouse_client = init_clickhouse_client()
    df = clickhouse_client.query_dataframe(SQL).sort_values('contract')
    contract_pair_dict = {}
    for breed_class in df.groupby('breed'):
        contract_pair_lst = []
        contract_lst = breed_class[1]['contract'].tolist()
        contract_pair_lst += [[rename_db_to_param(contract_lst[i]), rename_db_to_param(contract_lst[i+1])] for i in range(len(contract_lst)-1)]
        contract_pair_dict[breed_class[0]] = contract_pair_lst
    return contract_pair_dict



        
        
# 单一套利对，起止交易日之间全部早、午、夜市
def pair_plot_section(contract_pair:list, start_date:int, end_date:int):
    # Trading_section = [['9','11:30'], ['13:30','15:00'] ,['21:00','2:30']] or [1D, 3D, 5D, 11D, 22D]
    pair_data = get_pairwise_data(contract_pair, start_date=trade_day[trade_day.index(start_date)], end_date=trade_day[trade_day.index(end_date)])
    print("从数据库中获取到套利对数据：", pair_data.head())
    if len(pair_data) > 0:
        df_0 = pair_data[(pair_data['time'] > '09') & (pair_data['time'] < '11:30')]
        df_1 = pair_data[(pair_data['time'] > '13:30') & (pair_data['time'] < '15:00')]
        df_2 = pair_data[(pair_data['time'] > '21') | (pair_data['time'] < '02:30')]
        df = [df_0, df_1, df_2]
        #买近卖远
        df_abr_b = [pd.concat([temp['trading_date'], (temp['ap1_' + contract_pair[0]] - temp['bp1_' + contract_pair[1]])], axis=1) for temp in df]
        #卖近买远
        df_abr_s = [(temp['bp1_' + contract_pair[0]] - temp['ap1_' + contract_pair[1]]) for temp in df]
        return df_abr_b, df_abr_s
    else:
        return [pd.DataFrame(), pd.DataFrame(), pd.DataFrame()], [pd.DataFrame(), pd.DataFrame(), pd.DataFrame()]
    
        
# Get data from start_date[MorningMarket] to end_date[EveningMarket]
def get_pairwise_data(contract_pair:list, start_date:int, end_date:int):
    if not ("clickhouse_client" in locals() or "clickhouse_client" in globals()):
        clickhouse_client = init_clickhouse_client()
    start = time.time()
    select_clause = 'SELECT contract, trading_date, time, ap1, av1, bp1, bv1 from ctp_future_tick '
    where_clause = 'WHERE contract in ' + str(tuple(contract_pair)) + ' and trading_date >= '+ str(start_date) + ' and trading_date <= ' + str(end_date)
    query = select_clause + where_clause
    res = clickhouse_client.query_dataframe(query)
    print("SQL TIME: ", time.time()-start)
    if len(res) > 0:
        res = res.sort_values(by=['trading_date', 'time'])
        res['ms'] = res['time'].apply(lambda x: x.split(':')[3])
        # 个别tick毫秒数据会带有小数点，需要转换为整数
        res['ms'] = res['ms'].astype('float').astype('int')
        res['time'] = res.apply(lambda x: x['time'].split(':')[0] + ':' + x['time'].split(':')[1] + ':' + x['time'].split(':')[2] + ':000' if x['ms'] < 500 else x['time'].split(':')[0] + ':' + x['time'].split(':')[1] + ':' + x['time'].split(':')[2] + ':500', axis=1)

        # Inner join
        pair_data_0 = res[res['contract'] == contract_pair[0]]
        pair_data_1 = res[res['contract'] == contract_pair[1]]
        pair_data = pd.merge(pair_data_0, pair_data_1, how='inner', on=['trading_date','time'],suffixes=('_' + contract_pair[0], '_' + contract_pair[1]))
        pair_data.drop(columns=['contract_'+contract_pair[0], 'contract_'+contract_pair[1]], inplace=True)
        pair_data.sort_values(by=['trading_date', 'time'])
        pair_data = pair_data.fillna(method='ffill').replace([np.inf, -np.inf], np.nan).dropna()
    
        return pair_data
    
    else:
        return pd.DataFrame()
    
    
def check_update_flag(contract_pair:list, q:float):
    # future_pair: ['fu2301', 'fu2305'] --> Paramater CSV: BOUNDARY_PATH/fu2301-fu2305.csv
    pair_filename = contract_pair[0] + '-' + contract_pair[1] + '.csv'
    PARA = os.path.join(BOUNDARY_PATH, "q=" + str(q))
    flag = os.path.exists(os.path.join(PARA, pair_filename))
    if flag:
        df = pd.read_csv(os.path.join(PARA, pair_filename))
        if len(df) == 0:
            flag = False
    return flag, pair_filename
    
    
def export_parameter_csv(contract_pair_group:list, start_date:int, end_date:int, q:float):
    start = time.time()
    PARA = os.path.join(BOUNDARY_PATH, "q=" + str(q))
    print("开始导出套利对数据：", contract_pair_group)
    try:
        for pair in contract_pair_group:
            pair_name = pair[0] + '-' + pair[1]
            column_lst = ['date','max(buy)', 'min(buy)', 'mean(buy)', 'mean-std(buy)', 'mean-2std(buy)', 'mean-2.5std(buy)', 'price_quantile_90(buy)', 'price_quantile_95(buy)', 'price_quantile_N(buy)', 'max(sell)', 'min(sell)', 'mean(sell)', 'mean+std(sell)', 'mean+2std(sell)', 'mean+2.5std(sell)', 'price_quantile_90(sell)', 'price_quantile_95(sell)', 'price_quantile_N(sell)']
            df = pd.DataFrame(columns=column_lst)
            # Appending module
            update_flag, pair_fname = check_update_flag(pair,q)
            print("增量模式：", update_flag)
            if update_flag:
                Boundary = os.path.join(BOUNDARY_PATH, "q=" + str(q))
                # Reading saved params file of current pair
                df = pd.read_csv(os.path.join(Boundary, pair_fname))
                saved_date = int(df['date'].max().split('_')[0])
                saved_section = int(df['date'].max().split('_')[1])
                # Append from next trading section
                if saved_section == 0 or saved_section == 1:
                    # Append to next trading day
                    start_date = saved_date
                    df_b, df_s = pair_plot_section(pair, start_date, start_date)
                    for i in range(saved_section, 2):
                        df_b_temp = df_b[i]
                        df_s_temp = df_s[i]
                        df_combine = pd.concat([df_b_temp,df_s_temp],axis=1)
                        if len(df_combine) > 0:
                            for temp in pd.concat([df_b_temp,df_s_temp],axis=1).groupby('trading_date'):
                                # 交易单元代码轮换
                                if i == 0:
                                    date = str(trade_day[trade_day.index(temp[0])]) + '_' + str(1)
                                if i == 1:
                                    date = str(trade_day[trade_day.index(temp[0])]) + '_' + str(2)
                                max_0 = temp[1].iloc[:,1].max()
                                min_0 = temp[1].iloc[:,1].min()
                                mean_0 = temp[1].iloc[:,1].mean()
                                std_0 = temp[1].iloc[:,1].std()
                                q_0_90 = temp[1].iloc[:,1].quantile(.9)
                                q_0_95 = temp[1].iloc[:,1].quantile(.95)
                                q_0_N = temp[1].iloc[:,1].quantile(q)
                                max_1 = temp[1].iloc[:,2].max()
                                min_1 = temp[1].iloc[:,2].min()
                                mean_1 = temp[1].iloc[:,2].mean()
                                std_1 = temp[1].iloc[:,2].std()
                                q_1_90 = temp[1].iloc[:,2].quantile(.1)
                                q_1_95 = temp[1].iloc[:,2].quantile(.05)
                                q_1_N = temp[1].iloc[:,2].quantile(1-q)
                                #print(q_0_90,q_0_95, max_1,min_1)
                                row_data = {'date': [date], 'max(buy)': max_0, 'min(buy)': min_0, 'mean(buy)':mean_0, 'mean-std(buy)':mean_0-std_0, 'mean-2std(buy)':mean_0-2*std_0, 'mean-2.5std(buy)':mean_0-2.5*std_0, 'price_quantile_90(buy)':q_0_90, 'price_quantile_95(buy)':q_0_95, 'price_quantile_N(buy)':q_0_N, 'max(sell)':max_1, 'min(sell)':min_1, 'mean(sell)':mean_1, 'mean+std(sell)':mean_1+std_1, 'mean+2std(sell)':mean_1+2*std_1, 'mean+2.5std(sell)':mean_1+2.5*std_1, 'price_quantile_90(sell)':q_1_90, 'price_quantile_95(sell)':q_1_95, 'price_quantile_N(sell)':q_1_N}
                                df = pd.concat([df,pd.DataFrame(row_data)])
                # Pushing start pointer one day later
                start_date = trade_day[trade_day.index(saved_date)+1]
            if start_date <= end_date:
                # Appending following days
                # Query DB            
                df_b, df_s = pair_plot_section(pair, start_date, end_date)
                for i in range(3):
                    df_b_temp = df_b[i]
                    df_s_temp = df_s[i]
                    df_combine = pd.concat([df_b_temp,df_s_temp],axis=1)
                    if len(df_combine) > 0:
                        for temp in pd.concat([df_b_temp,df_s_temp],axis=1).groupby('trading_date'):
                            # 交易单元代码轮换
                            if i == 0:
                                date = str(trade_day[trade_day.index(temp[0])]) + '_' + str(1)
                            if i == 1:
                                date = str(trade_day[trade_day.index(temp[0])]) + '_' + str(2)
                            if i == 2:
                                date = str(trade_day[trade_day.index(temp[0])]) + '_' + str(0)
                            max_0 = temp[1].iloc[:,1].max()
                            min_0 = temp[1].iloc[:,1].min()
                            mean_0 = temp[1].iloc[:,1].mean()
                            std_0 = temp[1].iloc[:,1].std()
                            q_0_90 = temp[1].iloc[:,1].quantile(.9)
                            q_0_95 = temp[1].iloc[:,1].quantile(.95)
                            q_0_N = temp[1].iloc[:,1].quantile(q)
                            max_1 = temp[1].iloc[:,2].max()
                            min_1 = temp[1].iloc[:,2].min()
                            mean_1 = temp[1].iloc[:,2].mean()
                            std_1 = temp[1].iloc[:,2].std()
                            q_1_90 = temp[1].iloc[:,2].quantile(.1)
                            q_1_95 = temp[1].iloc[:,2].quantile(.05)
                            q_1_N = temp[1].iloc[:,2].quantile(1-q)
                            #print(q_0_90,q_0_95, max_1,min_1)
                            row_data = {'date': [date], 'max(buy)': max_0, 'min(buy)': min_0, 'mean(buy)':mean_0, 'mean-std(buy)':mean_0-std_0, 'mean-2std(buy)':mean_0-2*std_0, 'mean-2.5std(buy)':mean_0-2.5*std_0, 'price_quantile_90(buy)':q_0_90, 'price_quantile_95(buy)':q_0_95, 'price_quantile_N(buy)':q_0_N, 'max(sell)':max_1, 'min(sell)':min_1, 'mean(sell)':mean_1, 'mean+std(sell)':mean_1+std_1, 'mean+2std(sell)':mean_1+2*std_1, 'mean+2.5std(sell)':mean_1+2.5*std_1, 'price_quantile_90(sell)':q_1_90, 'price_quantile_95(sell)':q_1_95, 'price_quantile_N(sell)':q_1_N}
                            df = pd.concat([df,pd.DataFrame(row_data)])
        df = df.sort_values('date')
        print("套利对数据：", df.head())
        print("DataLoading time: ", time.time()-start)
        df.to_csv(os.path.join(PARA, pair_name + '.csv'), index=False)
        print("已成功导出套利对数据：", pair_name)
        return df
    except Exception as e:
        print("导出套利对数据失败：", contract_pair_group)
        print(traceback.format_exc())
    


# 进度条计时器在这里放
def export_boundary_dataset(q=0.95, start_date=20220908):
    # 获取库中所有的套利对
    date, section = get_date_section()
    date, section = from_predict(date, section)
    """
    region_df = pd.read_excel('./info/region_info.xlsx', sheet_name="Sheet1")
    pairs = region_df['pairs_id']
    append_pairs = [f.split('.')[0] for f in os.listdir(r'Z:\300_Group\HFT\Program\CTA_UI\info\boundary_info\q=0.95')]
    pairs = pairs.append(pd.Series(append_pairs))
    pairs = pairs.drop_duplicates()
    """
    pairs = [value for sublist in contract_pair_dict.values() for value in sublist]
    
    with Pool(2) as p:
        for i in range(0, len(pairs), 2):
            contract_pairs = pairs[i:i+2]
            arg_lst = [([x], start_date, date, q) for x in contract_pairs]
            p.starmap(export_parameter_csv, arg_lst)
        p.close() 
        p.join()


if __name__ == "__main__":
    print(export_boundary_dataset())
