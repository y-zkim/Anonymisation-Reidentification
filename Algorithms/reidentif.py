import csv 
import pandas as pd 
import numpy as np

def count_qids(df_Anon):
    # Read from CSV's file 
    df = pd.read_csv(df_Anon, sep=',').set_axis(['index','Id', 'Date', 'longitude', 'lattitude','Week','QID'], axis=1, inplace=False)

    #declarer dictionnaire (Maps: key->value)
    Dict = {}

    # Transform : Date to Year-week 
    df['Date'] = pd.to_datetime(df['Date'], errors='coerce')
    df['Week'] = df['Date'].dt.strftime('%Y-%U')

    for index, row in df.iterrows():
        #print(row['Week'],row['Id'])
        if str(row['Week'])+'-'+str(row['QID']) in Dict:
            Dict[str(row['Week'])+'-'+str(row['QID'])] = 1 + Dict[str(row['Week'])+'-'+str(row['QID'])]
        elif str(row['QID'])=='DEL' or str(row['Id'])=='DEL':
            if str(row['Week']) + '-DEL' in Dict:
                Dict[str(row['Week']) + '-DEL'] = 1 + Dict[str(row['Week']) + '-DEL']
            else:
                Dict[str(row['Week']) + '-DEL'] = 1
        else:
            Dict[str(row['Week'])+'-'+str(row['QID'])] = 1
    
    print(Dict)



def count_ids(df_Orig):
    # Read from CSV's file 
    df = pd.read_csv(df_Orig, sep=',').set_axis(['Id', 'Date', 'longitude', 'lattitude'], axis=1, inplace=False)

    # Transform : Date to Year-week 
    df['Date'] = pd.to_datetime(df['Date'], errors='coerce')
    df['Week'] = df['Date'].dt.strftime('%Y-%U')

    #declarer dictionnaire (Maps: key->value)
    Dict = {}
    #Dict['1']=2
    #Dict['1']=Dict['1']+1
    #print(Dict)
    #boucler sur chaque week
    """
        key -> Value 
        week-id  -> nbr de fois
        2015-11-2 -> 3
    """
    for index, row in df.iterrows():
        #print(row['Week'],row['Id'])
        if str(row['Week'])+'-'+str(row['Id']) in Dict:
            Dict[str(row['Week'])+'-'+str(row['Id'])] = 1 + Dict[str(row['Week'])+'-'+str(row['Id'])]
        else:
            Dict[str(row['Week'])+'-'+str(row['Id'])] = 1  
    print(Dict)
    #pd.set_option("display.max_rows", None, "display.max_columns", None)
    #print(df[['Week','Id']])
    #print(df)


count_ids('Classeur1.csv')
count_qids('df_Anony.csv')


