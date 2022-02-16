from os import name
import pandas as pd
import csv
from math import *
import json
import gc
import datetime

# PATH of Original Database
path_orig = "../INSAnonym/c3465dad3864bb1e373891fdcfbfcca5f974db6a9e0b646584e07c5f554d7df7"

# PATH of Anonymized Database
path_ano = "/media/alaghlid/Disque/Attack Dbs/Sibyl_483/files/S_user_40_2b9a2307d8c8d01b3885a20a7d22f25eccfa2d8c319c85521c3b1947994c4b82"

# PATH of JSON file
path_json = "../SYBYL_483.json"

### Read files and generate files of each week 

def read(df_org, df_ano):

    ## Read Anonymized Database
    dfAnon = pd.read_csv(df_ano, sep="\t", header=None).set_axis(['pseudo_id', 'Date', 'longitude', 'lattitude'], axis=1, inplace=False)
    ## Delete rows : pseudo_id == "DEL"
    dfAnon = dfAnon[dfAnon['pseudo_id']!='DEL']
    dfAnon['Date'] = pd.to_datetime(dfAnon['Date'], errors='coerce', dayfirst=True)
    ## Create Column of week
    dfAnon['week'] = dfAnon['Date'].dt.strftime('%Y-%W')

    ## Read Original Database
    dfOrig = pd.read_csv(df_org, sep="\t", header=None).set_axis(['Id', 'Date', 'longitude', 'lattitude'], axis=1, inplace=False)
    dfOrig['Date'] = pd.to_datetime(dfOrig['Date'], errors='coerce', dayfirst=True)
    ## Create Column of week
    dfOrig['week'] = dfOrig['Date'].dt.strftime('%Y-%W')

    print("-->Reading Files : Passed : ")
    i=0
    ## Generate files (original & anonymized) of each week
    for week in dfOrig['week'].unique():
         dfOrig[dfOrig['week']  == week].to_csv(f'../bases_org/week_{week}.csv', header=False, index=False,sep='\t')
         dfAnon[dfAnon['week'] == week].to_csv(f'../bases_ano/week_{week}.csv', header=False, index=False,sep='\t')
         i+=1
    print("--> Generating files of weeks : Passed")
    return dfOrig['week'].unique(),dfOrig['Id'].unique()


### Create JSON file 

def counter(base_org, base_ano):
    gc.enable()
    
    ## lists of ids & weeks
    weeks,ids = read(base_org,base_ano)
    Guesses = {}
    for id in ids:
        Guesses[str(id)]={}
    print("--> Create Dict of Guesses")

    for week in weeks:
        print(f"It's week {week}")
        
        # Create dataframe of each week & index by date
        dforig = pd.read_csv(f'../bases_org/week_{week}.csv', sep='\t', names=["id", "date", "lat", "long","week"]).set_index('date')
        dfano = pd.read_csv(f'../bases_ano/week_{week}.csv', sep='\t', names=["pseudo_id", "date", "lat", "long","week"]).set_index('date')
        
        # Drop column of Week
        dforig = dforig.drop(columns=['week'])
        dfano = dfano.drop(columns=['week'])

        # Uncomment if u want to apply round function
            # cols = ['lat', 'long']
            # dforig[cols] = dforig[cols].round(3)
            # dfano[cols] = dfano[cols].round(3)
            
        # Merge by date & GPS
        merged_df = dfano.merge(dforig, how = 'inner', on = ['date', 'lat', 'long']).set_index('id')
        
        # Drop columns of GPS
        merged_df = merged_df.drop(columns=['lat', 'long'])
        m = merged_df.groupby(['id','pseudo_id']).count().reset_index()
        m['pseudo_id'] = m['pseudo_id'].astype('str')

        # Assign guesses for each id 
        for id in ids:
            Guesses[str(id)][str(week)] = (m[m['id'] == id])['pseudo_id'].values.tolist()

    ## Convert Guesses to a json File
    with open(path_json,'w') as f:
        json.dump(Guesses, f)

    print('--> END Reidentification ')

counter(path_orig,path_ano)
