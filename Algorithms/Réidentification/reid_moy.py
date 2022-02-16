import csv 
import pandas as pd 
import numpy as np
import json
from math import *
from pandas.core.dtypes.missing import isnull, notnull

Guesses_final = {}
Deleted = {}


def main(df_orig, df_anon):
    # Read from CSV's file  anonymisé
    dfAnon = pd.read_csv(df_anon, sep="\t", header=None).set_axis(['QId', 'Date', 'longitude', 'lattitude'], axis=1, inplace=False)
    dfAnon = dfAnon[dfAnon['QId']!='DEL']
    # Read from CSV's file  original
    dfOrig = pd.read_csv(df_orig, sep="\t", header=None).set_axis(['Id', 'Date', 'longitude', 'lattitude'], axis=1, inplace=False)
    dfOrig['Date'] = pd.to_datetime(dfOrig['Date'], dayfirst=True, errors='coerce')
    dfOrig['Week'] = dfOrig['Date'].dt.strftime('%Y-%W')

    

    dfAnon['Date'] = pd.to_datetime(dfAnon['Date'], dayfirst=True, errors='coerce')
    dfAnon['Week'] = dfAnon['Date'].dt.strftime('%Y-%W')
    #fin read

    
    ids, weeks = dfOrig['Id'].unique() , dfOrig['Week'].unique()
    #DelCount(dfOrig,dfAnon)
    for id in ids:
        Guesses_final[str(id)]={}
        for week in weeks:
            Guesses_final[str(id)][week]=[]

    #new ajout
    for week in weeks:
         dfOrig[dfOrig['Week'] == week].to_csv(f'./bases_org/week_{week}.csv', header=False, index=False,sep='\t')
         dfAnon[dfAnon['Week'] == week].to_csv(f'./bases_ano/week_{week}.csv', header=False, index=False,sep='\t')
         Deleted[week] = 0
    
    

    for week in weeks:
        dforig = pd.read_csv(f'./bases_org/week_{week}.csv', sep='\t', names=['Id', 'Date', 'longitude', 'lattitude','Week'])
        dfano = pd.read_csv(f'./bases_ano/week_{week}.csv', sep='\t', names=['QId', 'Date', 'longitude', 'lattitude','Week'])
        Deleted[week] = len(dforig)-len(dfano)
        if Deleted[week] == 0: 
            Guesses = GuessIfDelNul(week,dforig,dfano)
            for key in Guesses:
                Guesses_final[str(key)][week].extend(Guesses[key]) 
        else:
            Guesses = GuessIfDelNotNul(week,dforig,dfano) 
            for key in Guesses:
                Guesses_final[str(key)][week].extend(Guesses[key])

    for key in Guesses_final:
        for week in Guesses_final[key]:
            if len(Guesses_final[key][week])==0:
                Guesses_final[key][week].append("DEL")

    with open('./guesses.json','w') as f:
        json.dump(Guesses_final,f)

def GuessIfDelNotNul(week,dfOrig, dfAnon):
    #declarer dictionnaire (Maps: key->value)
    OccurAnon = {}
    OccurOrig = {}

    k=0
    moyenneLongitudeId = {}
    maxLongitudeId = {}
    moyenneLatitudeId = {}
    maxLatitudeId = {}
    #initialiser la var par le premier id de la liste
    dfOrig = dfOrig.sort_values(by=['Id'])
    j=0
    max = 0
    
    wee = {}
    wee[week]={}
    #orig
    #sort by Id
    for index, row in dfOrig.iterrows():
            #calculer moyenne et le max for chaque id
            if j==0:
                var = row['Id']
                moyenneLatitudeId[var]=0
                moyenneLongitudeId[var]=0
                maxLongitudeId[var]=0
                maxLatitudeId[var]=0
                j += 1
            if row['Id'] != var :
                moyenneLatitudeId[var] = moyenneLatitudeId[var] / k
                moyenneLongitudeId[var] = moyenneLongitudeId[var] / k
                k=0
                moyenneLatitudeId[row['Id']]=0
                moyenneLongitudeId[row['Id']]=0
                maxLongitudeId[row['Id']] = 0
                maxLatitudeId[row['Id']] = 0
            if row['Id'] not in maxLongitudeId.keys():
                maxLongitudeId[row['Id']] = 0
                maxLatitudeId[row['Id']] = 0

            if maxLongitudeId[row['Id']]  < row['longitude']:
                maxLongitudeId[row['Id']] = row['longitude']
            if maxLatitudeId[row['Id']] < row['lattitude']:
                maxLatitudeId[row['Id']] = row['lattitude']
            k += 1
            #calculer la moyenne des latitudes et longitudes de chaque id (pour la comparer à celle du qid +- la difference entre la moyenne et la min de l'id)
            if row['Id'] in moyenneLatitudeId.keys():
                moyenneLatitudeId[row['Id']] = moyenneLatitudeId[row['Id']] + row['lattitude']
                moyenneLongitudeId[row['Id']] = moyenneLongitudeId[row['Id']] + row['longitude']
            var = row['Id']

    #le cas du dernier id qui 799 la condition du week , kikon howa dernier element du dictionnaire vu qu'il est sorté
    r = list(moyenneLongitudeId)


    k=0
    moyenneLongitudeQId = {}
    moyenneLatitudeQId = {}
    #initialiser la var par le premier id de la liste
    dfAnon = dfAnon.sort_values(by=['QId'])
    var = dfOrig['Id'][0] 
    j=0

    #anon
    #sort by QId
    for index, row in dfAnon.iterrows():
        if str(row['Week'])==week :
            if j==0:
                var = row['QId']
                moyenneLatitudeQId[var]=0
                moyenneLongitudeQId[var]=0
                j += 1
            if row['QId'] != var :
                moyenneLatitudeQId[var] = moyenneLatitudeQId[var] / k
                moyenneLongitudeQId[var] = moyenneLongitudeQId[var] / k
                k=0
                moyenneLatitudeQId[row['QId']]=0
                moyenneLongitudeQId[row['QId']]=0
            
            k += 1
            #calculer la moyenne des latitudes et longitudes de chaque id (pour la comparer à celle du qid +- la difference entre la moyenne et la min de l'id)
            if row['QId'] in moyenneLongitudeQId.keys():
                moyenneLatitudeQId[row['QId']] = moyenneLatitudeQId[row['QId']] + float(row['lattitude'])
                moyenneLongitudeQId[row['QId']] = moyenneLongitudeQId[row['QId']] + float(row['longitude'])
            var = row['QId']

    #le cas du dernier id qui 799 la condition du week , kikon howa dernier element du dictionnaire vu qu'il est sorté
    r = list(moyenneLongitudeQId)
    if len(r) != 0:
        moyenneLongitudeQId[r[len(moyenneLongitudeQId)-1]] = moyenneLongitudeQId[r[len(moyenneLongitudeQId)-1]] / k


    #orig
    for index, row in dfOrig.iterrows():
        if str(row['Week'])==week :
            if row['Id'] in OccurOrig :
                OccurOrig[row['Id']] = 1 + OccurOrig[row['Id']]
            else:
                OccurOrig[row['Id']] = 1

    #anon
    #sort by QId
    for index, row in dfAnon.iterrows():
        if str(row['Week'])==week :
            if row['QId'] in OccurAnon:
                OccurAnon[row['QId']] = 1 + OccurAnon[row['QId']]
            else:
                OccurAnon[row['QId']] = 1

    #print("********** GUESSES ***********")
    Guesses = {}                                
    #nbrDelThisWeek, ids , weeks = DelCount(df_orig,df_anon)
    for id_ , occur_id in OccurOrig.items():
        listeGuesses = []
        Guesses[id_]=listeGuesses
        for qid_ , occur_qid in OccurAnon.items():
            if(occur_id >= occur_qid and Deleted[week]+occur_qid >= occur_id):
                if moyenneLongitudeQId[qid_]-abs(maxLongitudeId[id_]-moyenneLongitudeId[id_])-0.001 <= moyenneLongitudeId[id_] <= moyenneLongitudeQId[qid_]+abs(maxLongitudeId[id_]-moyenneLongitudeId[id_]) + 0.001 :
                    Guesses[id_].append(qid_)

    return Guesses

def GuessIfDelNul(week,dfOrig, dfAnon):
    
    #declarer dictionnaire (Maps: key->value)
    OccurAnon = {}
    OccurOrig = {}


    wee = {}
    wee[week]={}
    #orig
    for index, row in dfOrig.iterrows():
            if row['Id'] in OccurOrig :
                OccurOrig[row['Id']] = 1 + OccurOrig[row['Id']]
            else:
                OccurOrig[row['Id']] = 1
    
    #anon
    for index, row in dfAnon.iterrows():
        if row['QId'] in OccurAnon:
            OccurAnon[row['QId']] = 1 + OccurAnon[row['QId']]
        else:
            OccurAnon[row['QId']] = 1

    Guesses = {}
    
    for id_ , occur_id in OccurOrig.items():
        Guesses[id_]=[]
        for qid_ , occur_qid in OccurAnon.items():
            if(occur_id==occur_qid):
                Guesses[id_].append(qid_)
    return Guesses

def count_qids(df_Anon):
    # Read from CSV's file 
    df = pd.read_csv(df_Anon, sep="\t", header=None).set_axis(['QId', 'Date', 'longitude', 'lattitude'], axis=1, inplace=False)
    df = df[df['QId']!='DEL']
    #declarer dictionnaire (Maps: key->value)
    Dict = {}

    # Transform : Date to Year-week 
    df['Date'] = pd.to_datetime(df['Date'], errors='coerce', dayfirst=True)
    df['Week'] = df['Date'].dt.strftime('%Y-%W')

    #calculer pour chaque week-id nbr d'occurances
    for index, row in df.iterrows():
        if str(row['Week'])+'-'+str(row['QID']) in Dict:
            Dict[str(row['Week'])+'-'+str(row['QID'])] = 1 + Dict[str(row['Week'])+'-'+str(row['QID'])]
        else:
            Dict[str(row['Week'])+'-'+str(row['QID'])] = 1





def count_ids(df_Orig):
    # Read from CSV's file 
    df = pd.read_csv(df_Orig, sep="\t", header=None).set_axis(['Id', 'Date', 'longitude', 'lattitude'], axis=1, inplace=False)

    # Transform : Date to Year-week 
    df['Date'] = pd.to_datetime(df['Date'], errors='coerce')
    df['Week'] = df['Date'].dt.strftime('%Y/%W')

    #declarer dictionnaire (Maps: key->value)
    Dict = {}

    """
        key -> Value 
        week-id  -> nbr de fois
        2015-11-2 -> 3
    """
    #calculer pour chaque week-id nbr d'occurances
    for index, row in df.iterrows():
        if str(row['Week'])+'-'+str(row['Id']) in Dict:
            Dict[str(row['Week'])+'-'+str(row['Id'])] = 1 + Dict[str(row['Week'])+'-'+str(row['Id'])]
        else:
            Dict[str(row['Week'])+'-'+str(row['Id'])] = 1  


main('../DB/V_2/base_org21.csv','../DB/V_2/base_ano21.csv')