from datetime import datetime , timedelta 
import pandas as pd
from numpy import random
import random

# To disable safely the warning
pd.options.mode.chained_assignment = None  # default='warn'

# PATH of Original Database
path_orig = "INSAnonym/c3465dad3864bb1e373891fdcfbfcca5f974db6a9e0b646584e07c5f554d7df7"

# PATH of Anonymized Database
path_ano = 'DB/soumission_3.csv'

def anonymize_DB():

    ### Read Database

    df = pd.read_csv(path_orig, sep='\t', header=None).set_axis(['id', 'Date', 'longitude', 'lattitude'], axis=1, inplace=False)
    print('--> Reading File : Passed ')

    df['Date'] = pd.to_datetime(df['Date'], dayfirst=True, errors='coerce')

    ### Generate Weeks

    df['Week'] = df['Date'].dt.strftime('%Y-%W')
    print('--> Generating Weeks : Passed ')

    df_groupe_by = df[['id', 'Week']]

    tmp = 0
    for i in df['id']:
        ### Delete or Hash id
        if (tmp % 10 == 0):
            ## Choose 2 ids to delete
            ids_Del = random.sample(range(tmp, tmp+9), 2)
        if (tmp in ids_Del):
            ## Delete id ( id = DEL)
            df_groupe_by['id'][tmp] = "DEL"
        else:
            ## Hash id : Generate pseudo_id
            chaine_à_crypter = str(i)+"_*_"+str(df['Week'][tmp])
            df_groupe_by['id'][tmp] = hash(str(chaine_à_crypter).encode("utf-8")) % 10**5
    
        ### Disturb Data

        ## Disturb days
        # Positions : [0 , 6]
        pos = df['Date'][tmp].dayofweek
        if (pos < 3):
            # Workdays
            df['Date'][tmp] = df['Date'][tmp] + timedelta(days=1)
        elif (pos == 4 or pos == 6):
            # Friday & Sunday
            df['Date'][tmp] = df['Date'][tmp] - timedelta(days=1)

        ## Disturb Hours & Seconds
        actual_hour = df['Date'][tmp].hour
        # Night [00h , 06h]
        if(actual_hour in (0, 2)):
            df['Date'][tmp] = df['Date'][tmp] + timedelta(seconds= random.randint(0, 29), hours=random.randint(1, 3))
        elif(actual_hour in (3, 6)):
            df['Date'][tmp] = df['Date'][tmp] - timedelta(seconds=random.randint(0, 29), hours=random.randint(1, 3))
        if (pos > 4):
            # Weekeend [10h , 18h]
            if(actual_hour in (10, 14)):
                df['Date'][tmp] = df['Date'][tmp] + timedelta(seconds=random.randint(0, 29), hours=random.randint(1, 3))
            elif(actual_hour in (15, 18)):
                df['Date'][tmp] = df['Date'][tmp] - timedelta(seconds=random.randint(0, 29), hours=random.randint(1, 3))
        else:
            # Work [09h , 16h]
            if(actual_hour in (9, 12)):
                df['Date'][tmp] = df['Date'][tmp] + timedelta(seconds=random.randint(0, 29), hours=random.randint(1, 3))
            elif(actual_hour in (13, 16)):
                df['Date'][tmp] = df['Date'][tmp] - timedelta(seconds=random.randint(0, 29), hours=random.randint(1, 3))
        
        ## Disturb GPS
        
        df['longitude'][tmp] += float(format(random.uniform(0.0001, 0.0006), ".6f"))
        df['lattitude'][tmp] += float(format(random.uniform(0.0001, 0.0006), ".6f"))

        tmp += 1
    
    print('--> Disturbing Data : Passed')

    df['id'] = df_groupe_by['id']
    df = df.drop(columns=['Week'])

    # Generate Anonymized Database
    df.to_csv(path_ano, sep='\t', header=False, index=False)
    print('--> END Anonymisation')

anonymize_DB()
