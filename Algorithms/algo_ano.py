import csv
import pandas as pd
import numpy as np
from pandas.api.types import is_string_dtype
from pandas.api.types import is_numeric_dtype
import hashlib
import datetime

# To disable safely the warning
pd.options.mode.chained_assignment = None  # default='warn'


def read():

    ######## Read from CSV's file

    # if there is a header : header=0    else:   header=None
    df = pd.read_csv('DB/base_org.csv', sep=',', header=None).set_axis(
        ['Id', 'Date', 'longitude', 'lattitude'], axis=1, inplace=False)

    ######## Create Column of Week : Date to Year-week

    # if day<13 => day and month switched  Solution => dayfirst=True
    df['Date'] = pd.to_datetime(df['Date'], dayfirst=True, errors='coerce')
    df['Week'] = df['Date'].dt.strftime('%Y/%U')

    ######## Create Pseudo-Id

    # Create new dataframe with 2 columns
    df_groupe_by = df[['Id', 'Week']]

    # Replace the Id by the new pseudo-Id (Hash)
    tmp = 0
    for i in df['Id']:
        chaine_à_crypter = str(i)+"_*_"+str(df['Week'][tmp])
        a = str(chaine_à_crypter).encode("utf-8")
        df_groupe_by['Id'][tmp] = hash(a) % 10**5
        tmp += 1

    df['Id'] = df_groupe_by['Id']
    df = df.drop(columns=['Week'])

    ######## Disturb GPS & Date OR Delete lines

    #df['Date'] = pd.to_datetime(df['Date'], dayfirst=True, errors='coerce')

    tmp = 0
    for i in df['Id']:
        first_flip = np.random.randint(0, 2)
        if(first_flip == 1):
            second_flip = np.random.randint(0, 2)
            if(second_flip == 0):
                ### Disturb GPS & Date if only f1==1 & f2==0

                ## Disturb GPS
                tmp_value = float(format(np.random.uniform(0.01, 0.04), ".6f"))
                df['longitude'][tmp] = df['longitude'][tmp]+tmp_value
                tmp_value = float(format(np.random.uniform(0.01, 0.06), ".6f"))
                df['lattitude'][tmp] = df['lattitude'][tmp]+tmp_value

                ## Disturb Date
                pos = (df['Date'][tmp].dayofweek + 4) % 7
                if (pos < 3):
                    df['Date'][tmp] = df['Date'][tmp] + \
                        datetime.timedelta(days=4)
                else:
                    df['Date'][tmp] = df['Date'][tmp] - \
                        datetime.timedelta(days=3)

            else:
                ### Delete lines if only f1 == 1 & f2 != 0

                random_ = np.random.randint(0, 3)
                if (random_ == 2):
                    ## DEL values
                    df['Id'][tmp] = "DEL"
                    df['Date'][tmp] = "DEL"
                    df['longitude'][tmp] = "DEL"
                    df['lattitude'][tmp] = "DEL"
        tmp += 1

    ######## Reset the format of Date
    tmp = 0
    for i in df['Id']:
        if (df['Date'][tmp] != "DEL"):
            df['Date'][tmp] = df['Date'][tmp].strftime('%d/%m/%Y %H:%M')
        tmp += 1

    ######## Shuffle lines
    #df = pd.DataFrame(np.random.permutation(df)).set_axis(['Id', 'Date','longitude', 'lattitude'], axis=1, inplace=False)

    ######## Generate a new csv file without Header & Index
    df.to_csv('DB/base_ano.csv', header=False, index=False)

    # Print all rows & Cols of Dataframe
    pd.set_option("display.max_rows", None, "display.max_columns", None)

    print(df)


read()
