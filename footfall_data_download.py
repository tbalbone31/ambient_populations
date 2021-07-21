from bs4 import BeautifulSoup  # requirement beautifulsoup4
from urllib.request import (
    urlopen, urlretrieve)
import os, os.path
import sys
import pandas as pd


from source import *

# The following code connect to the data Mill North page, parse the html and process the csv files uploaded by LSC.  It is adapted from Nic Malleson's initial exploration of the data found at https://github.com/Urban-Analytics/dust/blob/main/Projects/Ambient_Populations/AmbientPopulations.ipynb.
# There are various checks to ensure duplicate files are not downloaded and merged into the final dataframe.  Initially the code included a check on the filename to filter out anything that started with 'Copy of', however after visualising the data I discovered that a lot of the data was missing from
# earlier years (mostly 2015-2017) as many of the files had been named 'Copy of....' yet were not duplicates.  The code already ensures files that exist are not downloaded and I've gone through and eyeballed the files to do a sense check of whether duplicates exist or not.

#set data directory
data_dir = "data/lcc_footfall"

#Function to parse the html and download the csv files to specified location
download_data(data_dir)

#import data and output to a merged csv
footfalldf_imported = import_data(data_dir)

importlist = ['Monthly%20Data%20Feed-April%202017%20-%2020170510.csv',
            'Copy%20of%20Monthly%20Data%20Feed-November%202016%20-%2020161221.csv']



for file in importlist:
    df = pd.read_csv(f"data/lcc_footfall/{file}",
                                  parse_dates=['Date'],
                                  #dtype={"BRCYear": int,"BRCWeekNum":int},
                                  index_col=[0])

    df = df.rename(columns={'BRCWeek':'BRCWeekNum','DayOfWeek':'DayName','BRCMonthName':'BRCMonth','InCount':'Count'})
    df = df.dropna(subset=['Hour'])
    df['FileName'] = file
    df['Hour'] = convert_hour(df['Hour'])
    df['Hour'] = df['Hour'].astype(int)
    df['DateTime'] = pd.to_datetime(pd.Series(data=[date.replace(hour=hour) for date,hour in zip(df.Date,df.Hour)]))
    footfalldf_imported = pd.concat([footfalldf_imported,df])


footfalldf_imported = footfalldf_imported.loc[:,'Location':'BRCYear']


footfalldf_imported.to_csv("data/footfall_merged.csv",index=False)
footfalldf_imported.to_csv("data/footfall_merged.csv.gz",compression="gzip")

