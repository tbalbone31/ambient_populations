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
print(footfalldf_imported.dtypes)
#footfalldf_imported.to_csv("./data/footfall_merged.csv",index=False)
footfalldf_imported.to_csv("data/footfall_merged.csv.gz",compression="gzip")
