import pandas as pd
import numpy as np
import os, os.path
import sys
from bs4 import BeautifulSoup  # requirement beautifulsoup4
from urllib.request import (
    urlopen, urlretrieve)
import plotly.express as px
import datetime

datadir = "data/weather"

if not os.path.isdir(datadir):
    os.makedirs(datadir)

# Connect to the ncas weather page and parse the html
root = 'https://sci.ncas.ac.uk/leedsweather/Archive/'
soup = BeautifulSoup(urlopen(root), 'html.parser')

# Iterate over all links and see which are csv files

for link in soup.find_all('a'):
    # print("\n****",link,"****\n")
    url = link.get('href')
    if url == None:  # if no 'href' tag
        continue

    if url.endswith("METRIC.csv"):
        filename = url.strip().split("/")[-1]  # File is last part of the url

        # For some reason some files are duplicated - DEPRECATED CODE UNLESS ISSUES CROP UP
        # if (filename.startswith("Copy") or filename.startswith("copy")) and not filename.endswith(
        # tuple(legit_copy_suff)):
        # files_with_copy.append(filename)
        # continue

        # Save the csv file (unless it already exists already)
        full_path = os.path.join("data/weather", filename)
        if os.path.isfile(full_path):
            continue
        else:
            csv_url = "https://sci.ncas.ac.uk/leedsweather/Archive/" + url
            data = pd.read_csv(csv_url)
            data.to_csv(full_path)

template = pd.DataFrame(columns=["timestamp", "temp_°C", "wind_ms¯¹", "rain_mm", "FileName"])
template["timestamp"] = pd.to_datetime(template["timestamp"], dayfirst=True)
template["temp_°C"] = pd.to_numeric(template["temp_°C"])
template["wind_ms¯¹"] = pd.to_numeric(template["wind_ms¯¹"])
template["rain_mm"] = pd.to_numeric(template["rain_mm"])

frames = []  # Build up a load of dataframes then merge them
total_rows = 0  # For checking that the merge works
files = []  # Remember the names of the files we tried to analyse
failures = []  # Remember which ones didn't work

# Read the files in
for filename in os.listdir(datadir):
    if filename.endswith(".csv"):
        try:
            # print(filename)
            files.append(filename)
            df = pd.read_csv(os.path.join(datadir, filename))

            # Check the file has the columns that we need, and work out what the column names are for this file (annoyingly it changes)
            timestamp_col = "Timestamp (UTC)"  # Doesn't change
            temp_col = "Temp / °C"
            wind_col = "Wind / ms¯¹"
            rain_col = "Rain / mm"

            if False in [timestamp_col in df.columns, temp_col in df.columns, wind_col in df.columns,
                         rain_col in df.columns]:
                raise Exception("File '{}' is missing a column. timestamp? {}, temperature? {}, wind? {}, rain? {}".
                                format(filename, timestamp_col in df.columns, temp_col in df.columns,
                                       wind_col in df.columns, rain_col in df.columns))

            # Check if any of the columns have nans
            bad_cols = []
            for x in [timestamp_col,temp_col,wind_col,rain_col]:
                if True in df[x].isnull().values:
                    bad_cols.append(x)
            if len(bad_cols) > 0:
                failures.append(filename)
                print(f"File {filename} has nans in the following columns: '{str(bad_cols)}'. Ignoring at initial pass, check data download script for additional processing")
                continue

            # Create Series' that will represent each column
            timestamp = pd.to_datetime(df[timestamp_col], dayfirst=True)
            temp = pd.to_numeric(df[temp_col])
            wind = pd.to_numeric(df[wind_col])  # Hours can come in different forms
            rain = pd.to_numeric(df[rain_col])


            # Also useful to have the filename
            fnames = [filename for _ in range(len(df))]

            if False in [len(df) == len(x) for x in [timestamp, temp, wind, rain]]:
                raise Exception("One of the dataframe columns does not have enough values")
            total_rows += len(df)

            # Create a temporary dataframe to represent the information in that file.
            # Note that consistent column names (defined above) are used
            frames.append(pd.DataFrame(data=
                                       {"timestamp": timestamp, "temp_°C": temp, "wind_ms¯¹": wind,
                                        "rain_mm": rain, 'FileName': fnames}))

        except Exception as e:
            print("Caught exception on file {}".format(filename))
            raise e

# Finally megre the frames into one big one
merged_frames = pd.concat(frames)
if total_rows != len(merged_frames):
    raise Exception(f"The number of rows in the individual files {total_rows} does \
    not match those in the final dataframe {len(merged_frames)}.")

weatherdata = template.append(merged_frames)

weatherdata.to_csv("./data/weatherdata.csv",index=False)