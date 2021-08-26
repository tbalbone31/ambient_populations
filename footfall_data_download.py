from bs4 import BeautifulSoup  # requirement beautifulsoup4
from urllib.request import (
    urlopen, urlretrieve)
import os, os.path
import sys
import pandas as pd

def csv_check(soup):
    for link in soup.find_all('a'):
        # print("\n****",link,"****\n")
        url = link.get('href')
        if url == None:  # if no 'href' tag
            continue

        if url.endswith(".csv"):
            filename = url.strip().split("/")[-1]  # File is last part of the url

            # For some reason some files are duplicated - DEPRECATED CODE UNLESS ISSUES CROP UP
            # if (filename.startswith("Copy") or filename.startswith("copy")) and not filename.endswith(
            # tuple(legit_copy_suff)):
            # files_with_copy.append(filename)
            # continue
            # And we don't care about xmas analysis
            if filename.startswith("Christ"):
                continue

            # Save the csv file (unless it already exists already)
            full_path = os.path.join("data/lcc_footfall", filename)
            if os.path.isfile(full_path):
                continue
            else:
                csv_url = "https://datamillnorth.org/" + url
                data = pd.read_csv(csv_url)
                data.to_csv(full_path)

def download_data(datadir):
    if not os.path.isdir(datadir):
        os.makedirs(datadir)

    # Connect to the data Mill North page and parse the html
    root = 'https://datamillnorth.org/dataset/leeds-city-centre-footfall-data'
    soup = BeautifulSoup(urlopen(root), 'html.parser')

    # Iterate over all links and see which are csv files
    csv_check(soup)

def create_template_df():
    templatedf = pd.DataFrame(columns=["Location", "Date", "Hour", "Count", "DateTime", "FileName"])
    templatedf["Date"] = pd.to_datetime(templatedf["Date"], dayfirst=True)
    templatedf["Hour"] = pd.to_numeric(templatedf["Hour"])
    templatedf["Count"] = pd.to_numeric(templatedf["Count"])
    templatedf["DateTime"] = pd.to_datetime(templatedf["DateTime"])
    return templatedf

def import_data(datadir):
    template = create_template_df()

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
                date_col = "Date"  # Doesn't change
                count_col = "Count" if "Count" in df.columns else "InCount"  # Two options
                hour_col = "Hour"
                loc_col = "Location" if "Location" in df.columns else "LocationName"
                BRCYear_col = "BRCYear" if "BRCYear" in df.columns else "Year"
                BRCMonth_col = "BRCMonthName" if "BRCMonthName" in df.columns else "Month"
                BRCWeek_col = "BRCWeekNum" if "BRCWeekNum" in df.columns else "WeekNum" if "WeekNum" in df.columns else "BRCWeek"

                if False in [date_col in df.columns, count_col in df.columns, hour_col in df.columns,
                             loc_col in df.columns, BRCYear_col in df.columns, BRCMonth_col in df.columns,
                             BRCWeek_col in df.columns]:
                    raise Exception("File '{}' is missing a column. Date? {}, Count? {}, Hour? {}, Location? {}, "
                                    "BRCYear? {}, BRCMonthName? {}, BRCWeekNum? {}".
                                    format(filename, date_col in df.columns, count_col in df.columns,
                                           hour_col in df.columns, loc_col in df.columns, BRCYear_col in df.columns,
                                           BRCMonth_col in df.columns, BRCWeek_col in df.columns))

                # Check if any of the columns have nans
                bad_cols = []
                for x in [date_col, count_col, hour_col, loc_col, BRCYear_col, BRCMonth_col, BRCWeek_col]:
                    if True in df[x].isnull().values:
                        bad_cols.append(x)
                if len(bad_cols) > 0:
                    failures.append(filename)
                    print(f"File {filename} has nans in the following columns: '{str(bad_cols)}'. Ignoring at initial pass, check data download script for additional processing")
                    continue

                # Create Series' that will represent each column
                dates = pd.to_datetime(df[date_col], dayfirst=True)
                counts = pd.to_numeric(df[count_col])
                hours = convert_hour(df[hour_col])  # Hours can come in different forms
                locs = df[loc_col]
                brcweek = pd.to_numeric(df[BRCWeek_col], downcast='integer')
                brcmonth = df[BRCMonth_col]
                brcyear = pd.to_numeric(df[BRCYear_col], downcast='integer')

                # Strip whitespace from the locations
                locs = locs.apply(lambda row: row.strip())

                # Derive a proper date from the date and hour
                # (Almost certainly a more efficient way to do this using 'apply' or whatever)
                dt = pd.to_datetime(pd.Series(data=[date.replace(hour=hour) for date, hour in zip(dates, hours)]))

                # Also useful to have the filename
                fnames = [filename for _ in range(len(df))]

                # df.apply(lambda x: x[date_col].replace(hour = x[hour_col]), axis=1)

                if False in [len(df) == len(x) for x in [dates, counts, hours, locs, dt, brcyear, brcmonth, brcweek]]:
                    raise Exception("One of the dataframe columns does not have enough values")
                total_rows += len(df)

                # Create a temporary dataframe to represent the information in that file.
                # Note that consistent column names (defined above) are used
                frames.append(pd.DataFrame(data=
                                           {"Location": locs, "Date": dates, "Hour": hours,
                                            "Count": counts, "DateTime": dt, "BRCWeekNum": brcweek,
                                            "BRCMonth": brcmonth, "BRCYear": brcyear, "FileName": fnames}))

            except Exception as e:
                print("Caught exception on file {}".format(filename))
                raise e

    # Finally megre the frames into one big one
    merged_frames = pd.concat(frames)
    if total_rows != len(merged_frames):
        raise Exception(f"The number of rows in the individual files {total_rows} does \
    not match those in the final dataframe {len(merged_frames)}.")

    footfall_data = template.append(merged_frames)
    return footfall_data

def convert_hour(series):
    """Assumes the given series represents hours. Works out if they're
    integers or in the format '03:00:00' and returns them as integers"""

    # If it's a number then just return it
    if isinstance(series.values[0], np.int64):
        return series

    if isinstance(series.values[0], np.float64) or isinstance(
            series.values[0], float):
        return series.astype(dtype=int)

    # If it's a string see if it can be made into a number
    try:
        int(series.values[0])
        return pd.to_numeric(series)
    except:  # If get here then it couldn't be made into an integer
        pass

    if ":" in series.values[0]:
        return pd.to_numeric(series.apply(lambda x: x.strip().split(":")[0]))

    # If here then I don't know what to do.
    raise Exception("Unrecognised type of hours: {}".format(series))

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

footfalldf_imported['Location'] = footfalldf_imported['Location'].str.strip()


footfalldf_imported.to_csv("data/LCC_footfall_2021.csv",index=False)
footfalldf_imported.to_csv("data/LCC_footfall_2021.gz",compression="gzip", index=False)

