import pandas as pd
import numpy as np
import os, os.path
import sys
from bs4 import BeautifulSoup  # requirement beautifulsoup4
from urllib.request import (
    urlopen, urlretrieve)


def start_pipeline(dataf):
    return dataf.copy()


def convert_hour(series):
    """Assumes the given series represents hours. Works out if they're
    integers or in the format '03:00:00' and returns them as integers"""

    # If it's a number then just return it
    if isinstance(series.values[0], np.int64) or isinstance(series.values[0], np.float64) or isinstance(
            series.values[0], float):
        return series

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
            full_path = os.path.join("./data/lcc_footfall", filename)
            if os.path.isfile(full_path):
                continue
            else:
                csv_url = "https://datamillnorth.org/" + url
                data = pd.read_csv(csv_url)
                data.to_csv(full_path)


def create_template_df():
    templatedf = pd.DataFrame(columns=["Location", "Date", "Hour", "Count", "DateTime", "FileName"])
    templatedf["Date"] = pd.to_datetime(templatedf["Date"], dayfirst=True)
    templatedf["Hour"] = pd.to_numeric(templatedf["Hour"])
    templatedf["Count"] = pd.to_numeric(templatedf["Count"])
    templatedf["DateTime"] = pd.to_datetime(templatedf["DateTime"])
    return templatedf


def download_data(datadir):
    if not os.path.isdir(datadir):
        os.makedirs(datadir)

    # Connect to the Data Mill North page and parse the html
    root = 'https://datamillnorth.org/dataset/leeds-city-centre-footfall-data'
    soup = BeautifulSoup(urlopen(root), 'html.parser')

    # Iterate over all links and see which are csv files
    csv_check(soup)


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
                    print(f"File {filename} has nans in the following columns: '{str(bad_cols)}'. Ignoring it")
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

def create_BRC_MonthNum(dataf):
    conditions = [
        dataf['BRCMonth'] == "January",dataf['BRCMonth'] == "February",dataf['BRCMonth'] == "March",
        dataf['BRCMonth'] == "April",dataf['BRCMonth'] == "May",dataf['BRCMonth'] == "June",
        dataf['BRCMonth'] == "July",dataf['BRCMonth'] == "August",dataf['BRCMonth'] == "September",
        dataf['BRCMonth'] == "October",dataf['BRCMonth'] == "November",dataf['BRCMonth'] == "December"
    ]

    outputs = [i for i in range(1, 13)]

    dataf['BRCMonthNum'] = np.select(conditions, outputs)

    return dataf


def check_remove_dup(dataf):
    # Groups footfall data by location and datetime, counting the number of occurrences by calling size.
    # Also Resets the index to restore the grouped columns and renames the size column to UniqueRowsCount
    unq_loc_datetime = dataf.groupby(
        ['Location', 'DateTime']).size().reset_index().rename(columns={0: 'UniqueRowsCount'})
    unq_loc_datetime

    # Check to see if there are any values in the UniqueRowsCount column greater than one (indicating there are
    # duplicate rows)
    if len(unq_loc_datetime[unq_loc_datetime.UniqueRowsCount > 1]) > 1:
        # Drop duplicates from dataframe (amended from initial code to concentrate only on Location and DateTime).
        ffd_no_dup = dataf.drop_duplicates(subset=['Location', 'DateTime'])
        # Rerun duplicate check and print to console.
        unq_loc_datetime = ffd_no_dup.groupby(
            ['Location', 'DateTime']).size().reset_index().rename(columns={0: 'UniqueRowsCount'})
        print(f"There are {len(unq_loc_datetime[unq_loc_datetime.UniqueRowsCount > 1])} duplicates left")
    return ffd_no_dup


def time_dico():
    time_dico = {
        "interval": ["hours", "day", "week", "month", "year"],
        "code": ["%H", "%a", "%W", "%b", "%y"],
        "freq": ["H", "D", "W", "MS", "Y"]
    }
    return time_dico


def resample_day(data):

    data = data.resample("D").sum()
    data['weekday'] = data.index.dayofweek
    data['weekdayname'] = data.index.day_name()
    data = data.groupby(['weekday', 'weekdayname'])['Count'].agg(['sum', 'mean']).droplevel(level=0)

    return data


def resample_week(data):

    data = data.groupby(['BRCWeekNum'])['Count'].sum()

    return data


def resample_month(data):

    data = data.groupby(['BRCMonth'])['Count'].sum()

    return data


def resample_year(data):

    data = data.groupby(['BRCYear'])['Count'].sum()

    return data


def invalid_op(data):
    raise Exception("Invalid Time Frequency - Needs either 'day', 'week', 'month' or 'year'.")


#def mean_hourly(dataf, freq, *year):
#    if freq == "month":
 #       dataf = dataf.set_index('DateTime').groupby(
  #          [pd.Grouper(level="DateTime",
   #                     freq="MS")]).aggregate(np.mean).rename(columns={'Count': 'Mean Hourly Footfall'})
    #elif freq == "week":
     #   dataf = dataf.set_index('DateTime').groupby(
      #      [pd.Grouper(level="DateTime",
       #                 freq="W")]).aggregate(np.mean).rename(columns={'Count': 'Mean Hourly Footfall'})
    #elif freq == "day":
     #   dataf = dataf.set_index('DateTime').groupby(
      #      [pd.Grouper(level="DateTime",
       #                 freq="D")]).aggregate(np.mean).rename(columns={'Count': 'Mean Hourly Footfall'})

   # if year:
    #    dataf = dataf.loc[dataf['BRCYear'] == year]

    #return dataf


def mean_hourly(dataf, freq, *year):

    if year:
        dataf = dataf.loc[dataf.BRCYear == year]

    if freq == "day":
        dataf = dataf.groupby([
            pd.Grouper(key="DateTime",freq="D"),'BRCWeekNum','BRCMonth'])['Count'].aggregate(np.mean)
    elif freq == "month":
        dataf = dataf.groupby(
            ['BRCMonthNum',pd.Grouper(key="BRCMonth"),'BRCYear'])['Count'].aggregate(np.mean).reset_index()
    elif freq == "week":
        dataf = dataf.set_index('DateTime').groupby(
            [pd.Grouper(key="BRCWeekNum")])['Count'].aggregate(np.mean)
    elif freq == "year":
        dataf = dataf.set_index('DateTime').groupby(
            [pd.Grouper(key="BRCYear")])['Count'].aggregate(np.mean)

    return dataf

def reset_df_index(dataf):
    dataf = dataf.reset_index()
    return dataf

def set_dt_index(dataf):
    dataf = dataf.set_index('DateTime')

    return dataf

def date_range(dataf,startdate,enddate):

    dataf = dataf.set_index('DateTime')
    dataf = dataf[(dataf.index >= startdate) & (dataf.index <= enddate)]

    return dataf

def per_change(dataf,freq):

    dataf[f'{freq}_per_change'] = dataf.Count.pct_change() * 100

    return dataf

def create_sum_df(data, time, year):
    freq = {
        "day": resample_day,
        "week": resample_week,
        "month": resample_month,
        "year": resample_year
    }

    data = data.set_index('DateTime')

    if year != "none":
        data = data.loc[data.BRCYear == year]

    resample_function = freq.get(time, invalid_op)

    return resample_function(data)


def combine_cameras(dataf):
    cameras_to_combine = dataf.loc[dataf.Location.isin(["Commercial Street at Lush",
                                                        "Commercial Street at Sharps"])]

    total_when_seperate = sum(cameras_to_combine['Count'])

    dataf = dataf.replace({'Location': {'Commercial Street at Lush': 'Commercial Street Combined',
                                        'Commercial Street at Sharps': 'Commercial Street Combined'}})

    total_combined = sum(dataf.loc[dataf.Location == "Commercial Street Combined", "Count"])

    if total_when_seperate == total_combined:
        print("Footfall hasn't changed when combining cameras")
    else:
        print("Footfall has changed when combining cameras")

    return dataf


def set_start_date(dataf):
    dataf = dataf.loc[dataf.DateTime >= '2008-08-27']

    return dataf
