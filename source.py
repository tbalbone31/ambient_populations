import pandas as pd
import numpy as np
import os, os.path
import sys
from bs4 import BeautifulSoup  # requirement beautifulsoup4
from urllib.request import (
    urlopen, urlretrieve)
import plotly.express as px
import datetime

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


def mean_hourly(dataf, freq):

    if freq == "day":
        dataf = dataf.groupby([
            pd.Grouper(key="DateTime",freq="D"),'BRCWeekNum','BRCMonth','BRCYear'])['Count'].aggregate(np.mean)
    elif freq == "month":
        dataf = dataf.groupby(
            ['BRCMonthNum',pd.Grouper(key="BRCMonth"),'BRCYear'])['Count'].aggregate(np.mean).reset_index()
    elif freq == "week":
        dataf = dataf.set_index('DateTime').groupby(
            [pd.Grouper(key="BRCWeekNum"),'BRCYear'])['Count'].aggregate(np.mean)
    elif freq == "year":
        dataf = dataf.set_index('DateTime').groupby(
            [pd.Grouper(key="BRCYear")])['Count'].aggregate(np.mean)

    return dataf

def remove_new_cameras(dataf):

    dataf.drop(dataf[ (dataf['Location'] == "Albion Street at McDonalds") & (dataf['Location'] == "Park Row")].index,inplace=True)

    return dataf

def reset_df_index(dataf):
    dataf = dataf.reset_index()
    return dataf

def set_dt_index(dataf):
    dataf = dataf.set_index('DateTime')

    return dataf

def date_range(dataf,startdate,enddate):

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

def mean_hourly_location(dataf,freq):

    if freq == "day":
        dataf = dataf.groupby(['Location',
            pd.Grouper(key="DateTime",freq="D"),'BRCWeekNum','BRCMonth','BRCYear'])['Count'].aggregate(np.mean)
    elif freq == "month":
        dataf = dataf.groupby(
            ['Location','BRCMonthNum',pd.Grouper(key="BRCMonth"),'BRCYear'])['Count'].aggregate(np.mean).reset_index()
    elif freq == "week":
        dataf = dataf.set_index('DateTime').groupby(
            ['Location',pd.Grouper(key="BRCWeekNum")])['Count'].aggregate(np.mean)
    elif freq == "year":
        dataf = dataf.set_index('DateTime').groupby(
            ['Location',pd.Grouper(key="BRCYear")])['Count'].aggregate(np.mean)

    return dataf

def set_lockdown_timeframe(dataf):
    dataf = dataf.loc[(dataf.BRCYear == 2020) | (dataf.BRCYear == 2021)]

    return dataf

def calculate_baseline(dataf):

    dataf['Day_Name'] = dataf.index.day_name()

    dataf = dataf.groupby([pd.Grouper(level='DateTime', freq="D"), 'Day_Name'])[
        'Count'].sum().reset_index()
    dataf = dataf.set_index('DateTime')

    baseline = (dataf
                .pipe(start_pipeline)
                .pipe(date_range, "2020-01-03", "2020-03-05"))

    baseline = baseline.groupby([pd.Grouper(key="Day_Name")])['Count'].aggregate(np.median)

    dataf = dataf.loc[dataf.index > "2020-03-05"]
    dataf.loc[:, 'baseline'] = dataf.Day_Name.map(baseline.to_dict())
    dataf.loc[:, 'baseline_change'] = dataf.Count - dataf.baseline
    dataf.loc[:, 'baseline_per_change'] = (dataf.baseline_change / dataf.baseline)

    return dataf

def chart_lockdown_dates(fig):
    # Create a dictionary of annotation parameters for the Plotly vertical lines
    vline_anno = {"date": ['2020-03-16',
                           '2020-03-23',
                           '2020-05-10',
                           '2020-06-01',
                           '2020-06-15',
                           '2020-10-14',
                           '2020-11-05',
                           '2020-09-22',
                           '2020-12-02',
                           '2020-08-03',
                           '2021-01-05',
                           '2021-03-08',
                           '2021-03-29',
                           '2021-04-12'],

                  "text": ["16th Mar",
                           "23rd Mar",
                           "10th <br> May",
                           "1st Jun",
                           "15th <br> Jun",
                           '14th Oct',
                           '5th <br> Nov',
                           '22nd <br> Sept',
                           '2nd <br> Dec',
                           '3rd <br> Aug',
                           '2nd <br> Jan',
                           '8th <br> Mar',
                           '29th Mar',
                           '12th <br> Apr'],

                  "textangle": [0, -90, 0, -90, 0, -90, 0, 0, 0, 0, 0, 0, -90, 0],
                  "position": ["top", "right", "top", "left", "top", "left", "top", "top", "top", "top", "top", "top",
                               "left", "top"],
                  "showarrow": [True, False, False, False, False, False, False, False, False, False, False, False,
                                False, False]
                  }
    # Create a dictionary of annotation parameters for the Plotly vertical rectangles
    vrec_anno = {"x0": ['2020-03-23', '2020-06-15', '2020-11-05', '2020-12-02', '2021-01-05', '2021-03-29'],
                 "x1": ['2020-06-15', '2020-11-05', '2020-12-02', '2021-01-05', '2021-03-29', '2021-04-25'],
                 "fillcolor": ['red', 'orange', 'red', 'orange', 'red', 'orange']
                 }

    for i, date in enumerate(vline_anno['date']):
        fig.add_vline(
            x=datetime.datetime.strptime(date, "%Y-%m-%d").timestamp() * 1000,
            line_color="green", line_dash="dash",
            annotation_position=vline_anno['position'][i],
            annotation=dict(text=vline_anno['text'][i],
                            font_size=10,
                            textangle=vline_anno['textangle'][i],
                            showarrow=vline_anno['showarrow'][i],
                            arrowhead=1)
        )

    for i, x0 in enumerate(vrec_anno['x0']):
        fig.add_vrect(
            x0=datetime.datetime.strptime(x0, "%Y-%m-%d").timestamp() * 1000,
            x1=datetime.datetime.strptime(vrec_anno['x1'][i], "%Y-%m-%d").timestamp() * 1000,
            fillcolor=vrec_anno['fillcolor'][i], opacity=0.25, line_width=0)

    return fig

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


def set_start_date(dataf,date):
    dataf = dataf.loc[dataf.DateTime >= date]

    return dataf



def hosp_indoor(dataf):

    dataf['hosp_indoor'] = 3
    dataf.loc[(dataf.DateTime >= '2020-03-20') & (dataf.DateTime < '2020-06-15'),'hosp_indoor'] = 1
    dataf.loc[(dataf.DateTime >= '2020-10-14') & (dataf.DateTime < '2020-11-02'), 'hosp_indoor'] = 2
    dataf.loc[(dataf.DateTime >= '2020-11-02') & (dataf.DateTime < '2021-05-17'),'hosp_indoor'] = 1


    return dataf

def hosp_outdoor(dataf):

    dataf['hosp_outdoor'] = 3
    dataf.loc[(dataf.DateTime >= '2020-03-20') & (dataf.DateTime < '2020-06-15'),'hosp_outdoor'] = 1
    dataf.loc[(dataf.DateTime >= '2020-10-14') & (dataf.DateTime < '2020-11-02'),'hosp_outdoor'] = 2
    dataf.loc[(dataf.DateTime >= '2020-11-02') & (dataf.DateTime < '2021-04-12'),'hosp_outdoor'] = 1


    return dataf

def hotels(dataf):

    dataf['hotels'] = 4
    dataf.loc[(dataf.DateTime >= '2020-03-26') & (dataf.DateTime < '2020-07-04'),'hotels'] = 1
    dataf.loc[(dataf.DateTime >= '2020-07-04') & (dataf.DateTime < '2020-11-02'), 'hotels'] = 3
    dataf.loc[(dataf.DateTime >= '2020-11-05') & (dataf.DateTime < '2020-12-02'),'hotels'] = 1
    dataf.loc[(dataf.DateTime >= '2020-11-02') & (dataf.DateTime < '2020-11-05'), 'hotels'] = 2
    dataf.loc[(dataf.DateTime >= '2020-12-02') & (dataf.DateTime < '2021-01-06'),'hotels'] = 2
    dataf.loc[(dataf.DateTime >= '2021-01-06') & (dataf.DateTime < '2021-05-17'), 'hotels'] = 1

    return dataf

def ent_indoor(dataf):

    #default value of open with no restrictions
    dataf['ent_indoor'] = 5

    #full closure until reopening on 4th July 2020
    dataf.loc[(dataf.DateTime >= '2020-03-20') & (dataf.DateTime < '2020-07-04'), 'ent_indoor'] = 1

    #reopens on 4th July with up to 30 people legally allowed until rule of 6 legally introduced on 14th Sept 2020
    dataf.loc[(dataf.DateTime >= '2020-07-04') & (dataf.DateTime < '2020-09-14'), 'ent_indoor'] = 4

    #open with rule of 6 until 14th October 2020 when put into tier 2
    dataf.loc[(dataf.DateTime >= '2020-09-14') & (dataf.DateTime < '2020-10-14'),'ent_indoor'] = 3

    #open but only with household until 2nd November when put into tier 3
    dataf.loc[(dataf.DateTime >= '2020-10-14') & (dataf.DateTime < '2021-11-02'), 'ent_indoor'] = 2

    #full closed after being put in tier 3, through national lockdown 2, Christmas in tier 3 and national lockdown 3.
    dataf.loc[(dataf.DateTime >= '2020-11-02') & (dataf.DateTime < '2021-05-17'), 'ent_indoor'] = 1

    #Reopens to rule of 6 on 17th May 2020
    dataf.loc[(dataf.DateTime >= '2021-05-17'),'ent_indoor'] = 3

    return dataf

def ent_outdoor(dataf):

    # default value of open with no restrictions
    dataf['ent_outdoor'] = 5

    # full closure until reopening on 4th July 2020
    dataf.loc[(dataf.DateTime >= '2020-03-20') & (dataf.DateTime < '2020-07-04'), 'ent_outdoor'] = 1

    # reopens on 4th July with up to 30 people legally allowed until rule of 6 legally introduced on 14th Sept 2020
    dataf.loc[(dataf.DateTime >= '2020-07-04') & (dataf.DateTime < '2020-09-14'), 'ent_outdoor'] = 5

    # open with rule of 6 until 5th november 2020 when national lockdown starts
    dataf.loc[(dataf.DateTime >= '2020-09-14') & (dataf.DateTime < '2020-11-05'), 'ent_outdoor'] = 3

    # full closure during 2nd national lockdown until put back into tier 3 on 2nd December 2020.
    dataf.loc[(dataf.DateTime >= '2020-11-05') & (dataf.DateTime < '2020-12-02'), 'ent_outdoor'] = 1

    # open with rule of 6 until 6th January 2021 when 3rd national lockdown starts
    dataf.loc[(dataf.DateTime >= '2020-12-02') & (dataf.DateTime < '2021-01-06'), 'ent_outdoor'] = 3

    # full closure during 3rd national lockdown until reopens on 12th April 2021
    dataf.loc[(dataf.DateTime >= '2021-01-06') & (dataf.DateTime < '2021-04-12'), 'ent_outdoor'] = 1

    # Reopens to rule of 6 on 12th April 2021
    dataf.loc[(dataf.DateTime >= '2021-04-12'), 'ent_outdoor'] = 3

    return dataf

def weddings(dataf):

    # default value of Yes with no restrictions
    dataf['weddings'] = 5

    #Fully banned during lockdown 1 until restrictions eased on 4th July
    dataf.loc[(dataf.DateTime >= '2020-03-23') & (dataf.DateTime < '2020-07-04'), 'weddings'] = 1

    #Weddings of up to 30 people allowed
    dataf.loc[(dataf.DateTime >= '2020-07-04') & (dataf.DateTime < '2020-09-28'), 'weddings'] = 4

    #Weddings of up to 15 people allowed
    dataf.loc[(dataf.DateTime >= '2020-09-28') & (dataf.DateTime < '2020-11-05'), 'weddings'] = 3

    #Weddings banned during lockdown 2 until restrictions eased on 2nd December
    dataf.loc[(dataf.DateTime >= '2020-11-05') & (dataf.DateTime < '2020-12-02'), 'weddings'] = 1

    # Weddings of up to 15 people allowed until start of lockdown 3 in January 2021
    dataf.loc[(dataf.DateTime >= '2020-12-02') & (dataf.DateTime < '2021-01-06'), 'weddings'] = 3

    # Weddings banned during lockdown 3 until restrictions eased on 29th March 2021
    dataf.loc[(dataf.DateTime >= '2021-01-05') & (dataf.DateTime < '2021-03-29'), 'weddings'] = 1

    # Weddings of up to 6 people allowed until 12th April 2021
    dataf.loc[(dataf.DateTime >= '2021-03-29') & (dataf.DateTime < '2021-04-12'), 'weddings'] = 2

    # Weddings of up to 15 people allowed until 17th May 2021
    dataf.loc[(dataf.DateTime >= '2021-04-12') & (dataf.DateTime < '2021-05-17'), 'weddings'] = 3

    # Weddings of up to 30 people allowed until 21st June 2021
    dataf.loc[(dataf.DateTime >= '2021-05-17') & (dataf.DateTime < '2021-06-21'), 'weddings'] = 4

    return dataf

def self_acc(dataf):

    dataf['self_acc'] = 5

    #Fully banned during lockdown 1 until restrictions eased on 4th July
    dataf.loc[(dataf.DateTime >= '2020-03-23') & (dataf.DateTime < '2020-07-04'), 'self_acc'] = 1

    #Allowed with max legal limits of 30 people up to rule of 6 on 14th September
    dataf.loc[(dataf.DateTime >= '2020-07-04') & (dataf.DateTime < '2020-09-14'), 'self_acc'] = 4

    #Rule of 6
    dataf.loc[(dataf.DateTime >= '2020-09-14') & (dataf.DateTime < '2020-10-14'), 'self_acc'] = 3

    #Household only
    dataf.loc[(dataf.DateTime >= '2020-10-14') & (dataf.DateTime < '2020-11-05'), 'self_acc'] = 2

    #Fully banned during lockdown 2 until special Christmas rules 24-26th December
    dataf.loc[(dataf.DateTime >= '2020-11-05') & (dataf.DateTime < '2020-12-24'), 'self_acc'] = 1

    #Special christmas rules allow more than one household of any size up to 3 households to get together.  Just classify as rule of 6 for the purposes of modelling
    dataf.loc[(dataf.DateTime >= '2020-12-24') & (dataf.DateTime < '2020-12-27'), 'self_acc'] = 3

    #Fully banned under tier 3 and all through national lockdown 3 until 12th April 2021
    dataf.loc[(dataf.DateTime >= '2020-12-27') & (dataf.DateTime < '2021-04-12'), 'self_acc'] = 1

    #Household only
    dataf.loc[(dataf.DateTime >= '2021-04-12') & (dataf.DateTime < '2021-05-17'), 'self_acc'] = 2

    #Household only
    dataf.loc[dataf.DateTime >= '2021-05-17', 'self_acc'] = 2

    return dataf

def sport_lei_indoor(dataf):

    #Default values of open with no restrictions
    dataf['sport_lei_indoor'] = 5

    #Fully banned during lockdown 1 until restrictions eased on 25th July
    dataf.loc[(dataf.DateTime >= '2020-03-23') & (dataf.DateTime < '2020-07-25'), 'sport_lei_indoor'] = 1

    #Reopen legally for groups of up to 30 (although guidance states rule of 6)
    dataf.loc[(dataf.DateTime >= '2020-07-25') & (dataf.DateTime < '2020-09-14'), 'sport_lei_indoor'] = 4

    #Open with rule of 6
    dataf.loc[(dataf.DateTime >= '2020-09-14') & (dataf.DateTime < '2020-10-14'), 'sport_lei_indoor'] = 3

    #Household only
    dataf.loc[(dataf.DateTime >= '2020-10-14') & (dataf.DateTime < '2020-11-05'), 'sport_lei_indoor'] = 4

    #Fully banned during lockdown 2, through tier 3 and lockdown 3 until restrictions eased on 12 April
    dataf.loc[(dataf.DateTime >= '2020-11-05') & (dataf.DateTime < '2021-04-12'), 'sport_lei_indoor'] = 1

    #Open to household only
    dataf.loc[(dataf.DateTime >= '2021-04-12'), 'sport_lei_indoor'] = 1

    return dataf

def sport_lei_outdoor(dataf):

    dataf['sport_lei_outdoor'] = 5

    #Fully banned during lockdown 1 until restrictions eased on 4th July
    dataf.loc[(dataf.DateTime >= '2020-03-23') & (dataf.DateTime < '2020-07-04'), 'sport_lei_outdoor'] = 1

    #No restrictions on organised sport or leisure organised formally
    dataf.loc[(dataf.DateTime >= '2020-07-04') & (dataf.DateTime < '2020-11-05'), 'sport_lei_outdoor'] = 5

    #Fully banned during lockdown 2, through tier 3 until restrictions eased on
    dataf.loc[(dataf.DateTime >= '2020-11-05') & (dataf.DateTime < '2021-03-29'), 'sport_lei_outdoor'] = 1

    #Fully banned during lockdown 1 until restrictions eased on 4th July
    dataf.loc[(dataf.DateTime >= '2021-03-29'), 'sport_lei_outdoor'] = 5

    return dataf

def non_essential_retail(dataf):

    dataf['non_ess_retail'] = 1

    #Fully closed during lockdown 1 until restrictions eased on 15th June
    dataf.loc[(dataf.DateTime >= '2020-03-23') & (dataf.DateTime < '2020-06-15'), 'non_ess_retail'] = 0

    #Fully closed during lockdown  until restrictions eased on 2nd December
    dataf.loc[(dataf.DateTime >= '2020-11-05') & (dataf.DateTime < '2020-12-02'), 'non_ess_retail'] = 0

    #Fully closed during lockdown 3 until restrictions eased on 12th April
    dataf.loc[(dataf.DateTime >= '2021-01-05') & (dataf.DateTime < '2021-04-12'), 'non_ess_retail'] = 0

    return dataf

def primary_schools(dataf):

    dataf['prim_sch'] = 1

    #Fully closed during lockdown 1 until restrictions eased on 1st June 2020
    dataf.loc[(dataf.DateTime >= '2020-03-23') & (dataf.DateTime < '2020-06-01'), 'prim_sch'] = 0

    #Fully closed during lockdown 3 until restrictions eased on 8th March 2021
    dataf.loc[(dataf.DateTime >= '2021-01-06') & (dataf.DateTime < '2021-03-08'), 'prim_sch'] = 0

    return dataf

def secondary_schools(dataf):

    dataf['sec_sch'] = 1

    #Fully closed during lockdown until restrictions eased on
    dataf.loc[(dataf.DateTime >= '2020-03-23') & (dataf.DateTime < '2020-06-15'), 'sec_sch'] = 0

    #Fully closed during lockdown until restrictions eased on
    dataf.loc[(dataf.DateTime >= '2021-01-06') & (dataf.DateTime < '2021-03-08'), 'sec_sch'] = 0

    return dataf

def university(dataf):

    #Open or blended learning
    dataf['uni_campus'] = 1

    #Mostly closed during lockdown until start of 2020/2021 academic year
    dataf.loc[(dataf.DateTime >= '2020-03-23') & (dataf.DateTime < '2020-09-01'), 'uni_campus'] = 0

    #Mostly closed during lockdown 3 until restrictions eased on 17th May
    dataf.loc[(dataf.DateTime >= '2021-01-05'), 'uni_campus'] = 0

    return dataf

def outdoor_grp_public(dataf):

    dataf['outdoor_grp_public'] = 5

    #Max two people gathering outside of household
    dataf.loc[(dataf.DateTime >= '2020-03-23') & (dataf.DateTime < '2020-06-01'), 'outdoor_grp_public'] = 2

    #Max 6 people gathering
    dataf.loc[(dataf.DateTime >= '2020-06-01') & (dataf.DateTime < '2020-07-04'), 'outdoor_grp_public'] = 3

    #Max 30 people gathering (although rule of 6 as 'guidance')
    dataf.loc[(dataf.DateTime >= '2020-07-04') & (dataf.DateTime < '2020-09-14'), 'outdoor_grp_public'] = 4

    #Rule of 6 becomes legal
    dataf.loc[(dataf.DateTime >= '2020-09-14') & (dataf.DateTime < '2020-11-05'), 'outdoor_grp_public'] = 3

    #Max two people gathering outside of household
    dataf.loc[(dataf.DateTime >= '2020-11-05') & (dataf.DateTime < '2020-12-02'), 'outdoor_grp_public'] = 2

    #Rule of 6
    dataf.loc[(dataf.DateTime >= '2020-12-02') & (dataf.DateTime < '2021-01-05'), 'outdoor_grp_public'] = 3

    #Max two people gathering outside of household
    dataf.loc[(dataf.DateTime >= '2021-01-05') & (dataf.DateTime < '2021-03-29'), 'outdoor_grp_public'] = 2

    #Rule of 6
    dataf.loc[(dataf.DateTime >= '2021-03-29'), 'outdoor_grp_public'] = 3

    return dataf

def outdoor_grp_private(dataf):

    dataf['outdoor_grp_private'] = 5

    # Max two people gathering outside of household
    dataf.loc[(dataf.DateTime >= '2020-03-23') & (dataf.DateTime < '2020-06-01'), 'outdoor_grp_private'] = 2

    #Max 6 people gathering
    dataf.loc[(dataf.DateTime >= '2020-06-01') & (dataf.DateTime < '2020-07-04'), 'outdoor_grp_private'] = 3

    #Max 30 people gathering (although rule of 6 as 'guidance')
    dataf.loc[(dataf.DateTime >= '2020-07-04') & (dataf.DateTime < '2020-09-14'), 'outdoor_grp_private'] = 4

    #Rule of 6 becomes legal
    dataf.loc[(dataf.DateTime >= '2020-09-14') & (dataf.DateTime < '2020-11-02'), 'outdoor_grp_private'] = 3

    #Household only
    dataf.loc[(dataf.DateTime >= '2020-11-02') & (dataf.DateTime < '2020-11-05'), 'outdoor_grp_private'] = 1

    #Max two people gathering outside of household
    dataf.loc[(dataf.DateTime >= '2020-11-05') & (dataf.DateTime < '2020-12-02'), 'outdoor_grp_private'] = 2

    #Household only
    dataf.loc[(dataf.DateTime >= '2020-12-02') & (dataf.DateTime < '2021-03-29'), 'outdoor_grp_private'] = 1

    #Rule of 6
    dataf.loc[(dataf.DateTime >= '2021-03-29'), 'outdoor_grp_private'] = 3

    return dataf

def indoor_grp(dataf):

    dataf['indoor_grp'] = 4

    # Household group only
    dataf.loc[(dataf.DateTime >= '2020-03-23') & (dataf.DateTime < '2020-07-04'), 'indoor_grp'] = 1

    #Max 30 people gathering (although rule of 6 as 'guidance')
    dataf.loc[(dataf.DateTime >= '2020-07-04') & (dataf.DateTime < '2020-09-14'), 'indoor_grp'] = 3

    #Rule of 6
    dataf.loc[(dataf.DateTime >= '2020-09-14') & (dataf.DateTime < '2020-10-14'), 'indoor_grp'] = 2

    #Household only
    dataf.loc[(dataf.DateTime >= '2020-10-14') & (dataf.DateTime < '2020-12-24'), 'indoor_grp'] = 1

    #Special christmas rules allow more than one household of any size up to 3 households to get together.  Just classify as rule of 6 for the purposes of modelling
    dataf.loc[(dataf.DateTime >= '2020-12-24') & (dataf.DateTime < '2020-12-27'), 'indoor_grp'] = 2

    #Household only
    dataf.loc[(dataf.DateTime >= '2020-12-27'), 'indoor_grp'] = 1

    return dataf

def eat_out(dataf):

    dataf['eat_out'] = 0

    #Eat out to Help out scheme active, encouraging people to go and use hospitality venues.
    dataf.loc[(dataf.DateTime >= '2020-08-03') & (dataf.DateTime <= '2020-08-31'), 'eat_out'] = 1

    return dataf


def create_lockdown_var(dataf):

    lockdown_var_list = [hosp_indoor,
                         hosp_outdoor,
                         hotels,
                         ent_indoor,
                         ent_outdoor,
                         weddings,
                         self_acc,
                         sport_lei_indoor,
                         sport_lei_outdoor,
                         non_essential_retail,
                         primary_schools,
                         secondary_schools,
                         university,
                         outdoor_grp_public,
                         outdoor_grp_private,
                         indoor_grp,
                         eat_out]

    for func in lockdown_var_list:
        dataf = func(dataf)

    return dataf