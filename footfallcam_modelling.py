import pandas as pd
import numpy as np
import datetime
from sklearn.model_selection import train_test_split

from source import *

footfalldf = pd.read_csv("data/lcc_footfall_combined.csv",
                         parse_dates=['DateTime','Date'],
                         dtype= {'BRCWeekNum':int,
                                 'BRCYear':int})
footfalldf.info()



dataf = (footfalldf
        .pipe(start_pipeline)
        .pipe(create_lockdown_var))

dataf = dataf.loc[dataf.BRCYear == 2019]

y = footfalldf_lockdownvar.Count
X = footfalldf_lockdownvar.loc[:,"hosp_indoor":]

X.info()


