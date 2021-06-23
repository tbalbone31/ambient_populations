import pandas as pd
import numpy as np
import datetime

footfalldf = pd.read_csv("data/lcc_footfall_combined.csv",
                         parse_dates=['DateTime','Date'],
                         dtype= {'BRCWeekNum':int,
                                 'BRCYear':int})
footfalldf.info()

one_hot = [0,1]
one_to_three = range(1,3)


lockdownvar = {'hosp_indoor': [1,2,3],'hosp_outdoor': [1,2,3],'hotels': [1,2,3],'ent_indoor': [1,2,3],
                'ent_outdoors': [1,0],'wed_indoor': [1,2,3,4,5],'wed_outdoor': [1,2,3,4,5],'selfcont_acc': [1,2,3,4],
                'sport_leisure_indoor': [1,2,3,4],'sport_leisure_outdoor': [1,2,3,4],'nonessential_retail': [0,1],
                'prim_sch' : [0,1],'sec_sch_college': [0,1],'uni_campus' : [0,1],'max_outdoor_grp_public': [1,2,3,4],
                'max_outdoor_grp_priv': [1,2,3,4,5],'max_indoor_grp': [1,2,3],'eat_out_help': [0,1]}

def hosp_indoor(dataf):

    dataf['hosp_indoor'] = 3
    dataf.loc[(dataf.DateTime >= '2020-03-20') & (dataf.DateTime <'2020-06-15'),'hosp_indoor'] = 1
    dataf.loc[(dataf.DateTime >= '2020-11-02') & (dataf.DateTime < '2021-05-16'),'hosp_indoor'] = 1
    dataf.loc[(dataf.DateTime >= '2020-10-14') & (dataf.DateTime < '2020-11-01'),'hosp_indoor'] = 2

    return dataf

def hosp_outdoor(dataf):

    dataf['hosp_outdoor'] = 3
    dataf.loc[(dataf.DateTime >= '2020-03-20') & (dataf.DateTime <'2020-06-15'),'hosp_outdoor'] = 1
    dataf.loc[(dataf.DateTime >= '2020-11-02') & (dataf.DateTime < '2021-04-11'),'hosp_outdoor'] = 1
    dataf.loc[(dataf.DateTime >= '2020-10-14') & (dataf.DateTime < '2020-11-01'),'hosp_outdoor'] = 2

    return dataf

def hotels(dataf):

    dataf['hotels'] = 4
    dataf.loc[(dataf.DateTime >= '2020-03-26') & (dataf.DateTime <'2020-07-03'),'hotels'] = 1
    dataf.loc[(dataf.DateTime >= '2020-07-04') & (dataf.DateTime < '2020-11-01'), 'hotels'] = 3
    dataf.loc[(dataf.DateTime >= '2020-11-05') & (dataf.DateTime < '2020-12-01'),'hotels'] = 1
    dataf.loc[(dataf.DateTime >= '2020-11-02') & (dataf.DateTime < '2020-11-04'), 'hotels'] = 2
    dataf.loc[(dataf.DateTime >= '2020-12-02') & (dataf.DateTime < '2021-01-05'),'hotels'] = 2
    dataf.loc[(dataf.DateTime >= '2021-01-06') & (dataf.DateTime < '2021-05-16'), 'hotels'] = 1

    return dataf




def create_lockdown_var(dataf):
    lockdown_var = {'hosp_indoor': hosp_indoor, 'hosp_outdoor': [1, 2, 3], 'hotels': [1, 2, 3], 'ent_indoor': [1, 2, 3],
    'ent_outdoors': [1, 0], 'wed_indoor': [1, 2, 3, 4, 5], 'wed_outdoor': [1, 2, 3, 4, 5], 'selfcont_acc': [1, 2, 3, 4],
    'sport_leisure_indoor': [1, 2, 3, 4], 'sport_leisure_outdoor': [1, 2, 3, 4], 'nonessential_retail': [0, 1],
    'prim_sch': [0, 1], 'sec_sch_college': [0, 1], 'uni_campus': [0, 1], 'max_outdoor_grp_public': [1, 2, 3, 4],
    'max_outdoor_grp_priv': [1, 2, 3, 4, 5], 'max_indoor_grp': [1, 2, 3], 'eat_out_help': [0, 1]}

