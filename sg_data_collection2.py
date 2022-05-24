import pandas as pd
import os
from glob import glob

reports = dict()
sec_to_hr = (1/3600)
for x in glob('/home/ted/Documents/kc/CCA and SG/KC/Onfleet/profitPerRoute/OF Data/Dec 2021/*/*.csv', recursive=True):
    reports[os.path.basename(x).strip('.csv')] = pd.read_csv(x)
completions = reports.get('taskCompletionResult_by_date').drop('Unnamed: 0', axis='columns').T.reset_index()
completions.columns = ['Date', 'Succeeded', 'Failed']
delayed = reports.get('taskDelayedOnCompletion_by_date').drop('Unnamed: 0', axis='columns').T.reset_index()
delayed.columns = ['Date', 'On Time', 'Delayed < 10 min', 'Delayed 10-30 mins', 'Delayed 30-60 mins', 'Delayed > 60 mins']
distance = reports.get('workerDistance_by_date').drop('Unnamed: 0', axis='columns').T.reset_index()
distance.columns = ['Date', 'Dist In Transit', 'Dist Idle']
duration = reports.get('workerDuration_by_date').drop('Unnamed: 0', axis='columns').T.reset_index()
duration.columns = ['Date', 'Dur In Transit', 'Dur Idle']

summary = completions.merge(
    delayed,
    how='inner',
    on='Date'
    ).merge(
        distance,
        how='inner',
        on='Date'
    ).merge(
        duration,
        how='inner',
        on='Date'
    )
summary['Dur In Transit'] = summary['Dur In Transit'] * sec_to_hr
summary['Dur Idle'] = summary['Dur Idle'] * sec_to_hr
summary['% On Time'] = (summary['On Time'] / (summary['Succeeded']+ summary['Failed'])) 
summary['% <10 min Late'] = (summary['Delayed < 10 min'] / (summary['Succeeded']+ summary['Failed']))
summary['% 10-30 min Late'] = (summary['Delayed 10-30 mins'] / (summary['Succeeded']+ summary['Failed']))
summary['% 30-60 min Late'] = (summary['Delayed 30-60 mins'] / (summary['Succeeded']+ summary['Failed']))
summary['Tasks per Hour'] = ((summary['Succeeded'] + summary['Failed']) / summary['Dur In Transit']).round(2)
summary['Miles per Task'] = ((summary['Dist In Transit'] + summary['Dist Idle']) / (summary['Succeeded'] + summary['Failed'])).round(2)
summary['Order Rate'] = summary['Succeeded'] + summary['Failed']
summary['% Dur Idle'] = summary['Dur Idle'] / (summary['Dur Idle'] + summary['Dur In Transit'])
summary = summary[[
    'Date',
    'Order Rate',
    'Tasks per Hour',
    'Miles per Task',
    'Dur Idle',
    '% Dur Idle',
    'Dist Idle',
    '% On Time',
    '% <10 min Late',
    '% 10-30 min Late',
    '% 30-60 min Late',
    'Failed'
     ]]
