######################################################
#         Author:  Edmundo Garcia
#   Date Created:  27/05/2024
#   Program Name:  Intrusion Identification

#    Description:  TBD
#

#        Version:  1.0
#  Date Modified:  27/05/2024
######################################################

import pandas as pd
from datetime import datetime

raw_bbmp_data = 'bbmp_aggregated_profiles.csv'
BBMP_data = pd.read_csv(raw_bbmp_data)

target_variables = ['time_string','pressure','salinity','temperature']
target_data = BBMP_data.loc[:,target_variables]

date_format = "%Y-%m-%d %H:%M:%S"
dates_type_datetime = pd.to_datetime(target_data.iloc[:,0], format=date_format)
target_data['time_string'] = dates_type_datetime

reference_dates = datetime(1970,1, 1)
days_from_reference = (dates_type_datetime - reference_dates).dt.days

target_data['date_from_1970'] = days_from_reference
grouped_by_date = target_data.groupby('date_from_1970')
grouped_by_date.get_group(10716)