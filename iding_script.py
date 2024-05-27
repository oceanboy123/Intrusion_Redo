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

raw_bbmp_data = 'bbmp_aggregated_profiles.csv'
BBMP_data = pd.read_csv(raw_bbmp_data)

target_variables = ['time_string','pressure','salinity','temperature']
target_data = BBMP_data.loc[:,target_variables]

dates_type_datetime = pd.to_datetime(target_data.iloc[:,0])
target_data['time_string'] = dates_type_datetime


