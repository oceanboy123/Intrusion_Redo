######################################################
#         Author:  Edmundo Garcia
#   Date Created:  27/05/2024
#   Program Name:  Intrusion Identification

#    Description:  TBD
#

#        Version:  1.0
#  Date Modified:  27/05/2024
######################################################

# Importing relevant modules
import pandas as pd
from datetime import datetime
import numpy as np

# Reading data file from BBMP BIO
raw_bbmp_data = 'bbmp_aggregated_profiles.csv'
BBMP_data = pd.read_csv(raw_bbmp_data)

# Extrating target data
target_variables = ['time_string','pressure','salinity','temperature']
target_data = BBMP_data.loc[:,target_variables]

# Formating date_string into interger. Days from 01/01/1970 
date_format = "%Y-%m-%d %H:%M:%S"
dates_type_datetime = pd.to_datetime(target_data.iloc[:,0], format=date_format)
target_data['time_string'] = dates_type_datetime

reference_dates = datetime(1970,1, 1)
days_from_reference = (dates_type_datetime - reference_dates).dt.days

# Updating target data and grouping by day
target_data['date_from_1970'] = days_from_reference
grouped_by_date = target_data.groupby('date_from_1970')


unique_dates = list(grouped_by_date.groups.keys())
nested_groups = {}

for group_name, group_data in grouped_by_date:
    nested_groups[group_name] = group_data.values.tolist()

unique_depths = list(set(list(target_data.iloc[:,1])))
unique_depths.sort()

def normalize_length_data(data,upress):
    for key, values in data.items():
        data_frame = pd.DataFrame(values)

        for p in upress:
            if p not in data_frame.iloc[:,1].values:

                new_row = [
                    data_frame.iloc[0,0],
                    p,
                    float('nan'),
                    float('nan'),
                    data_frame.iloc[0,-1]
                ]
                new_df_row = pd.DataFrame(new_row)
                data_frame = pd.concat([data_frame, new_df_row.T], ignore_index=True)
        
        data_frame = data_frame.sort_values(by=1).reset_index(drop=True)
        
        data[key] = data_frame.values.tolist()

    return data

normalized_data = normalize_length_data(nested_groups,unique_depths)

final_depths = pd.DataFrame(normalized_data[list(normalized_data.keys())[0]]).loc[:,1]
interger_dates_final = list(normalized_data.keys())

def separate_target_variables(string_name, data):
    
    all_columns = []
    labels = {
        'temperature':3,
        'salinity':2
    }

    column_num = labels.get(string_name)

    for key, values in data.items():
        next_column = pd.DataFrame(values).iloc(:,column_num)

    return all_columns

# Found out there are timestamps that are different and therefore
# have multiple measuerements for the same day. Making some profiles
# have a lot of repited depths. 
sizess = []
for key, values in normalized_data.items():
    sizess.append(len(pd.DataFrame(values)))

not_good_indices = [index for index, value in enumerate(sizess) if value != 276]
wrong_data_length = pd.DataFrame(normalized_data.keys()).iloc[not_good_indices]