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
import matplotlib.pyplot as plt

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

dates_type_int = []
for days in dates_type_datetime:

    int_ = days.timestamp()
    dates_type_int.append(int_ )

# Updating target data and grouping by day
target_data['Timestamp'] = dates_type_int
grouped_by_date = target_data.groupby('Timestamp')


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

        # Check for duplicated data
        column_index = 1
        seen = set()
        unique_data = []

        for row in data_frame.values.tolist():
            value = row[column_index]
            if value not in seen:
                seen.add(value)
                unique_data.append(row)


        data_frame = pd.DataFrame(unique_data)
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

    for key, values in normalized_data.items():
        next_column = pd.DataFrame(values).iloc[:,column_num]
        all_columns.append(next_column)

    return np.transpose(all_columns)

temperature_matrix = separate_target_variables('temperature', normalized_data)
salinity_matrix = separate_target_variables('salinity', normalized_data)

temperature_m = pd.DataFrame(temperature_matrix)
salinity_m = pd.DataFrame(salinity_matrix)

temperature_m_inter_1 = temperature_m.interpolate(axis=1).replace(0,np.nan)
temperature_m_inter_12 = temperature_m_inter_1.interpolate(axis=0).replace(0,np.nan)

salinity_m_inter_1 = salinity_m.interpolate(axis=1).replace(0,np.nan)
salinity_m_inter_12 = salinity_m_inter_1.interpolate(axis=0).replace(0,np.nan)

temperature_m_diff = pd.DataFrame(np.diff(temperature_m_inter_12, axis=1)).replace(0,np.nan)
salinity_m_diff = pd.DataFrame(np.diff(salinity_m_inter_12, axis=1)).replace(0,np.nan)

temperature_avg_60 = temperature_m_diff.iloc[list(np.where(final_depths > 60)[0]),:].mean(axis=0)
salinity_avg_60 = salinity_m_diff.iloc[list(np.where(final_depths > 60)[0]),:].mean(axis=0)

temperature_intrusion_dates = pd.DataFrame(interger_dates_final).iloc[list(np.where(temperature_avg_60 > 0.26)[0])]
salinity_intrusion_dates = pd.DataFrame(interger_dates_final).iloc[list(np.where(salinity_avg_60 > 0.07)[0])]

Both_intrusion_dates = [value for value in temperature_intrusion_dates.values.tolist() if value in salinity_intrusion_dates.values.tolist()]

#plt.imshow(temperature_m_inter_12.to_numpy(), cmap = 'jet')
#plt.colorbar()
#plt.show()

#    Found out there are timestamps that are different and therefore
#    have multiple measuerements for the same day. Making some profiles
#    have a lot of repited depths. 
#
#sizess = []
#for key, values in normalized_data.items():
#    sizess.append(len(pd.DataFrame(values)))
#
#not_good_indices = [index for index, value in enumerate(sizess) if value != 276]
#wrong_data_length = pd.DataFrame(normalized_data.keys()).iloc[not_good_indices]