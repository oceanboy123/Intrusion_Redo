######################################################
#         Author:  Edmundo Garcia
#   Date Created:  29/05/2024
#   Program Name:  Coefficient Estimator for
#                  Intrusion Identification

#    Description:  TBD
#

#        Version:  1.0
#  Date Modified:  29/05/2024
######################################################

import numpy as np
import datetime
import random
import pandas as pd

number_days = 48
number_depths = 294
low_temp = 0.5
high_temp = 8
low_salt = 29
high_salt = 33
num_intrusions = 5
init_year = 2020
end_year = 2024

def make_sample_data(number_days,number_depths,low_temp,high_temp, low_salt,high_salt,num_intrusions, init_year,end_year):
    sample_depths = np.linspace(0.5, 74, number_depths)
    
    sample_temp_matrix = np.random.randint(low_temp, high_temp, size=(number_depths, number_days))
    sample_row_temp = pd.DataFrame(sample_temp_matrix).iloc[list(np.where(sample_depths > 60)[0]),:].mean(axis=0).values
    sample_rowtemp_diff = np.diff(sample_row_temp.reshape(-1,1), axis=0).T
    temp_diff_mean = np.mean(sample_rowtemp_diff)
    temp_diff_var = np.var(sample_rowtemp_diff)
    temp_intrusion = temp_diff_mean + temp_diff_var - (temp_diff_var/4)


    sample_salt_matrix = np.random.randint(low_salt, high_salt, size=(number_depths, number_days))
    sample_row_salt = pd.DataFrame(sample_salt_matrix).iloc[list(np.where(sample_depths > 60)[0]),:].mean(axis=0).values
    sample_rowt_diff = np.diff(sample_row_salt.reshape(-1,1), axis=0).T
    salt_diff_mean = np.mean(sample_rowsalt_diff)
    salt_diff_var = np.var(sample_rowsalt_diff)
    salt_intrusion = salt_diff_mean + salt_diff_var - (salt_diff_var/4)
    
    def sample_timestamp_create(y1,m1,d1,y2,m2,d2,lent):
      start_timestamp = int(datetime.datetime(y1,m1,d1).timestamp())
      end_timestamp = int(datetime.datetime(y2,m2,d2).timestamp())
      num_timestamps = lent
      sample_timestamps = [random.randint(start_timestamp, end_timestamp) for _ in range(num_timestamps)]
      
      return sample_timestamps

    #global sample_timestamp_global
    #sample_timestamp_global = sample_timestamp_create
    
    sample_timestamps = sample_timestamp_create(init_year,1,1,end_year,1,1,number_days)
    sample_intrusion_timestamps = random.choices(sample_timestamps[1:], k=num_intrusions)
    intrusion_indices = np.array([index for index, value in enumerate(sample_timestamps) if value in sample_intrusion_timestamps])

    sample_rowtemp_diff[indices_to_change-1] = temp_intrusion
    sample_rowsalt_diff[indices_to_change-1] = salt_intrusion

    return {'sample row temp': sample_row_temp, 'sample diff row temp': sample_rowtemp_diff,'sample row salt': sample_row_salt, 'sample diff row salt': sample_rowsalt_diff,
            'sample matrix temp': sample_temp_matrix, 'sample matrix salt': sample_salt_matrix,'sample timestamps': sample_timestamps, 
            'sample intrusion timestamps':sample_intrusion_timestamps, 'sample depth': sample_depths,
            'intrusion indice':intrusion_indices}


sample_data = make_sample_data(number_days,number_depths,low_temp,high_temp, low_salt,high_salt,num_intrusions, init_year,end_year)

