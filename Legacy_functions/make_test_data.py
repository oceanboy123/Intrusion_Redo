import numpy as np
import datetime
import random
import pandas as pd

def make_test_data(number_days,number_depths,low_temp,high_temp, low_salt,high_salt,num_intrusions, init_year,end_year):
    sample_depths = np.linspace(0.5, 74, number_depths)
        
    sample_temp_matrix = np.random.uniform(low_temp, high_temp, size=(number_depths, number_days))
    sample_row_temp = pd.DataFrame(sample_temp_matrix).iloc[list(np.where(sample_depths > 60)[0]),:].mean(axis=0).values
    sample_rowtemp_diff = np.diff(sample_row_temp.reshape(-1,1), axis=0)
    temp_diff_intrusion = np.percentile(sample_rowtemp_diff,80)


    sample_salt_matrix = np.random.uniform(low_salt, high_salt, size=(number_depths, number_days))
    sample_row_salt = pd.DataFrame(sample_salt_matrix).iloc[list(np.where(sample_depths > 60)[0]),:].mean(axis=0).values
    sample_rowsalt_diff = np.diff(sample_row_salt.reshape(-1,1), axis=0)
    salt_diff_intrusion = np.percentile(sample_rowsalt_diff,80)
        
    def sample_timestamp_create(y1,m1,d1,y2,m2,d2,lent):
        start_timestamp = int(datetime.datetime(y1,m1,d1).timestamp())
        end_timestamp = int(datetime.datetime(y2,m2,d2).timestamp())
        num_timestamps = lent
        sample_timestamps = [random.randint(start_timestamp, end_timestamp) for _ in range(num_timestamps)]
        
        return sample_timestamps

        #global sample_timestamp_global
        #sample_timestamp_global = sample_timestamp_create
        
    sample_timestamps = sample_timestamp_create(init_year,1,1,end_year,1,1,number_days)
    sample_intrusion_timestamps = list(set(random.choices(sample_timestamps[1:], k=num_intrusions)))
    intrusion_indices = np.array([index for index, value in enumerate(sample_timestamps) if value in sample_intrusion_timestamps])

    sample_rowtemp_diff[intrusion_indices-1] = temp_diff_intrusion + random.uniform(0,np.percentile(sample_rowsalt_diff,60))
    sample_rowsalt_diff[intrusion_indices-1] = salt_diff_intrusion + + random.uniform(0,np.percentile(sample_rowsalt_diff,60))

    return {
        'sample_row_temp': sample_row_temp,
        'sample_diff_row_temp': sample_rowtemp_diff,
        'sample_matrix_temp': sample_temp_matrix,

        'sample_row_salt': sample_row_salt,
        'sample_diff_row_salt': sample_rowsalt_diff,
        'sample_matrix_salt': sample_salt_matrix,
    
        'sample_timestamps': sample_timestamps,
        'sample_depth': sample_depths,

        'sample_intrusion_timestamps':sample_intrusion_timestamps,
        'intrusion_indice':intrusion_indices,
        'temperature_coeff':temp_diff_intrusion,
        'sallinity_coeff':salt_diff_intrusion
        }

