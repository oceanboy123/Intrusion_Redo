import sys
import os

sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'src', 'data'))

import BBMP_data_functions as BBMP
import pandas as pd

def get_and_group_test(file_name, variables_target):
    
    print('Reading CSV file')
    raw_bbmp_data = '../src/data/' + file_name
    BBMP_data = pd.read_csv(raw_bbmp_data).iloc[7000:10000, :]

    print('Extrating target data')
    target_variables = variables_target
    target_data = BBMP_data.loc[:,target_variables]

  
    date_format = "%Y-%m-%d %H:%M:%S"
    dates_type_datetime = pd.to_datetime(target_data.iloc[:,0], format=date_format)
    target_data['time_string'] = dates_type_datetime

    dates_type_int = []
    for days in dates_type_datetime:

        int_ = days.timestamp()
        dates_type_int.append(int_ )

    print('Updating target data and grouping by day')
    target_data['Timestamp'] = dates_type_int
    grouped_by_date = target_data.groupby('Timestamp')

    nested_groups = {}
    for group_name, group_data in grouped_by_date:
        nested_groups[group_name] = group_data

    unique_depths = list(set(list(target_data.iloc[:,1])))
    unique_depths.sort()

    return {
        'Nested Groups':nested_groups,
        'Unique Depths':unique_depths
    }


raw_bbmp_data = 'bbmp_aggregated_profiles.csv'
target_variables = ['time_string','pressure','salinity','temperature','oxygen']

nested_data = get_and_group_test(raw_bbmp_data,target_variables)

print(nested_data['Nested Groups'][list(nested_data['Nested Groups'].keys())[0]].head())

normalized_data = BBMP.normalize_length_data(nested_data['Nested Groups'],nested_data['Unique Depths'])

variables_matrices = []
for names in target_variables[2:]:
    matrix = BBMP.separate_target_variables(names, normalized_data['Normalized Data'])
    variables_matrices.append(matrix)

transformed_data = BBMP.data_transformations(variables_matrices,target_variables[2:],normalized_data['Normalized Depths'])

selected_data = {
    'sample_row_temp': [],
    'sample_diff_row_temp': transformed_data['temperature_avg_diff1_inter10'],
    'sample_matrix_temp': transformed_data['temperature_interpolated_axis10'],

    'sample_row_salt': [],
    'sample_diff_row_salt': transformed_data['salinity_avg_diff1_inter10'],
    'sample_matrix_salt': transformed_data['salinity_interpolated_axis10'],
    
    'sample_row_oxy': [],
    'sample_diff_row_oxy': transformed_data['oxygen_avg_diff1_inter10'],
    'sample_matrix_oxy': transformed_data['oxygen_interpolated_axis10'],

    'sample_timestamps': normalized_data['Normalized Dates'],
    'sample_depth': normalized_data['Normalized Depths'],

    'sample_intrusion_timestamps':[],
    'intrusion_indice':[],
    'temperature_coeff':[],
    'sallinity_coeff':[],
    'oxygen_coeff':[],
    'Performance':[]
}

file_name = 'BBMP_salected_data0.pkl'

joblib.dump(selected_data, file_name)
print(f'Saved as {file_name}')