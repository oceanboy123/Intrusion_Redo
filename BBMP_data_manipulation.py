# Importing relevant modules
import json
import pickle
import BBMP_data_tunnel as BBMP

raw_bbmp_data = 'bbmp_aggregated_profiles.csv'
target_variables = ['time_string','pressure','salinity','temperature']

nested_data = BBMP.get_and_group_data(raw_bbmp_data,target_variables)

normalized_data = BBMP.normalize_length_data(nested_data['Nested Groups'],nested_data['Unique Depths'])

variables_matrices = []
for names in target_variables[2:]:
    matrix = BBMP.separate_target_variables(names, normalized_data['Normalized Data'])
    variables_matrices.append(matrix)

transformed_data = BBMP.data_transformations(variables_matrices,target_variables[2:],normalized_data['Normalized Depths'])

selected_data = {
    'sample_row_temp': [],
    'sample_diff_row_temp': list(transformed_data['temperature_avg_diff1_inter10']),
    'sample_matrix_temp': list(transformed_data['temperature_interpolated_axis10']),

    'sample_row_salt': [],
    'sample_diff_row_salt': list(transformed_data['salinity_avg_diff1_inter10']),
    'sample_matrix_salt': list(transformed_data['salinity_interpolated_axis10']),
    
    'sample_timestamps': list(normalized_data['Normalized Dates']),
    'sample_depth': list(normalized_data['Normalized Depths']),

    'sample_intrusion_timestamps':[],
    'intrusion_indice':[],
    'temperature_coeff':[],
    'sallinity_coeff':[]
}

file_name = 'BBMP_salected_data'

with open(file_name+'.pkl', 'wb') as file:pickle.dump(selected_data, file)
with open(file_name+'.json', 'w') as json_file:json.dump(selected_data, json_file, indent=4)