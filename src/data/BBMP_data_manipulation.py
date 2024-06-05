# Importing relevant modules
import joblib
import BBMP_data_functions as BBMP

raw_bbmp_data = 'bbmp_aggregated_profiles.csv'
target_variables = ['time_string','pressure','salinity','temperature','oxygen']

nested_data = BBMP.get_and_group_data(raw_bbmp_data,target_variables)

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