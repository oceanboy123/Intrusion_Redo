# Importing relevant modules
import joblib
import BBMP_data_functions as BBMP

raw_bbmp_data = 'bbmp_aggregated_profiles.csv'

target_variables = ['time_string',
                    'pressure',
                    'salinity',
                    'temperature',
                    'oxygen']

nested_data = BBMP.get_and_group_data(raw_bbmp_data,target_variables)

nested_groups: dict = nested_data['Nested Groups']
unique_depths: list = nested_data['Unique Depths']

print(nested_groups[list(nested_groups.keys())[0]].head())

normal_data = BBMP.normalize_length_data(nested_groups,unique_depths)
normalized_data = normal_data['Normalized Data']

variables_matrices = [BBMP.separate_target_variables(names, normalized_data) 
                      for names in target_variables[2:]]

normalized_depths = normal_data['Normalized Depths']
transformed_data = BBMP.data_transformations(variables_matrices,target_variables[2:],normalized_depths)

selected_data = {
    'sample_diff_midrow_temp': transformed_data['temperature_avgmid_diff1_inter10'],
    'sample_diff_row_temp': transformed_data['temperature_avg_diff1_inter10'],
    'sample_matrix_temp': transformed_data['temperature_interpolated_axis10'],

    'sample_diff_midrow_salt': transformed_data['salinity_avgmid_diff1_inter10'],
    'sample_diff_row_salt': transformed_data['salinity_avg_diff1_inter10'],
    'sample_matrix_salt': transformed_data['salinity_interpolated_axis10'],
    
    'sample_diff_midrow_oxy': transformed_data['oxygen_avgmid_diff1_inter10'],
    'sample_diff_row_oxy': transformed_data['oxygen_avg_diff1_inter10'],
    'sample_matrix_oxy': transformed_data['oxygen_interpolated_axis10'],

    'sample_timestamps': normal_data['Normalized Dates'],
    'sample_depth': normal_data['Normalized Depths'],

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