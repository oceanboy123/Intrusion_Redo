from ETL_factory import ETL_factory

factory = ETL_factory()
method = factory.create('ETL_data_extraction')

# def main():
#     # Get command line arguments
#     varsin = {
#             'file_name': 'bbmp_aggregated_profiles.csv',
#             'deep_depth': 60,
#             'mid_depths_top': 20,
#             'mid_depths_bottom': 35,
#             'date_format': '%Y-%m-%d %H:%M:%S',
#         }

#     raw_name, deep_depth, mid_depth1, mid_depth2, date_format = get_command_line_args(varsin)
#     mid_depth = [mid_depth1, mid_depth2]


#     # Make sure the first 2 variables are date and depth
#     target_variables = ['time_string',
#                         'pressure',
#                         'salinity',
#                         'temperature',
#                         'oxygen']

#     # logger.debug(f'\nArguments: \n\nSource: {raw_bbmp_data}, \nDeep Intrusion Depth: {deep_depth}, \nMid Intrusion Depth: {mid_depth}, \nDate Format: {date_format}, \nTarget Variables: {target_variables}\n')

#     # Initializing ETL_Intrusion object
#     bbmp = data_info(raw_bbmp_data, target_variables, deep_depth, mid_depth, date_format)
    
#     profiles = data_extraction(bbmp)
#     profiles.get_target_data()
#     profiles.get_unique_depths()
#     profiles.group_data()
    
#     # Data Manipulation
#     profiles_normalized_depths = data_normalization(bbmp, profiles)
#     profiles_normalized_depths.normalize_length_data()

#     timedepth_space_matrices = timedepth_space(bbmp, profiles_normalized_depths)
#     timedepth_space_matrices.get_variable_matrices()

#     # Data Transformation
#     transformed_profiles = data_transformation(bbmp, profiles_normalized_depths, timedepth_space_matrices)
#     transformed_profiles.data_transformations()

#     # Schema and Metadata Recording
#     output_schema = data_loading(bbmp, profiles_normalized_depths, transformed_profiles)
#     output_schema.conform_schema()
#     output_schema.record_output_metadata()

#     return (
#         bbmp, 
#         profiles,  
#         profiles_normalized_depths, 
#         timedepth_space_matrices, 
#         transformed_profiles, 
#         output_schema
#             )


# if __name__ == '__main__':
#     ETL = main()