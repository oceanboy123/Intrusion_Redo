import pandas as pd
import numpy as np

#--------------------------------------------------------------------------------------
#--------------------------------------------------------------------------------------

def get_and_group_data(file_name: str, variables_target: list[str]) -> dict[list]:
    
    print('Reading CSV file')
    BBMP_data = pd.read_csv(file_name)

    print('Extrating target data')
    target_data = BBMP_data.loc[:,variables_target]
  
    date_format = "%Y-%m-%d %H:%M:%S"
    dates_type_datetime = pd.to_datetime(target_data.iloc[:,0], format=date_format)
    target_data['time_string'] = dates_type_datetime

    dates_type_int:list[int] = [days.timestamp() for days in dates_type_datetime]

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

#--------------------------------------------------------------------------------------
#--------------------------------------------------------------------------------------

def normalize_depth_from_list(upress: list, data_frame):
    for p in upress:
        if p not in data_frame.iloc[:,1].values:

            new_row = [
                data_frame.iloc[0,0],
                p,
                float('nan'),
                float('nan'),
                float('nan'),
                data_frame.iloc[0,-1]
            ]

            new_df_row = pd.DataFrame(new_row).T
            new_df_row.columns = data_frame.columns.tolist()
            data_frame = pd.concat([data_frame, new_df_row], ignore_index=True)
        
    data_frame = data_frame.sort_values(by='pressure')

    return data_frame

#--------------------------------------------------------------------------------------
#--------------------------------------------------------------------------------------

def check_duplicate_rows(data_frame):
    column_names = data_frame.columns.tolist()

    # Check for duplicated data
    column_index = 1
    seen = set()
    unique_data = []

    for row in data_frame.values.tolist():
        value = row[column_index]
        if value not in seen:
            seen.add(value)
            unique_data.append(row)

    data_frame = pd.DataFrame(unique_data, columns=column_names)

    return data_frame

#--------------------------------------------------------------------------------------
#--------------------------------------------------------------------------------------

def normalize_length_data(data: dict,upress: list) -> dict[dict,list,list]:
    print('Nomalizing depths and filling with NaN')
    for key, values in data.items():
        data_frame = values
        
        data_frame = normalize_depth_from_list(upress, data_frame)
        data_frame = check_duplicate_rows(data_frame)

        data[key] = data_frame

    normalized_depths = data[list(data.keys())[0]]['pressure'].tolist()
    normalized_dates = list(data.keys())

    return {
        'Normalized Data': data,
        'Normalized Depths': normalized_depths,
        'Normalized Dates': normalized_dates,
    }

#--------------------------------------------------------------------------------------
#--------------------------------------------------------------------------------------

def separate_target_variables(string_name: str, data: dict): 
    print('Creating Target Variable Matrices')
    all_columns = np.transpose([values[string_name] for key, values in data.items()])

    return all_columns

#--------------------------------------------------------------------------------------
#--------------------------------------------------------------------------------------

def data_transformations(matrix_list :list,variables_target : list[str],normalized_depths: list, deep:int, mid:list[int]) -> dict[any]:
    
    print('Interpolating Data')

    transform_data = {}
    transformation_names = [
        '_interpolated_axis0',
        '_interpolated_axis10',
        '_diff_axis1_inter10',
        '_avg_diff1_inter10',
        '_avgmid_diff1_inter10'
    ]

    count = 0
    for matrix in matrix_list:
        pandas_matrix = pd.DataFrame(matrix)
        matrix_interpolated_axis0 = pandas_matrix.interpolate(axis=0).replace(0,np.nan)
        matrix_interpolated_axis10 = matrix_interpolated_axis0.interpolate(axis=1).replace(0,np.nan)
        matrix_diff = pd.DataFrame(np.diff(matrix_interpolated_axis10, axis=1)).replace(0,np.nan)

        normal_depths = np.array(normalized_depths)
        rows_bellow60 = list(np.where(normal_depths > deep)[0])
        rows_over35 = list(np.where(normal_depths < mid[1])[0])
        rows_under20 = list(np.where(normal_depths > mid[0])[0])
        print(rows_under20)
        rows_btw20_35 = sorted(list(set(rows_over35+rows_under20)))
        print(rows_btw20_35)
        matrix_avg_below = matrix_diff.iloc[rows_bellow60,:].mean(axis=0)
        matrix_avg_btw = matrix_diff.iloc[rows_btw20_35,:].mean(axis=0)

        transform_data[variables_target[count]+transformation_names[0]] = matrix_interpolated_axis0
        transform_data[variables_target[count]+transformation_names[1]] = matrix_interpolated_axis10
        transform_data[variables_target[count]+transformation_names[2]] = matrix_diff
        transform_data[variables_target[count]+transformation_names[3]] = matrix_avg_below
        transform_data[variables_target[count]+transformation_names[4]] = matrix_avg_btw
        
        count += 1

    return transform_data

#--------------------------------------------------------------------------------------
#--------------------------------------------------------------------------------------

if __name__ == '__main__':
    # Importing relevant modules
    import joblib
    import ETL_processes as BBMP
    import os
    import time
    import csv

    raw_name = 'bbmp_aggregated_profiles.csv'
    raw_bbmp_data = '../data/RAW/' + raw_name
    
    metadata = {}
    stat_info = os.stat(raw_bbmp_data)
     
    metadata['Date_created'] = time.ctime(stat_info.st_birthtime)

    target_variables = ['time_string',
                        'pressure',
                        'salinity',
                        'temperature',
                        'oxygen']
    
    metadata['Target_variables'] = str(target_variables)

    nested_data = BBMP.get_and_group_data(raw_bbmp_data,target_variables)

    nested_groups: dict = nested_data['Nested Groups']
    unique_depths: list = nested_data['Unique Depths']

    metadata['Profile_count'] = [len(nested_groups)]

    print(nested_groups[list(nested_groups.keys())[0]].head())

    normal_data = BBMP.normalize_length_data(nested_groups,unique_depths)
    normalized_data = normal_data['Normalized Data']

    variables_matrices = [BBMP.separate_target_variables(names, normalized_data) 
                        for names in target_variables[2:]]

    mid = [20,35]
    deep = 60
    normalized_depths = normal_data['Normalized Depths']
    transformed_data = BBMP.data_transformations(variables_matrices,target_variables[2:],normalized_depths, deep, mid)

    metadata['Deep_averages'] = [deep]
    metadata['Mid_averages'] = str(mid)

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
    }

    file_NAME = 'BBMP_salected_data0.pkl'
    file_PATH = '../data/PROCESSED/' + file_NAME

    metadata['Output_dataset_path'] = file_PATH

    meta_processing = pd.DataFrame(metadata)

    metadata_csv = 'metadata_processing.csv'
    csv_path = '../data/PROCESSED/' + metadata_csv

    with open(csv_path,'r') as file:
        read = csv.reader(file)
        row_count = sum(1 for row in read)

    if row_count == 0:
        meta_processing.to_csv(csv_path,mode='a', header=True, index=False)
    else:
        meta_processing.to_csv(csv_path,mode='a', header=False, index=False)

    joblib.dump(selected_data, file_PATH)
    print(f'Saved as {file_NAME}')
