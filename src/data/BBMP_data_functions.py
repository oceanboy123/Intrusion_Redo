import pandas as pd
import numpy as np

def get_and_group_data(file_name, variables_target):
    
    print('Reading CSV file')
    raw_bbmp_data = file_name
    BBMP_data = pd.read_csv(raw_bbmp_data)

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




def normalize_length_data(data,upress):
    print('Nomalizing depths and filling with NaN')
    for key, values in data.items():
        data_frame = values

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
        data_frame = data_frame.sort_values(by='pressure').reset_index(drop=True)
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
        data[key] = data_frame

    normalized_depths = pd.DataFrame(data[list(data.keys())[0]]).loc[:,1].tolist()
    normalized_dates = list(data.keys())

    return {
        'Normalized Data': data,
        'Normalized Depths': normalized_depths,
        'Normalized Dates': normalized_dates,
    }




def separate_target_variables(string_name, data):
    
    all_columns = []
    labels = {
        'oxygen':4,
        'temperature':3,
        'salinity':2
    }

    column_num = labels.get(string_name)

    print('Creating Target Variable Matrices')
    for key, values in data.items():
        next_column = pd.DataFrame(values).iloc[:,column_num]
        all_columns.append(next_column)

    return np.transpose(all_columns)




def data_transformations(matrix_list,variables_target,normalized_depths):
    
    print('Interpolating Data')

    transform_data = {}
    transformation_names = [
        '_interpolated_axis0',
        '_interpolated_axis10',
        '_diff_axis1_inter10',
        '_avg_diff1_inter10'
    ]
    count = 0
    for matrix in matrix_list:
        pandas_matrix = pd.DataFrame(matrix)
        matrix_interpolated_axis0 = pandas_matrix.interpolate(axis=0).replace(0,np.nan)
        matrix_interpolated_axis10 = matrix_interpolated_axis0.interpolate(axis=1).replace(0,np.nan)
        matrix_diff = pd.DataFrame(np.diff(matrix_interpolated_axis10, axis=1)).replace(0,np.nan)
        matrix_avg_below = matrix_diff.iloc[list(np.where(normalized_depths > 60)[0]),:].mean(axis=0)

        transform_data[variables_target[count]+transformation_names[0]] = matrix_interpolated_axis0
        transform_data[variables_target[count]+transformation_names[1]] = matrix_interpolated_axis10
        transform_data[variables_target[count]+transformation_names[2]] = matrix_diff
        transform_data[variables_target[count]+transformation_names[3]] = matrix_avg_below
        
        count += 1

    return transform_data


