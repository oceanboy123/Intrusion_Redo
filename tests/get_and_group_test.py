import pandas as pd

def get_and_group_test(file_name, variables_target):
    
    print('Reading CSV file')
    raw_bbmp_data = '../src/data' + file_name
    BBMP_data = pd.read_csv(raw_bbmp_data).iloc[:800, :]

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