# Imports
import pytest
import sys
sys.path.append(r'D:\Projects\Intrusion_Redo\SRC')
import ETL_processes as ETL
import numpy as np

# Test Arguments
raw_data = 'raw_sample.csv'
raw_file_path = './tests/' + raw_data
deep = 60
shallow = [20, 35]
date_format ="%Y-%m-%d %H:%M"
variables = ['time_string',
            'pressure',
            'salinity',
            'temperature'
            ]

# Initialize class
testing_class = ETL.Intrusion_ETL(raw_file_path, variables, deep, shallow, date_format)


def test_get_target_data() -> None:
    """Tests that ensures the raw data is correctly extracted. Tested by
     calculating the average of all values in a column from the target
     variables on the raw and output data, and comparing"""

    testing_class.get_target_data()
    for var in variables[1:]:
        truth = np.mean(list(testing_class.raw_data.loc[:,var]))
        test_output = np.mean(list(testing_class.target_data.loc[:,var]))
        assert truth == test_output


def test_group_data() -> None:
    """Tests that the number of profiles remains consistent"""

    testing_class.group_data()
    truth = len(list(set(list(testing_class.target_data.loc[:,'Timestamp']))))
    test_output = testing_class.metadata['Profile_count'][0]
    assert truth == test_output


def test_normalize_depth_from_list() -> None:
    """"""
    
    test_dict = testing_class.nested_groups
    test_dataframe = test_dict[list(test_dict.keys())[0]]

    # Check if dataframe already has the correct number of columns
    count = 1
    while len(test_dataframe) == len(testing_class.unique_depths):
        test_dataframe = test_dict[list(test_dict.keys())[count]]
        count += 1

    normalized_dataframe = testing_class.normalize_depth_from_list(testing_class.unique_depths, test_dataframe)

    assert len(normalized_dataframe) == len(testing_class.unique_depths)

    for var in variables[1:]:
        truth = np.mean(list(test_dataframe.loc[:,var]))
        test_output = np.mean(list(normalized_dataframe.loc[:,var]))
        assert truth == test_output