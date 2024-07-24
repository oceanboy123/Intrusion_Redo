# Imports
import pytest
import sys
sys.path.append(r'D:\Projects\Intrusion_Redo\SRC')
import ETL_processes as ETL
import numpy as np
import pandas as pd

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

    # Tests/ Assertions
    for var in variables[1:]:
        truth = np.nanmean(list(testing_class.raw_data.loc[:,var]))
        test_output = np.nanmean(list(testing_class.target_data.loc[:,var]))
        assert truth == test_output


def test_group_data() -> None:
    """Tests that the number of profiles remains consistent"""

    testing_class.group_data()
    truth = len(list(set(list(testing_class.target_data.loc[:,'Timestamp']))))
    test_output = testing_class.metadata['Profile_count'][0]

    # Tests/ Assertions
    assert truth == test_output


def test_normalize_depth_from_list() -> None:
    """Tests for data consistency after normalization"""
    
    test_dict = testing_class.nested_groups
    test_dataframe = test_dict[list(test_dict.keys())[0]]

    # Check if dataframe already has the correct number of columns
    count = 1
    while len(test_dataframe) == len(testing_class.unique_depths):
        test_dataframe = test_dict[list(test_dict.keys())[count]]
        count += 1

    normalized_dataframe = testing_class.normalize_depth_from_list(testing_class.unique_depths, test_dataframe)

    # Tests/ Assertions
    assert len(normalized_dataframe) == len(testing_class.unique_depths)

    for var in variables[2:]:
        truth = np.nanmean(list(test_dataframe.loc[:,var]))
        test_output = np.nanmean(list(normalized_dataframe.loc[:,var]))
        assert truth == test_output

    testing_class.normalized_data = normalized_dataframe


def test_check_duplicate_rows() -> None:
    """Tests if the data has duplicates and if it does,
    it deletes the duplicated rows"""

    # Creating fake duplicated data
    test_not_dup_data = testing_class.normalized_data
    test_dup_data = pd.concat([test_not_dup_data, test_not_dup_data.tail(5)], ignore_index=True)

    # Applying function to get rid of duplication
    checked_data = testing_class.check_duplicate_rows(test_dup_data)

    # Tests/ Assertions
    for var in variables[1:]:
        truth = np.nanmean(list(test_not_dup_data.loc[:,var]))
        test_output = np.nanmean(list(checked_data.loc[:,var]))
        assert truth == test_output


def test_separate_target_variables() -> None:
    """Tests for data consistency after variables are
    separated into NParrays"""
    
    testing_class.normalize_length_data()
    var_name_test = variables[2]
    all_columns_test = testing_class.separate_target_variables(var_name_test)

    truth = np.nanmean(list(testing_class.raw_data.loc[:,var_name_test]))
    test_output = np.nanmean(all_columns_test)

    # Tests/ Assertions
    assert truth == test_output


def test_interpolation_2D() -> None:
    
    # Creating sample data for testing
    head = [1, 1, float('nan')]
    body = [float('nan'), 1, 1]
    body2 = [1, 1, 1]
    tail = [1, float('nan'), 1]
    data = [head, body, body2, tail]
    pandas_matrix = pd.DataFrame(data)

    # Tests/ Assertions
    testing_class.interpolation_2D(pandas_matrix)
    assert np.nanmean(testing_class.interpolated_data) == 1

    