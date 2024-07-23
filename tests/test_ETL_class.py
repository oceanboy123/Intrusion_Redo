# Imports
import pytest
import sys
sys.path.append('D:\Projects\Intrusion_Redo\SRC')
import ETL_processes as ETL
import numpy as np

# Test Arguments
raw_data = 'raw_sample.csv'
deep = 60
shallow = [20, 35]
date_format ="%Y-%m-%d %H:%M"
variables = ['time_string',
            'pressure',
            'salinity',
            'temperature'
            ]

# Initialize class
testing_class = ETL.Intrusion_ETL(raw_data, variables, deep, shallow, date_format)


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