# Imports
import pytest
import sys
sys.path.append('D:\Projects\Intrusion_Redo\SRC')
import ETL_processes as ETL

# Test Arguments
raw_bbmp_data = 'raw_sample.csv'
deep_depth = 60
mid_depth = [20, 35]
date_format ="%Y-%m-%d %H:%M"
target_variables = ['time_string',
                        'pressure',
                        'salinity',
                        'temperature'
                        ]

# Initialize class
test_ETL = ETL.Intrusion_ETL(raw_bbmp_data, target_variables, deep_depth, mid_depth, date_format)

def test_add():
    pass


