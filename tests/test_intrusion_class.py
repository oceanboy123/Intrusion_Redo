# Imports
import pytest
import sys
import joblib
sys.path.append(r'D:\Projects\Intrusion_Redo\SRC')
import ETL_processes as ETL
import Intrusion_analysis as INT
import numpy as np
import pandas as pd
from datetime import datetime

# Test Arguments
file_name = 'profile_dataset_sample.pkl'
path_data = './mock_data/'
file_dirpath = path_data + file_name

# Initialize class
testing_class = INT.Intrusions(file_dirpath)

def test_intrusion_date_comparison() -> None:
   """
   
   Tests:
   1- Single match within 10 days
   2- Multiple match within 10 days
   3- Empty manual
   4- Empty estimated
   
   """
   # Test Inputs
   manual = [
      [datetime(1996,3,11), datetime(1996,4,11)],
      [datetime(1996,3,11), datetime(1996,4,11)],
      [],
      [datetime(1996,3,11), datetime(1996,4,11)],
   ]

   estimated = [
      [datetime(1996,3,8), datetime(1996,5,11)],
      [datetime(1996,3,8), datetime(1996,3,2)],
      [datetime(1996,3,8), datetime(1996,3,2)],
      []
   ]

   # Test expected answers
   matched = [
      [[3, datetime(1996, 3, 11, 0, 0), datetime(1996, 3, 8, 0, 0)]],
      [[3, datetime(1996, 3, 11, 0, 0), datetime(1996, 3, 8, 0, 0)]],
      [],
      []
   ]

   only_manual = [
      [datetime(1996, 4, 11, 0, 0)],
      [datetime(1996, 4, 11, 0, 0)],
      [],
      [datetime(1996, 3, 11, 0, 0), datetime(1996, 4, 11, 0, 0)],
   ]

   only_estimated = [
      [datetime(1996, 5, 11, 0, 0)],
      [datetime(1996, 3, 2, 0, 0)],
      [datetime(1996, 3, 8, 0, 0), datetime(1996, 3, 2, 0, 0)],
      []
   ]

   answer = [
      matched,
      only_manual,
      only_estimated,
   ]

   for test in list(range(0, len(manual))):

      dict_result = testing_class.intrusion_date_comparison(manual[test], estimated[test])
      
      for ans in list(range(0, len(answer))):
          
          ans_result = dict_result[list(dict_result.keys())[ans]]
          ans_expected = answer[ans][test]
          assert ans_result == ans_expected


def generate_database_sample() -> None:
   """
    Manual Intrusion Identification Test:
       id_type.upper() == 'MANUAL'
       manual_type.upper() == 'OTHER'
    
   """
   # Test Arguments
   raw_data = 'raw_sample.csv'
   raw_file_path = './mock_data/' + raw_data
   deep = 60
   shallow = [20, 35]
   date_format ="%Y-%m-%d %H:%M"
   variables = ['time_string',
               'pressure',
               'salinity',
               'temperature'
               ]

   # Initialize class
   Process = ETL.Intrusion_ETL(raw_file_path, variables, deep, shallow, date_format)

   Process.get_target_data() 
   Process.group_data()
   Process.normalize_length_data()
   Process.get_variable_matrices()
   Process.data_transformations()
   Process.conform_schema()

   joblib.dump(Process.output_data, './mock_data/profile_dataset_sample.pkl')


def test_get_original_indices() -> None:
   """
    Manual Intrusion Identification Test:
       id_type.upper() == 'MANUAL'
       manual_type.upper() == 'OTHER'
    
   """
   manual_intrusion_input = './mock_data/manualID_MID_sample.pkl'
   intrusion_dates = INT.import_joblib(manual_intrusion_input)
   testing_class.manualID_dates = intrusion_dates
   testing_class.get_original_indices()
   # How do I make sure that I am capturing the effect 
   # of the intrusion and not the state previous to it?
   assert 1==1


def test_get_intrusion_effects() -> None:
    """
    Manual Intrusion Identification Test:
       id_type.upper() == 'MANUAL'
       manual_type.upper() == 'OTHER'
    
    """
    assert 1==1


def test_intrusion_identification() -> None:
    """
    Manual Intrusion Identification Test:
       id_type.upper() == 'MANUAL'
       manual_type.upper() == 'OTHER'
    
    """
    assert 1==1