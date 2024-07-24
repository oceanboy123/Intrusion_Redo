# Imports
import pytest
import sys
sys.path.append(r'D:\Projects\Intrusion_Redo\SRC')
import Intrusion_analysis as INT
import numpy as np
import pandas as pd

# Test Arguments
file_name = 'BBMP_salected_data0.pkl'
path_data = './data/PROCESSED/'
file_dirpath = path_data + file_name

# Initialize class
testing_class = INT.Intrusions(file_dirpath)