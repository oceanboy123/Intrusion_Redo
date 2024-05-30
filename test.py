# Importing relevant modules
import pandas as pd
from datetime import datetime
import numpy as np
import matplotlib.pyplot as plt
import BBMP_data_tunnel as BBMP

raw_bbmp_data = 'bbmp_aggregated_profiles.csv'
target_variables = ['time_string','pressure','salinity','temperature']

nested_data = BBMP.get_and_group_data(raw_bbmp_data,target_variables)