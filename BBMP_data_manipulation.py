# Importing relevant modules
import pandas as pd
from datetime import datetime
import numpy as np
import matplotlib.pyplot as plt
import BBMP_data_tunnel as BBMP

raw_bbmp_data = 'bbmp_aggregated_profiles.csv'
target_variables = ['time_string','pressure','salinity','temperature']

nested_data = BBMP.get_and_group_data(raw_bbmp_data,target_variables)

normalized_data = BBMP.normalize_length_data(nested_data['Nested Groups'],nested_data['Unique Depths'])

variables_matrices = []
for names in target_variables[2:]:
    matrix = BBMP.separate_target_variables(names, normalized_data['Normalized Data'])
    variables_matrices.append(matrix)

transformed_data = BBMP.data_transformations(variables_matrices,target_variables[2:],normalized_data['Normalized Depths'])