# TODO: Add __init__.py for simplicity
# Imports
import pandas as pd
from dataclasses import dataclass, field
from typing import List, Dict, Any
from logging import Logger
from abc import ABC

# Custom Functions and Wrappers
from misc.other.logging import function_log
from misc.other.date_handling import date_comparison
from misc.other.file_handling import import_joblib, count_csv_rows

# ABC Classes
from Process_builder.process_step import Step
from misc.request_arguments.request_info_analysis import RequestInfo_Analysis

# Transformation names based on ETL_data_loading
bottom_avg_names = ['sample_diff_row_temp', 'sample_diff_row_salt'] 
mid_avg_names = ['sample_diff_midrow_temp', 'sample_diff_midrow_salt'] 