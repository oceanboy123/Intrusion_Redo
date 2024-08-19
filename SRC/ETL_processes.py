# Imports
import joblib
import os
import time
import csv
import pandas as pd
import numpy as np
from misc import create_logger, get_command_line_args
from dataclasses import dataclass, field
from typing import List, Dict, Any

logger = create_logger('ETL_log,' 'etl.log')

@dataclass
class data_info:
    """
    TBD
    """
    original_file_name : str
    target_variables : str
    deep_depth : str
    mid_depth : str
    date_format : str
    metadata : Dict[str, Any] = field(default_factory=dict)

    def __post_init__(self)-> None:
        
        logger.info('\nReading CSV file')
        self.raw_data = pd.read_csv(self.original_file_name)

        # Recording ETL strategy characteristics as metadata
        stat_info = os.stat(self.original_file_name)
        self.metadata['Date_created'] = time.ctime()
        self.metadata['Target_variables'] = str(self.target_variables)
        self.metadata['Deep_averages'] = [self.deep_depth]
        self.metadata['Mid_averages'] = str(self.mid_depth)

@dataclass
class data_extraction:
    """
    TBD
    """
    data_info : data_info
    target_data : Dict[str, Any] = field(default_factory=dict)
    nested_groups : Dict[str, Any] = field(default_factory=dict)
    unique_depths : List[int] = field(default_factory=list)

    original_datename = 'time_string'
    groupby_datename = 'Timestamp'

    def get_target_data(self)-> None:
        """Read data from csv file and records target variables
        as a pandas DataFrame"""
        
        logger.info('\nExtrating target data')
        target_data = self.data_info.raw_data.loc[:, self.data_info.target_variables]

        dates_type_datetime = pd.to_datetime(target_data.iloc[:, 0], format = self.data_info.date_format)
        dates_type_int = [days.timestamp() for days in dates_type_datetime]
        
        target_data[self.original_datename] = dates_type_datetime
        target_data[self.groupby_datename] = dates_type_int
        
        # DataFrame of target variables
        self.target_data = target_data

    
    def get_unique_depths(self) -> None:
        unique_depths = list(set(list(self.target_data.iloc[:, 1])))
        unique_depths.sort()

        # Unique depths for all profiles
        self.unique_depths = unique_depths


    def group_data(self) -> None:
        """Group data by timestamps, aka separate data by profile"""

        logger.info('\nUpdating target data and grouping by profile')
        grouped_by_date = self.target_data.groupby(self.groupby_datename)

        nested_groups = {}
        for group_name, group_data in grouped_by_date:
            nested_groups[group_name] = group_data

        # Separated profiles
        self.nested_groups = nested_groups
        self.data_info.metadata['Profile_count'] = [len(self.nested_groups)]

        logger.debug(f'\nGrouped Data: \n{self.nested_groups[list(self.nested_groups.keys())[0]].head()}')


@dataclass
class data_normalization:
    """
    TBD
    """
    data_info : data_info
    data_extraction : data_extraction
    normalized_data : Dict[str, Any] = field(default_factory=dict)
    normalized_depth : List[int] = field(default_factory=list)
    normalized_dates : List[int] = field(default_factory=list)

    original_pressure_name = 'pressure'

    def normalize_depth_from_list(self, upress: list, data_frame):
        """Use list of unique depths from group_data() to normalize
        the depths available in a profile by creating new rows
        where expected depths are missing and filling with NaN
        values"""

        # Ensure that the columns to be filled with NaN are correctly identified
        num_columns = len(data_frame.columns)
        
        # Create a DataFrame containing all unique pressures (upress) with NaN values for missing depths
        missing_depths = [p for p in upress if p not in data_frame.iloc[:, 1].values]
        
        # Create a list of new rows with NaN values where depths are missing
        if missing_depths:
            missing_rows = pd.DataFrame({
                data_frame.columns[0]: data_frame.iloc[0, 0],  # Assuming the first column is a constant value like date or ID
                self.original_pressure_name: missing_depths,
                **{col: np.nan for col in data_frame.columns[2:-1]},  # Fill middle columns with NaN
                data_frame.columns[-1]: data_frame.iloc[0, -1]  # Assuming the last column is a constant value like a timestamp or group ID
            })

            # Concatenate the original DataFrame with the missing rows
            data_frame = pd.concat([data_frame, missing_rows], ignore_index=True)

        # Sort the DataFrame by the pressure column
        data_frame = data_frame.sort_values(by=self.original_pressure_name).reset_index(drop=True)
        
        return data_frame

    @staticmethod
    def check_duplicate_rows(data_frame):
        column_names = data_frame.columns.tolist()

        # Check for duplicated data
        column_index = 1
        seen = set()
        unique_data = []

        for row in data_frame.values.tolist():
            value = row[column_index]
            if value not in seen:
                seen.add(value)
                unique_data.append(row)

        data_frame = pd.DataFrame(unique_data, columns=column_names)

        return data_frame


    def normalize_length_data(self) -> None:
        logger.info('\nNomalizing depths and filling with NaN')
        data = self.data_extraction.nested_groups
        for key, values in data.items():
            data_frame = values
            data_frame = self.normalize_depth_from_list(self.data_extraction.unique_depths, data_frame)
            data_frame = self.check_duplicate_rows(data_frame)

            data[key] = data_frame

        # Normalized data
        self.normalized_data: dict = data

        normalized_depths = data[list(data.keys())[0]][self.original_pressure_name].tolist()
        normalized_dates = list(data.keys())

        # Final list of unique depths
        self.normalized_dates: list = normalized_dates
        # Final list of unique dates
        self.normalized_depth: list = normalized_depths
        logger.debug(f'\nNormalized Data: \n{data[list(data.keys())[0]].head()}')


@dataclass
class timedepth_space:
    """
    TBD
    """
    data_info : data_info
    data_normalization : data_normalization
    variables_matrices : List[int] = field(default_factory=list)

    def separate_target_variables(self, string_name: str):
        """Creates an NParray for one of the target variables where
        the y-axis represents depth and the x-axis represents
        date"""

        logger.info('Creating Target Variable Matrices')
        all_columns = np.transpose([values[string_name] for key, values in self.data_normalization.normalized_data.items()])

        return all_columns
    

    def get_variable_matrices(self) -> None:
        """Creates NParrays for all target variables"""

        # NParray for all target variables
        self.variables_matrices = [self.separate_target_variables(names) for names in self.data_info.target_variables[2:]]

        logger.debug(f'\nVariable Matrix: \n{pd.DataFrame(self.variables_matrices[0]).head()}')


@dataclass
class data_transformation:
    """
    TBD
    """
    data_info : data_info
    data_normalization : data_normalization
    timedepth_space : timedepth_space
    interpolated0_data : List[int] = field(default_factory=list)
    interpolated_data : List[int] = field(default_factory=list)
    interpolated_diff_data : List[int] = field(default_factory=list)
    avg_below : List[int] = field(default_factory=list)
    avg_btw : List[int] = field(default_factory=list) 
    transform_data : Dict[str, Any] = field(default_factory=dict)

    # Transformation names per variable
    transformation_names = [
            '_interpolated_axis0',
            '_interpolated_axis10',
            '_diff_axis1_inter10',
            '_avg_diff1_inter10',
            '_avgmid_diff1_inter10'
        ]

    def interpolation_2D(self, pandas_matrix) -> None:
        # Then check for interpolation where the is no change, this highlight
        # interpolation with NaNs such as near the ends of profiles.

        # Column interpolation
        interpolated0_data = pandas_matrix.interpolate(axis=0).replace(0, np.nan)

        # diff
        inter0_diff = pd.DataFrame(np.diff(interpolated0_data, axis=0))
        zero_diff = inter0_diff == 0
        mask_diff = np.zeros_like(interpolated0_data, dtype=bool)
        mask_diff[1:] = zero_diff

        interpolated0_data[mask_diff] = np.nan

        # Row interpolation
        self.interpolated_data = interpolated0_data
        self.interpolated_diff_data = pd.DataFrame(np.diff(self.interpolated_data, axis=1)).replace(0, np.nan)


    def depth_averages(self) -> None:
        normal_depths = np.array(self.data_normalization.normalized_depth)
        rows_bellow60 = list(np.where(normal_depths > self.data_info.deep_depth)[0])

        rows_over35 = list(np.where(normal_depths < self.data_info.mid_depth[1])[0])
        rows_under20 = list(np.where(normal_depths > self.data_info.mid_depth[0])[0])
        rows_btw20_35 = sorted(list(set(rows_over35+rows_under20)))

        matrix = self.interpolated_data
        matrix_diff = self.interpolated_diff_data

        self.avg_below_value = matrix.iloc[rows_bellow60, :].mean(axis=0) # Deep depth average
        self.avg_btw_value = matrix.iloc[rows_btw20_35, :].mean(axis=0) # Mid depth average
        self.avg_below = matrix_diff.iloc[rows_bellow60, :].mean(axis=0)
        self.avg_btw = matrix_diff.iloc[rows_btw20_35, :].mean(axis=0)


    def data_transformations(self) -> None:
        """Performs trasnformations of values. Incluiding 2D
        interpolation of profile data to fill NaNs, mainly the 
        ones added for depth normalization
        
        In addition, it also calculates depth averages based
        on the deep_depth and mid_depth variables, and calculates
        the n-th order discrete difference along the date axis
        for those depth averages"""

        logger.info('Interpolating Data')

        transform_data = {}
        count = 2
        for matrix in self.timedepth_space.variables_matrices:
            pandas_matrix = pd.DataFrame(matrix)

            # 2D interpolation
            self.interpolation_2D(pandas_matrix)

            # Calculate depth averages
            self.depth_averages()

            t_names = self.transformation_names
            v_names = self.data_info.target_variables

            transform_data[v_names[count]+t_names[0]] = self.interpolated0_data
            transform_data[v_names[count]+t_names[1]] = self.interpolated_data
            transform_data[v_names[count]+t_names[2]] = self.interpolated_diff_data
            transform_data[v_names[count]+t_names[3]] = self.avg_below
            transform_data[v_names[count]+t_names[4]] = self.avg_btw
            
            count += 1

        # Transformed data ready for loading
        self.transform_data: dict = transform_data
        
        logger.debug(f'\nInterpolated Data: \n{self.transform_data[list(self.transform_data.keys())[1]].head()}')
        logger.debug(f'\nDeep Data: \n{self.transform_data[list(self.transform_data.keys())[3]].head(20)}')
        logger.debug(f'\nMid Data: \n{self.transform_data[list(self.transform_data.keys())[4]].head(20)}')


@dataclass
class data_loading:
    """
    TBD
    """
    data_info : data_info
    data_normalization : data_normalization
    data_transformation : data_transformation
    output_data : Dict[str, Any] = field(default_factory=dict)

    output_file_name = 'BBMP_salected_data0.pkl'
    output_file_path = '../data/PROCESSED/' + 'BBMP_salected_data0.pkl'
    metadata_csv = 'metadata_processing.csv'
    metadata_csv_path = '../data/PROCESSED/' + 'metadata_processing.csv'

    def conform_schema(self) -> None:
        """Load data into predifined schema used in Intrusion_analysis.py.
        NOTE: if this the dict keys chnage, make sure to change them in
        the analysis python script"""
        
        transformed_data = self.data_transformation.transform_data
        self.output_data = {
        'sample_diff_midrow_temp': transformed_data['temperature_avgmid_diff1_inter10'],
        'sample_diff_row_temp': transformed_data['temperature_avg_diff1_inter10'],
        'sample_matrix_temp': transformed_data['temperature_interpolated_axis10'],

        'sample_diff_midrow_salt': transformed_data['salinity_avgmid_diff1_inter10'],
        'sample_diff_row_salt': transformed_data['salinity_avg_diff1_inter10'],
        'sample_matrix_salt': transformed_data['salinity_interpolated_axis10'],

        'sample_timestamps': self.data_normalization.normalized_dates,
        'sample_depth': self.data_normalization.normalized_depth,
        }
        
        try:
            self.output_data['sample_diff_midrow_oxy'] = transformed_data['oxygen_avgmid_diff1_inter10']
            self.output_data['sample_diff_row_oxy'] = transformed_data['oxygen_avg_diff1_inter10']
            self.output_data['sample_matrix_oxy'] = transformed_data['oxygen_interpolated_axis10']
        except Exception:
            self.output_data['sample_diff_midrow_oxy'] = []
            self.output_data['sample_diff_row_oxy'] = []
            self.output_data['sample_matrix_oxy'] = []
        

    def record_output_metadata(self) -> None:
        """Record metadata and save file for analysis"""

        self.data_info.metadata['Output_dataset_path'] = self.output_file_path
        meta_processing = pd.DataFrame(self.data_info.metadata)

        with open(self.metadata_csv_path, 'r') as file:
            read = csv.reader(file)
            row_count = sum(1 for _ in read)

        # Record metadata
        if row_count == 0:
            meta_processing.to_csv(self.metadata_csv_path, mode='a', header=True, index=False)
        else:
            meta_processing.to_csv(self.metadata_csv_path, mode='a', header=False, index=False)

        # Save .pkl file for analysis
        joblib.dump(self.output_data, self.output_file_path)
        logger.info(f'Saved as {self.output_file_name}')

        logger.debug(f'\nMetadata: \n{meta_processing.head()}')


def main():
    # Get command line arguments
    varsin = {
            'file_name': 'bbmp_aggregated_profiles.csv',
            'deep_depth': 60,
            'mid_depths_top': 20,
            'mid_depths_bottom': 35,
            'date_format': '%Y-%m-%d %H:%M:%S',
        }

    raw_name, deep_depth, mid_depth1, mid_depth2, date_format = get_command_line_args(varsin)
    mid_depth = [mid_depth1, mid_depth2]
    raw_bbmp_data = '../data/RAW/' + raw_name

    # Make sure the first 2 variables are date and depth
    target_variables = ['time_string',
                        'pressure',
                        'salinity',
                        'temperature',
                        'oxygen']

    logger.debug(f'\nArguments: \n\nSource: {raw_bbmp_data}, \nDeep Intrusion Depth: {deep_depth}, \nMid Intrusion Depth: {mid_depth}, \nDate Format: {date_format}, \nTarget Variables: {target_variables}\n')

    # Initializing ETL_Intrusion object
    bbmp = data_info(raw_bbmp_data, target_variables, deep_depth, mid_depth, date_format)
    
    profiles = data_extraction(bbmp)
    profiles.get_target_data()
    profiles.get_unique_depths()
    profiles.group_data()
    
    # Data Manipulation
    profiles_normalized_depths = data_normalization(bbmp, profiles)
    profiles_normalized_depths.normalize_length_data()

    timedepth_space_matrices = timedepth_space(bbmp, profiles_normalized_depths)
    timedepth_space_matrices.get_variable_matrices()

    # Data Transformation
    transformed_profiles = data_transformation(bbmp, profiles_normalized_depths, timedepth_space_matrices)
    transformed_profiles.data_transformations()

    # Schema and Metadata Recording
    output_schema = data_loading(bbmp, profiles_normalized_depths, transformed_profiles)
    output_schema.conform_schema()
    output_schema.record_output_metadata()

    return (
        bbmp, 
        profiles,  
        profiles_normalized_depths, 
        timedepth_space_matrices, 
        transformed_profiles, 
        output_schema
            )


if __name__ == '__main__':
    ETL = main()