# Importing relevant modules
import joblib
import os
import time
import csv
import argparse
import pandas as pd
import numpy as np


def get_command_line_args():
    file_name = 'bbmp_aggregated_profiles.csv'
    d_depth = 60
    m_depth = [20, 35]
    date_format="%Y-%m-%d %H:%M:%S"

    # Command line arguments
    parser = argparse.ArgumentParser(description='Arguments')
    parser.add_argument('--file_name', type=str, help="""TBD""", default=file_name)
    parser.add_argument('--deep_depth', type=str, help='TBD')
    parser.add_argument('--mid_depths', type=list[int], help='TBD')
    parser.add_argument('--date_format', type=str, help='TBD')

    # Parse and read arguments and assign them to variables if exists
    args, _ = parser.parse_known_args()

    raw_name = file_name
    if args.file_name:
        raw_name = args.file_name

    deep_depth = d_depth
    if args.d_depth:
        deep_depth = args.d_depth

    mid_depth = m_depth
    if args.m_depth:
        mid_depth = args.m_depth

    date_format_fin = date_format
    if args.date_format:
        date_format_fin= args.date_format

    return raw_name, deep_depth, mid_depth, date_format_fin


class Intrusion_ETL:

    original_datename = 'time_string'
    original_pressure_name = 'pressure'
    groupby_datename = 'Timestamp'
    transformation_names = [
            '_interpolated_axis0',
            '_interpolated_axis10',
            '_diff_axis1_inter10',
            '_avg_diff1_inter10',
            '_avgmid_diff1_inter10'
        ]
    output_file_name = 'BBMP_salected_data0.pkl'
    output_file_path = '../data/PROCESSED/' + output_file_name
    metadata_csv = 'metadata_processing.csv'
    metadata_csv_path = '../data/PROCESSED/' + metadata_csv

    def __init__(self, file_name: str, variables_target: list[str], deep_depth: int, mid_depth: list[int], date_format: str) -> None:
        self.original_file_name = file_name
        self.target_variables = variables_target
        self.deep_depth = deep_depth
        self.mid_depth = mid_depth
        self.date_format = date_format
        self.target_data = {}
        self.nested_groups = {}
        self.unique_depths = []
        self.normalized_data = {}
        self.normalized_depth = []
        self.normalized_dates = []
        self.variables_matrices = []
        self.transform_data = {}
        self.metadata = {}

        stat_info = os.stat(self.original_file_name)
        self.metadata['Date_created'] = time.ctime(stat_info.st_birthtime)
        self.metadata['Target_variables'] = str(self.target_variables)
        self.metadata['Deep_averages'] = [self.deep_depth]
        self.metadata['Mid_averages'] = str(self.mid_depth)


    def get_target_data(self)-> None:
        print('Reading CSV file')
        bbmp_data = pd.read_csv(self.original_file_name)
        
        print('Extrating target data')
        target_data = bbmp_data.loc[:, self.target_variables]

        dates_type_datetime = pd.to_datetime(target_data.iloc[:, 0], format = self.date_format)
        target_data[self.original_datename] = dates_type_datetime

        dates_type_int:list[int] = [days.timestamp() for days in dates_type_datetime]
        target_data[self.groupby_datename] = dates_type_int
        self.target_data : dict[str, list[str|int]] = target_data


    def group_data(self) -> None:

        print('Updating target data and grouping by day')
        grouped_by_date = self.target_data.groupby(self.groupby_datename)

        nested_groups = {}
        for group_name, group_data in grouped_by_date:
            nested_groups[group_name] = group_data

        self.nested_groups : dict = nested_groups
        self.metadata['Profile_count'] = [len(self.nested_groups)]

        unique_depths = list(set(list(self.target_data.iloc[:, 1])))
        unique_depths.sort()

        self.unique_depths : list = unique_depths


    def normalize_depth_from_list(self, upress: list, data_frame):
        for p in upress:
            if p not in data_frame.iloc[:, 1].values:

                new_row = [ 
                    data_frame.iloc[0,0],
                    p,
                    float('nan'), 
                    float('nan'),
                    float('nan'),
                    data_frame.iloc[0,-1]
                ]

                new_df_row = pd.DataFrame(new_row).T
                new_df_row.columns = data_frame.columns.tolist()
                data_frame = pd.concat([data_frame, new_df_row], ignore_index=True)
            
        data_frame = data_frame.sort_values(by= self.original_pressure_name)

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
        print('Nomalizing depths and filling with NaN')
        data = self.nested_groups
        for key, values in data.items():
            data_frame = values
            data_frame = self.normalize_depth_from_list(self.unique_depths, data_frame)
            data_frame = self.check_duplicate_rows(data_frame)

            data[key] = data_frame

        self.normalized_data : dict = data

        normalized_depths = data[list(data.keys())[0]][self.original_pressure_name].tolist()
        normalized_dates = list(data.keys())

        self.normalized_dates : list= normalized_dates
        self.normalized_depth : list = normalized_depths


    def separate_target_variables(self, string_name: str): 
        print('Creating Target Variable Matrices')
        all_columns = np.transpose([values[string_name] for key, values in self.normalized_data.items()])

        return all_columns
    

    def get_variable_matrices(self) -> None:
        self.variables_matrices = [self.separate_target_variables(names) for names in self.target_variables[2:]]


    def data_transformations(self) -> None:
        
        matrix_list = self.variables_matrices

        print('Interpolating Data')

        transform_data = {}
        count = 0
        for matrix in matrix_list:
            pandas_matrix = pd.DataFrame(matrix)
            matrix_interpolated_axis0 = pandas_matrix.interpolate(axis=0).replace(0, np.nan)
            matrix_interpolated_axis10 = matrix_interpolated_axis0.interpolate(axis=1).replace(0, np.nan)
            matrix_diff = pd.DataFrame(np.diff(matrix_interpolated_axis10, axis=1)).replace(0, np.nan)

            normal_depths = np.array(self.normalized_depth)
            rows_bellow60 = list(np.where(normal_depths > self.deep_depth)[0])
            rows_over35 = list(np.where(normal_depths < self.mid_depth[1])[0])
            rows_under20 = list(np.where(normal_depths > self.mid_depth[0])[0])
            print(rows_under20)
            rows_btw20_35 = sorted(list(set(rows_over35+rows_under20)))
            print(rows_btw20_35)
            matrix_avg_below = matrix_diff.iloc[rows_bellow60, :].mean(axis=0)
            matrix_avg_btw = matrix_diff.iloc[rows_btw20_35, :].mean(axis=0)

            t_names = self.transformation_names

            transform_data[self.target_variables[count]+t_names[0]] = matrix_interpolated_axis0
            transform_data[self.target_variables[count]+t_names[1]] = matrix_interpolated_axis10
            transform_data[self.target_variables[count]+t_names[2]] = matrix_diff
            transform_data[self.target_variables[count]+t_names[3]] = matrix_avg_below
            transform_data[self.target_variables[count]+t_names[4]] = matrix_avg_btw
            
            count += 1

        self.transform_data : dict = transform_data


    def conform_schema(self) -> None:
        transformed_data = self.transform_data

        self.output_data = {
        'sample_diff_midrow_temp': transformed_data['temperature_avgmid_diff1_inter10'],
        'sample_diff_row_temp': transformed_data['temperature_avg_diff1_inter10'],
        'sample_matrix_temp': transformed_data['temperature_interpolated_axis10'],

        'sample_diff_midrow_salt': transformed_data['salinity_avgmid_diff1_inter10'],
        'sample_diff_row_salt': transformed_data['salinity_avg_diff1_inter10'],
        'sample_matrix_salt': transformed_data['salinity_interpolated_axis10'],
        
        'sample_diff_midrow_oxy': transformed_data['oxygen_avgmid_diff1_inter10'],
        'sample_diff_row_oxy': transformed_data['oxygen_avg_diff1_inter10'],
        'sample_matrix_oxy': transformed_data['oxygen_interpolated_axis10'],

        'sample_timestamps': self.normalized_dates,
        'sample_depth': self.normalized_depth,
    }
        

    def record_output_metadata(self) -> None:
        self.metadata['Output_dataset_path'] = self.output_file_path
        meta_processing = pd.DataFrame(self.metadata)

        with open(self.metadata_csv_path, 'r') as file:
            read = csv.reader(file)
            row_count = sum(1 for _ in read)

        if row_count == 0:
            meta_processing.to_csv(self.metadata_csv_path, mode='a', header=True, index=False)
        else:
            meta_processing.to_csv(self.metadata_csv_path, mode='a', header=False, index=False)

        joblib.dump(self.output_data, self.output_file_path)
        print(f'Saved as {self.output_file_name}')


def main():
    raw_name, deep_depth, mid_depth, date_format = get_command_line_args()
    raw_bbmp_data = '../data/RAW/' + raw_name

    # Make sure the first 2 variables are date and depth
    target_variables = ['time_string',
                        'pressure',
                        'salinity',
                        'temperature',
                        'oxygen']

    bbmp = Intrusion_ETL(raw_bbmp_data, target_variables, deep_depth, mid_depth, date_format)
    bbmp.get_target_data()
    bbmp.group_data()

    print(bbmp.nested_groups[list(bbmp.nested_groups.keys())[0]].head())

    bbmp.normalize_length_data()
    bbmp.get_variable_matrices()
    bbmp.data_transformations()
    bbmp.conform_schema()
    bbmp.record_output_metadata()

    return bbmp


if __name__ == '__main__':
    Data = main()

    
