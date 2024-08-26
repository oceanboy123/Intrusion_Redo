import csv
import joblib
import pandas as pd
from dataclasses import dataclass, field
from typing import Dict, Any, List
from .ETL_method import ETL_method

@dataclass
class data_loading(ETL_method):
    """
    TBD
    """
    data_normalization : ETL_method
    data_transformation : ETL_method
    output_data : Dict[str, Any] = field(default_factory=dict)

    output_file_name = 'BBMP_salected_data0.pkl'
    output_file_path = './data/PROCESSED/' + 'BBMP_salected_data0.pkl'
    metadata_csv = 'metadata_processing.csv'
    metadata_csv_path = './data/PROCESSED/' + 'metadata_processing.csv'

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
        # logger.info(f'Saved as {self.output_file_name}')

        # logger.debug(f'\nMetadata: \n{meta_processing.head()}')

    def run(self) -> None:
        self.conform_schema()
        self.record_output_metadata()

    def GenerateMetadata(self) -> None:
        return "Metadata Generated"