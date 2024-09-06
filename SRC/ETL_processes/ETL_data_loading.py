import csv
import joblib

from misc.other.file_handling import count_csv_rows
from .config import *

@dataclass
class data_loading(ETL_method):
    """
    Creates an intrusion data schema, records metadata from the ETL, and saves
    data in ./data/PROCESSED/

    Inputs
    - data_info : Acquired using the RequestInfo_ETL(RequestInfo) class
    - data_normalization : Acquired using the data_normalization(ETL_method) class
    - data_transformation : Acquired using the data_transformation(ETL_method) class

    Important class attributes
    - output_data : Final step of ETL process, as named
    """
    normalization : ETL_method
    transformation : ETL_method
    matrices : ETL_method
    extraction : ETL_method
    output_data : Dict[str, Any] = field(default_factory=dict)

    output_file_name = 'BBMP_salected_data0.pkl'
    output_file_path = './data/PROCESSED/' + 'BBMP_salected_data0.pkl'
    metadata_csv = 'metadata_processing.csv'
    metadata_csv_path = './data/PROCESSED/' + 'metadata_processing.csv'

    
    def __post_init__(self) -> None:
        self.run()

    
    def conform_schema(self) -> None:
        """
        Load data into predifined schema used in Intrusion_analysis.py.
        
        NOTE: if this the dict keys chnage, make sure to change them in
        the analysis python script
        """
        
        transformed_data = self.transformation.transform_data
        self.output_data = {
        'sample_diff_midrow_temp': transformed_data['temperature_avgmid_diff1_inter10'],
        'sample_diff_row_temp': transformed_data['temperature_avg_diff1_inter10'],
        'sample_matrix_temp': transformed_data['temperature_interpolated_axis10'],

        'sample_diff_midrow_salt': transformed_data['salinity_avgmid_diff1_inter10'],
        'sample_diff_row_salt': transformed_data['salinity_avg_diff1_inter10'],
        'sample_matrix_salt': transformed_data['salinity_interpolated_axis10'],

        'sample_timestamps': self.normalization.normalized_dates,
        'sample_depth': self.normalization.normalized_depth,
        }
        
        try:
            self.output_data['sample_diff_midrow_oxy'] = transformed_data['oxygen_avgmid_diff1_inter10']
            self.output_data['sample_diff_row_oxy'] = transformed_data['oxygen_avg_diff1_inter10']
            self.output_data['sample_matrix_oxy'] = transformed_data['oxygen_interpolated_axis10']
        except Exception:
            self.output_data['sample_diff_midrow_oxy'] = []
            self.output_data['sample_diff_row_oxy'] = []
            self.output_data['sample_matrix_oxy'] = []
        

    def conform_schemav2(self) -> None:
        etl: List[List] = [
        f'{type(self.data_info).__name__}'         +' -> '+
        f'{type(self.extraction).__name__}'      +' -> '+
        f'{type(self.normalization).__name__}'   +' -> '+
        f'{type(self.matrices).__name__}'        +' -> '+
        f'{type(self.transformation).__name__}'
        ]
    
        transformed_data = self.transformation.output_data
        dep_avg_list: List[List]= [
            [
                transformed_data['temperature_avg_diff1_inter10'],
                transformed_data['sainity_avg_diff1_inter10'],
                transformed_data['oxygen_avg_diff1_inter10']
                ],
            [
                transformed_data['temperature_midavg_diff1_inter10'],
                transformed_data['sainity_midavg_diff1_inter10'],
                transformed_data['oxygen_midavg_diff1_inter10']
                ]
            ]

        interpolated_list: List[List]= [
            transformed_data['temperature_interpolated_axis10'],
            transformed_data['salinity_interpolated_axis10'],
            transformed_data['oxygen_interpolated_axis10']
            ]

        self.data_info.lineage = {
                'normalized'    : self.matrices.variables_matrices,
                'interpolated'  : interpolated_list,
                'dates'         : self.normalization.normalized_dates,
                'depths'        : self.normalization.normalized_depth,
                'depth_avg'     : dep_avg_list,
                'etl_process'   : etl
                }


    def record_output_metadata(self) -> None:
        """
        Record metadata and save file for analysis
        """

        self.data_info.metadata['output_dataset_path'] = self.output_file_path
        row_count = count_csv_rows(self.metadata_csv_path)

        # Record metadata
        if row_count == 0:
            self.data_info.metadata['processing_ID'] = 1
            meta_processing = pd.DataFrame(self.data_info.metadata)
            meta_processing.to_csv(self.metadata_csv_path, 
                                   mode='a', header=True, index=False)
        else:
            self.data_info.metadata['processing_ID'] = row_count
            meta_processing = pd.DataFrame(self.data_info.metadata)
            meta_processing.to_csv(self.metadata_csv_path, 
                                   mode='a', header=False, index=False)

        # Save .pkl file for analysis
        joblib.dump(self.output_data, self.output_file_path)
        

    def run(self) -> None:
        """
        Steps: conform_schema -> record_output_metadata
        """
        self.conform_schema()
        self.record_output_metadata()


    def GenerateLog(self, logger: Logger) -> None:
        """
        Log self.output_file_name
        """
        logger.info(f'Saved as {self.output_file_name}')