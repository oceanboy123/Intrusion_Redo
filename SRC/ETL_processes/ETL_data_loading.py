import csv
import joblib

from misc.other.file_handling import count_csv_rows
from .config import *

@dataclass
class data_loading(ETL_method):
    """
    Final step of the ETL process: creates an intrusion data schema, 
    records metadata, and saves data to the './data/PROCESSED/' directory.

    Inputs
    - data_info             :An object containing data information. 
                             Acquired using the RequestInfo_ETL(RequestInfo) class
    - data_normalization    : Acquired using the data_normalization(ETL_method) class
    - data_transformation   : Acquired using the data_transformation(ETL_method) class

    Important class attributes
    - output_data           : Final data schema to be saved for analysis.
    - output_file_name      : Name of the primary output file.
    - output_file_name2     : Name of the secondary output file (lineage information)
    - metadata_csv          : Name of the CSV file to store metadata.
    - file_path             : Directory path where output files will be saved.
    """
    normalization : ETL_method
    transformation : ETL_method
    matrices : ETL_method
    extraction : ETL_method
    output_data : Dict[str, Any] = field(default_factory=dict)

    # Default file names and paths
    output_file_name: str = 'BBMP_selected_data0.pkl'
    output_file_name2: str = 'Lineage0.pkl'
    metadata_csv: str = 'metadata_processing.csv'
    file_path: str = './data/PROCESSED/'

    
    def __post_init__(self) -> None:
        self.output_file_path = self.file_path + self.output_file_name
        self.output_file_path2 = self.file_path + self.output_file_name2
        self.metadata_csv_path = self.file_path + self.metadata_csv

        self.run()

    
    def conform_schema(self) -> None:
        """
        Load data into predifined schema used in Intrusion_analysis.py.
        
        NOTE: if this the dict keys chnage, make sure to change them in
        the analysis python script
        """
        
        transformed_data = self.transformation.transform_data
        self.output_data = {
        # Temperature
        'sample_diff_midrow_temp': transformed_data.get(
                                    'temperature_avgmid_diff1_inter10', []),
        'sample_diff_row_temp': transformed_data.get(
                                    'temperature_avg_diff1_inter10', []),
        'sample_matrix_temp': transformed_data.get(
                                    'temperature_interpolated_axis10', []),
        # Salinity
        'sample_diff_midrow_salt': transformed_data.get(
                                    'salinity_avgmid_diff1_inter10', []),
        'sample_diff_row_salt': transformed_data.get(
                                    'salinity_avg_diff1_inter10', []),
        'sample_matrix_salt': transformed_data.get(
                                    'salinity_interpolated_axis10', []),
        # Oxygen
        'sample_diff_midrow_oxy': transformed_data.get(
                                    'oxygen_avgmid_diff1_inter10', []),
        'sample_diff_row_oxy': transformed_data.get(
                                    'oxygen_avg_diff1_inter10', []),
        'sample_matrix_oxy': transformed_data.get(
                                    'oxygen_interpolated_axis10', []),
        # Time and Depth
        'sample_timestamps': self.normalization.normalized_dates,
        'sample_depth': self.normalization.normalized_depth,
        }
        

    def conform_schemav2(self) -> None:
        """
        Load data into predifined schema used in Intrusion_analysis.py.
        
        NOTE: if this the dict keys chnage, make sure to change them in
        the analysis python script
        """
        etl_process = ' -> '.join([
            type(self.data_info).__name__,
            type(self.extraction).__name__,
            type(self.normalization).__name__,
            type(self.matrices).__name__,
            type(self.transformation).__name__
        ])
    
        transformed_data = self.transformation.transform_data
        dep_avg_list = [
            [   # Deep depths averages
                transformed_data['temperature_avg_diff1_inter10'],
                transformed_data['salinity_avg_diff1_inter10'],
                transformed_data['oxygen_avg_diff1_inter10']
                ],
            [   # Mid depths averages
                transformed_data['temperature_avgmid_diff1_inter10'],
                transformed_data['salinity_avgmid_diff1_inter10'],
                transformed_data['oxygen_avgmid_diff1_inter10']
                ]
        ]

        interpolated_list = [
            transformed_data.get('temperature_interpolated_axis10', []),
            transformed_data.get('salinity_interpolated_axis10', []),
            transformed_data.get('oxygen_interpolated_axis10', [])
        ]

        self.data_info.lineage = {
            'normalized'    : self.matrices.variables_matrices,
            'interpolated'  : interpolated_list,
            'dates'         : self.normalization.normalized_dates,
            'depths'        : self.normalization.normalized_depth,
            'depth_avg'     : dep_avg_list,
            'etl_process'   : [etl_process]
        }


    def record_output_metadata(self) -> None:
        """
        Record metadata and save file for analysis
        """

        self.data_info.metadata['output_dataset_path'] = self.output_file_path
        row_count = count_csv_rows(self.metadata_csv_path)

        # Assign a processing ID
        processing_id = row_count + 1 if row_count else 1
        self.data_info.metadata['processing_ID'] = processing_id
        meta_processing = pd.DataFrame([self.data_info.metadata])

        # Record metadata
        meta_processing.to_csv(
            self.metadata_csv_path,
            mode='a',
            header=not row_count,
            index=False
        )

        # Save .pkl file for analysis
        joblib.dump(self.output_data, self.output_file_path)
        joblib.dump(self.data_info.lineage, self.output_file_path2)
        

    def run(self) -> None:
        """
        Executes the data loading process:
        - Conforms the data to the required schema.
        - Prepares additional data structures for analysis.
        - Records metadata and saves output files.
        """
        self.conform_schema()
        self.conform_schemav2()
        self.record_output_metadata()


    def GenerateLog(self, logger: Logger) -> None:
        """
        Logs the output file names
        """
        logger.info(f'Saved processed data as {self.output_file_name}')
        logger.info(f'Saved lineage information as {self.output_file_name2}')