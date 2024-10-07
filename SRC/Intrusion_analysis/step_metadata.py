from .config import (
    # Imports
    ABC,
    Logger,
    dataclass,
    field,
    pd,
    
    # Typing
    List,
    Dict,
    Any,
    
    # Wrapper
    function_log,
    
    # ABC Class
    Step,
    RequestInfo_Analysis,
    
    # Custom Functions
    import_joblib,
    count_csv_rows
)

import csv
import time
from datetime import datetime

@dataclass
class LoadAnalysis_Type(ABC):
    """
    ----------------Important fields
    meta_intrusion               : Metadata Table for the Intrusion Analysis
    intrusions                   : Manually identify intrusion Table
    estimated_intrusions         : Estimated intrusion Table
    coefficients                 : Coefficient Table
    coefficients_error           : Detailed Performance Coefficient Table
    table_coefficients_error_comb: Table used to record specific coefficient 
                                   data in 'coefficients_error.csv'
    analysis_id                  : Analysis Identification Number

    meta_path             : Path of all csv tables
    coeff_error_table     : csv file name 
    coeff_table           : ""
    intrusions_table      : ""
    est_intrusions_table  : ""
    meta_table            : ""
    
    required_data         : Input path of temporary required object
    cache_output          : Output path of temporary identification object
    """
    meta_intrusions               : Dict[str, Any] = field(init=False)
    intrusions                    : Dict[str, Any] = field(init=False)
    estimated_intrusions          : Dict[str, Any] = field(init=False)
    coefficients                  : Dict[str, Any] = field(init=False)
    coefficients_error            : Dict[str, Any] = field(init=False)
    table_coefficients_error_comb : Dict[str, Any] = field(default_factory=dict)
    analysis_id                   : int = 0
    
    meta_path            : str = './data/PROCESSED/TABLES/'
    coeff_error_table    : str = 'coefficients_error.csv'
    coeff_table          : str = 'coefficients.csv'
    intrusions_table     : str = 'intrusionID+effect.csv'
    est_intrusions_table : str = 'est_intrusionID+effect.csv'
    meta_table           : str = 'metadata_intrusions.csv'
    required_data     : List[str] = field(
        default_factory=lambda: 
        ['../data/CACHE/Processes/Analysis/temp_identification.pkl',
        '../data/CACHE/Processes/Analysis/temp_coefficient.pkl',
        '../data/CACHE/Processes/Analysis/temp_effects.pkl']
    )
    cache_output : str = None

@function_log
@dataclass
class meta(Step, LoadAnalysis_Type):
    """
    The meta (*) class allows you to record metadata into multiple .csv files.
    This includes:
    """

    def __post_init__(self, dataset: RequestInfo_Analysis) -> None:
        self.identification = import_joblib(self.required_data[0])
        self.analysis = import_joblib(self.required_data[1])
        self.effects = import_joblib(self.required_data[2])
        self.run(dataset)

    def record_single(self, table: str, dicts: dict) -> None:
        """
        Record single row metadata
        """
        table_path = self.meta_path+table
        row_num1 = count_csv_rows(table_path)

        if row_num1 == 0:
            dicts['intrusion_ID'] = 1
            dataf= pd.DataFrame(dicts)
            dataf.to_csv(table_path,mode='a', header=True, index=False)
        else:
            dicts['intrusion_ID'] = row_num1
            dataf= pd.DataFrame(dicts)
            dataf.to_csv(table_path,mode='a', header=False, index=False)

    def integrate_metadata(self, dataset: RequestInfo_Analysis) -> None:
        """
        Integrate metadata from multiple sources
        """
        self.meta_intrusions = {
            'input_dataset': dataset.path,
            'date_created': datetime.strptime(time.ctime(), 
                                              "%a %b %d %H:%M:%S %Y"),
            'init_year': [self.identification.uyears[0]],
            'end_year': [self.identification.uyears[-1]],
            'intrusion_type': [dataset.intrusion_type],
            'manual_input_type': dataset.id_type,
            'manual_input_path': dataset.manual_input,
            'manual_save': 'N/A',
            'manual_input_save': dataset.save_manual ,
            'variables_used': str(['salinity', 'temperature']),
            'date_error': [10]
            }

        self.intrusions = {
            'dates': self.identification.manualID_dates,
            'index': self.effects.manualID_indices,
            'temp_effects': self.effects.manualID_temp_effects,
            'salt_effects': self.effects.manualID_salt_effects,
            }
        
        self.estimated_intrusions = {
            'dates': [datetime(1,1,1)],
            'index': [-999],
            'temp_effects': [-999],
            'salt_effects': [-999],
            }

        self.coefficients = {
            'temp_coefficient': [self.analysis.OP_temp_coeff],
            'salt_coefficient': [self.analysis.OP_salt_coeff],
            'performance': [self.analysis.OP_performance]
            }

        result_comp = self.analysis.OP_performance_spec
        self.coefficients_error = {
            'Missed': result_comp['Only Manual'],
            'Extra': result_comp['Only Estimated'],
            'Found': result_comp['Matched']
            }
        
        error = self.coefficients_error
        rows_missed = len(error['Missed'])
        rows_extra = len(error['Extra'])
        rows_found = len(error['Found'])
        
        self.table_coefficients_error_comb = {
            'type' : ['Missed']*rows_missed + 
                     ['Extra']*rows_extra + 
                     ['Found']*rows_found,
            'dates' : list(error['Missed']) + 
                      list(error['Extra']) + 
                      [sub[-1] for sub in list(error['Found'])]
        }
        
    def extract(self) -> None:
        """
        Record metadata in their corresponing .csv files
        """
        rows_error = len(self.table_coefficients_error_comb['dates'])
        row_num = count_csv_rows(self.meta_path+self.meta_table)
        rows_mintrusion = len(self.intrusions['dates'])
        rows_eintrusion = len(self.estimated_intrusions['dates'])

        index = row_num
        head = False
        
        if row_num == 0:
            index = 1
            head = True

        
        self.intrusions['intrusion_ID'] = [index]*rows_mintrusion
        self.estimated_intrusions['intrusion_ID'] = [index]*rows_eintrusion
        self.table_coefficients_error_comb['error'] = [index]*rows_error

        try:
            datam_ideffects = pd.DataFrame(
                self.intrusions)
        except:
            return
        
        datae_ideffects = pd.DataFrame(self.estimated_intrusions)
        data_error = pd.DataFrame(self.table_coefficients_error_comb)

        datae_ideffects.to_csv(self.meta_path+self.est_intrusions_table,
                               mode='a', header=head, index=False)
        datam_ideffects.to_csv(self.meta_path+self.intrusions_table,
                               mode='a', header=head, index=False)
        data_error.to_csv(self.meta_path+self.coeff_error_table,
                           mode='a', header=head, index=False)
        
        self.record_single(self.meta_table, 
                           self.meta_intrusions)
        self.record_single(self.coeff_table, 
                           self.coefficients)
        self.analysis_id = self.meta_intrusions['intrusion_ID']

    def run(self, dataset: RequestInfo_Analysis) -> None:
        """
        Steps: integrate_metadata -> extract
        """
        self.integrate_metadata(dataset)
        self.extract()

    def GenerateLog(self, logger: Logger) -> None:
        """
        Logs the metadata information.
        """
        logger.info('Process Metadata Recorded and Data Saved')