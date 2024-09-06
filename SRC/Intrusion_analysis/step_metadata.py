import csv
import time
import pandas as pd

from datetime import datetime
from misc.other.file_handling import count_csv_rows
from .config import *

@function_log
@dataclass
class meta(analysis_step):
    """
    The meta (*) class allows you to record metadata into multiple .csv files.
    This includes:

    ----------------Important fields
    table_coefficients_error_comb: Table used to record specific coefficient 
                                   data in 'coefficients_error.csv'

    NOTE: The results are not saved in dataset
    """
    table_coefficients_error_comb : Dict[str, Any] = field(default_factory=dict)
    
    meta_path = './data/PROCESSED/TABLES/'
    coeff_error_table = 'coefficients_error.csv'
    coeff_table = 'coefficients.csv'
    intrusions_table = 'intrusionID+effect.csv'
    est_intrusions_table = 'est_intrusionID+effect.csv'
    meta_table = 'metadata_intrusions.csv'
    

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
        dataset.identification.intrusions = {
            'input_dataset': dataset.path,
            'date_created': datetime.strptime(time.ctime(), 
                                              "%a %b %d %H:%M:%S %Y"),
            'init_year': [dataset.identification.uyears[0]],
            'end_year': [dataset.identification.uyears[-1]],
            'intrusion_type': [dataset.identification.intrusion_type],
            'manual_input_type': dataset.identification.manual_input_type,
            'manual_input_path': dataset.identification.manual_input,
            'manual_save': 'N/A',
            'manual_input_save': dataset.identification.save ,
            'variables_used': str(['salinity', 'temperature']),
            'date_error': [10]
            }

        dataset.identification.table_IDeffects = {
            'dates': dataset.identification.manualID_dates,
            'index': dataset.identification.effects.manualID_indices,
            'temp_effects': dataset.identification.effects.manualID_temp_effects,
            'salt_effects': dataset.identification.effects.manualID_salt_effects,
            }
        
        dataset.analysis.table_IDeffects = {
            'dates': [datetime(1,1,1)],
            'index': [-999],
            'temp_effects': [-999],
            'salt_effects': [-999],
            }

        dataset.analysis.table_coefficients = {
            'temp_coefficient': [dataset.analysis.OP_temp_coeff],
            'salt_coefficient': [dataset.analysis.OP_salt_coeff],
            'performance': [dataset.analysis.OP_performance]
            }

        result_comp = dataset.analysis.OP_performance_spec
        dataset.analysis.table_coefficients_error = {
            'Missed': result_comp['Only Manual'],
            'Extra': result_comp['Only Estimated'],
            'Found': result_comp['Matched']
            }
        
        error = dataset.analysis.table_coefficients_error
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

        
    def extract(self, dataset: RequestInfo_Analysis) -> None:
        """
        Record metadata in their corresponing .csv files
        """
        rows_error = len(self.table_coefficients_error_comb['dates'])
        row_num = count_csv_rows(self.meta_path+self.meta_table)
        rows_mintrusion = len(dataset.identification.table_IDeffects['dates'])
        rows_eintrusion = len(dataset.analysis.table_IDeffects['dates'])

        index = row_num
        head = False
        
        if row_num == 0:
            index = 1
            head = True

        
        dataset.identification.table_IDeffects['intrusion_ID'] = [
                                                        index]*rows_mintrusion
        dataset.analysis.table_IDeffects['intrusion_ID'] = [
                                                        index]*rows_eintrusion
        
        self.table_coefficients_error_comb['error'] = [index]*rows_error

        try:
            datam_ideffects = pd.DataFrame(
                dataset.identification.table_IDeffects)
        except:
            return
        
        datae_ideffects = pd.DataFrame(dataset.analysis.table_IDeffects)
        data_error = pd.DataFrame(self.table_coefficients_error_comb)

        datae_ideffects.to_csv(self.meta_path+self.est_intrusions_table,
                               mode='a', header=head, index=False)
        datam_ideffects.to_csv(self.meta_path+self.intrusions_table,
                               mode='a', header=head, index=False)
        data_error.to_csv(self.meta_path+self.coeff_error_table,
                           mode='a', header=head, index=False)
        
        self.record_single(self.meta_table, 
                           dataset.identification.intrusions)
        self.record_single(self.coeff_table, 
                           dataset.analysis.table_coefficients)
    

    def run(self, dataset: RequestInfo_Analysis) -> None:
        """
        Steps: integrate_metadata -> extract
        """
        self.integrate_metadata(dataset)
        self.extract(dataset) 