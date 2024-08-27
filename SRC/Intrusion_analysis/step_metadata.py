import csv
import time
import pandas as pd
from dataclasses import dataclass, field
from typing import Dict, Any
from misc.other.logging import function_log
from .analysis_step import analysis_step

@function_log
@dataclass
class meta(analysis_step):
    """
    The meta (*) class allows you to record metadata into multiple .csv files.
    This includes:

    - *.table_coefficients_error_comb: Table used to record specific coefficient 
    data in .csv

    NOTE: The results are not saved in dataset
    """
    table_coefficients_error_comb : Dict[str, Any] = field(default_factory=dict)
    
    meta_path = './data/PROCESSED/TABLES/'
    coeff_error_table = 'coefficients_error.csv'
    coeff_table = 'coefficients.csv'
    intrusions_table = 'intrusionID+effect.csv'
    meta_table = 'metadata_intrusions.csv'

    @staticmethod
    def count_csv_rows(path) -> int:
        """Count number of rows to identify the new recording's index"""
        with open(path,'r') as file:
            read = csv.reader(file)
            row_count = sum(1 for _ in read)

        return row_count
    

    def record_single(self, table, dicts) -> None:
        """Record single row metadata"""
        table_path = self.meta_path+table
        row_num1 = self.count_csv_rows(table_path)

        if row_num1 == 0:
            dicts['ID'] = 1
            dataf= pd.DataFrame(dicts)
            dataf.to_csv(table_path,mode='a', header=True, index=False)
        else:
            dicts['ID'] = row_num1
            dataf= pd.DataFrame(dicts)
            dataf.to_csv(table_path,mode='a', header=False, index=False)

    def integrate_metadata(self, dataset) -> None:
        dataset.identification.intrusions = {
            'Input_dataset' : dataset.path,
            'Current_time' : time.ctime(),
            'Init_year' : [dataset.identification.uyears[0]],
            'End_year' : [dataset.identification.uyears[-1]],
            'Intrusion_type' : [dataset.identification.intrusion_type],
            'manual_input_type' : dataset.identification.manual_input_type,
            'manual_input_path' : dataset.identification.manual_input,
            'manual_input_save' : dataset.identification.save ,
            'Variables_used' : str(['salinity', 'temperature'])
            }

        dataset.identification.table_IDeffects = {
            'Dates' : dataset.identification.manualID_dates,
            'Index' : dataset.identification.effects.manualID_indices,
            'Temp_effects' : dataset.identification.effects.manualID_temp_effects,
            'Salt_effects' : dataset.identification.effects.manualID_salt_effects,
            }

        dataset.analysis.table_coefficients = {
            'Temp_coefficient' : [dataset.analysis.OP_temp_coeff],
            'Salt_coefficient' : [dataset.analysis.OP_salt_coeff],
            'Performance' : [dataset.analysis.OP_performance]
            }

        result_comp = dataset.analysis.OP_performance_spec
        dataset.analysis.table_coefficients_error = {
            'Missed' : result_comp['Only Manual'],
            'Extra' : result_comp['Only Estimated'],
            'Found' : result_comp['Matched']
            }
    

    def record_metadata(self, dataset) -> None:
        """Record all the metadata into their corresponding .csv file"""

        error = dataset.analysis.table_coefficients_error
        rows_missed = len(error['Missed'])
        rows_extra = len(error['Extra'])
        rows_found = len(error['Found'])
        
        self.table_coefficients_error_comb = {
            'Type' : ['Missed']*rows_missed + ['Extra']*rows_extra + ['Found']*rows_found,
            'Dates' : list(error['Missed']) + list(error['Extra']) + [sub[-1] for sub in list(error['Found'])]
        }

        rows_error = len(self.table_coefficients_error_comb['Dates'])
        row_num = self.count_csv_rows(self.meta_path+self.meta_table)
        rows_intrusion = len(dataset.identification.table_IDeffects['Dates'])

        index = row_num
        head = False
        
        if row_num == 0:
            index = 1
            head = True

        dataset.identification.table_IDeffects['ID'] = [index]*rows_intrusion
        self.table_coefficients_error_comb['Error'] = [index]*rows_error
        
        dataf_ideffects = pd.DataFrame(dataset.identification.table_IDeffects)
        dataf_error = pd.DataFrame(self.table_coefficients_error_comb)
        dataf_ideffects.to_csv(self.meta_path+self.intrusions_table,mode='a', header=head, index=False)
        dataf_error.to_csv(self.meta_path+self.coeff_error_table,mode='a', header=head, index=False)
        
        self.record_single(self.meta_table, dataset.identification.intrusions)
        self.record_single(self.coeff_table, dataset.analysis.table_coefficients)

    def extract(self, dataset: object) -> None:
        ...
    
    def run(self, dataset: object) -> None:
        self.integrate_metadata(dataset)
        self.record_metadata(dataset) 