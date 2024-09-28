from .builder_interface import ProcessBuilder
from .config import (
    # ETL process
    RequestInfo_ETL, 
    data_extraction, 
    data_normalization, 
    timedepth_space, 
    data_transformation, 
    data_loading,

    # Intrusion Analysis
    RequestInfo_Analysis,
    imported_identification,
    manual_identification, 
    intrusion_analysis,
    intrusion_data,
    meta
)


class DataETL(ProcessBuilder):
    def __init__(self) -> None:
        super().__init__()
        self.request = RequestInfo_ETL
        self.steps = [
            data_extraction, 
            data_normalization, 
            timedepth_space, 
            data_transformation
        ]
        self.meta_steps = [data_loading]
        self.__defaultargs__ = {
            'file_name'         : 'bbmp_aggregated_profiles.csv',
            'deep_depth'        : 60,
            'mid_depths1'       : 20,
            'mid_depths2'       : 35,
            'date_format'       : '%Y-%m-%d %H:%M:%S',
        } 


class IntrusionAnalaysis(ProcessBuilder):
    def __init__(self, identification: int = 0) -> None:
        super().__init__()
        self.request = RequestInfo_Analysis
        self.steps = [
            imported_identification, 
            manual_identification, 
            intrusion_analysis, 
            intrusion_data
        ]
        self.meta_steps = [meta]
        self.__defaultargs__ = {
            'file_name'         : 'BBMP_selected_data0.pkl',
            'intrusion_type'    : 'NORMAL',
            'id_type'           : 'MANUAL',
            'analysis_type'     : 'GET_COEFFICIENTS',
            'coefficient_temp'  : 0.5,
            'coefficient_salt'  : 0.5,
            'save_manual'       : 'OFF',
            'manual_input'      : 'manualID_NORMAL1724797813.pkl'
        }
        
        if identification == 0:
            self.steps.pop(identification)
        elif identification == 1:
            self.steps.pop(identification)
        else:
            raise ValueError("Identification Type Unknown:"
                             +" Please select (0) for manual,"
                             +" or (1) for imported identification")
