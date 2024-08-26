from other.file_handling import import_joblib
from other.date_handling import timestamp2datetime_lists
from .request_info import RequestInfo
from dataclasses import dataclass, field
import time
from typing import Dict, Any

# file_name, intrusion_type, id_type, analysis_type, coefficient_temp, coefficient_salt, save_manual, manual_input = get_command_line_args(varsin)

@dataclass
class RequestInfo_Analysis(RequestInfo):
    """
    TBD
    """
    intrusion_type : str
    id_type : str
    analysis_type : str
    coefficient_temp : int
    coefficient_salt : int
    save_manual : str
    manual_input : str
    dir_path = '../data/PROCESSED/'
    metadata : Dict[str, Any] = field(default_factory=dict)

    def __post_init__(self)-> None:
        self.file_data_path = self.dir_path + self.file_name
        self.data = import_joblib(self.file_data_path)
        self.dates_stamp = self.data[self.dates_name]
        self.dates = timestamp2datetime_lists(self.dates_stamp)
        self.coefficients = [self.coefficient_temp, self.coefficient_salt]

        self.metadata['Input_dataset'] = self.file_data_path
        self.metadata['Current_time'] = time.ctime()
        self.metadata['Intrusion_type'] = self.intrusion_type
        self.metadata['Analysis_type'] = self.analysis_type
        self.metadata['Coefficients'] = self.coefficients
        self.metadata['Save_manual'] = self.save_manual
        self.metadata['Manual_input'] = self.manual_input
        self.metadata['Variables_used'] = str(['salinity', 'temperature']) # Record Variables used
    

