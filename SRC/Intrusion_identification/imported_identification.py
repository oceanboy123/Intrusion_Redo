import numpy as np
from dataclasses import dataclass, field
from typing import Dict, Any, List
from misc.other.logging import function_log
from misc.other.file_handling import *
from .id_method import id_method

@function_log
@dataclass
class imported_identification(id_method):
    intrusion_type : str
    manual_input : str
    
    save : str = 'OFF'
    manualID_dates : List[int] = field(default_factory=list)
    table_IDeffects : Dict[str, Any] = field(default_factory=dict)
    intrusions : Dict[str, Any] = field(default_factory=dict)
    effects : object = field(default_factory=empty)

    def fill_request_info(self, dates) -> None:
        self.uyears  = np.unique([dt.year for dt in dates])
        self.manualID_dates = import_joblib(self.manual_input)
        self.manual_input_type = 'IMPORTED'
    
    def extract(self, dataset) -> None:
        dataset.identification = self
    

    def run(self, dataset):
        self.fill_request_info(dataset.dates)
        self.extract(dataset)