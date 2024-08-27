import time
import pandas as pd
from .request_info import RequestInfo
from dataclasses import dataclass, field
from typing import Dict, Any

@dataclass
class RequestInfo_ETL(RequestInfo):
    """
    TBD
    """
    deep_depth : str
    mid_depth1 : int
    mid_depth2 : int
    date_format : str
    target_variables = ['time_string',
                        'pressure',
                        'salinity',
                        'temperature',
                        'oxygen']
    dir_path = './data/RAW/'
    metadata : Dict[str, Any] = field(default_factory=dict)

    def __post_init__(self)-> None:
        
        # logger.info('\nReading CSV file')
        self.file_data_path = self.dir_path + self.file_name
        self.raw_data = pd.read_csv(self.file_data_path)
        self.mid_depth = [self.mid_depth1, self.mid_depth2]

        # Recording ETL strategy characteristics as metadata
        self.metadata['Input_dataset'] = self.file_data_path
        self.metadata['Date_created'] = time.ctime()
        self.metadata['Deep_averages'] = [self.deep_depth]
        self.metadata['Mid_averages'] = str(self.mid_depth)
        self.metadata['date_format'] = self.date_format
        self.metadata['Target_variables'] = str(self.target_variables)