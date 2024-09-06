from datetime import datetime
from .config import *

@dataclass
class RequestInfo_ETL(RequestInfo):
    """
    This is the object representing the raw profile data. Here you will find the 
    data itself, the metadata, and the ETL request parameters.
    
    ----------------Inputs
    file_name   : .csv file path with profile data
    deep_depth  : Depth used to calculate depth-averages for bottom time
                    series (Check data_transformations.depth_averages in 
                    ETL_processes/ETL_data_transformation.py)
                    Imported (File generated from Manual)
    mid_depth1  : Depth used to calculate depth-averages for mid time
                    series. Specifically the top boundary
    mid_depth2  : Depth used to calculate depth-averages for mid time
                    series. Specifically the bottom boundary
    date_format : Date format of the .csv file

    ----------------Important class attributes\
    lineage     : Dictionary used to contruct metadata tables
    metadata    : Dictionary used to contruct metadata tables
    """
    
    deep_depth : str
    mid_depth1 : int
    mid_depth2 : int
    date_format : str

    metadata : Dict[str, Any] = field(default_factory=dict)
    lineage : Dict[str, Any] = field(default_factory=dict)

    dir_path = './data/RAW/' # Raw data path
    target_variables = ['time_string',
                        'pressure',
                        'salinity',
                        'temperature',
                        'oxygen'] # .csv target variable column names


    def __post_init__(self)-> None:
        self.file_data_path = self.dir_path + self.file_name
        self.raw_data = pd.read_csv(self.file_data_path)
        self.mid_depth = [self.mid_depth1, self.mid_depth2]
        self.GenerateMetadata()

    def GenerateMetadata(self) -> None:
        # Recording ETL strategy characteristics as metadata
        self.metadata['input_dataset'] = self.file_data_path
        self.metadata['date_created'] = datetime.strptime(time.ctime(), 
                                                          "%a %b %d %H:%M:%S %Y"
                                                          )
        self.metadata['deep_averages'] = [self.deep_depth]
        self.metadata['mid_averages'] = str(self.mid_depth)
        self.metadata['date_format'] = self.date_format
        self.metadata['target_variables'] = str(self.target_variables)