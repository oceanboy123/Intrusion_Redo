from .config import *
from misc.other.file_handling import *
from misc.other.date_handling import timestamp2datetime_lists
from typing import List, Dict, Any, Tuple
from datetime import datetime

@dataclass
class RequestInfo_Analysis(RequestInfo):
    """
    This is the object representing the profile data. Here you will find the 
    data itself, the metadata, and the analysis request parameters. In the same 
    place you can find the results for the identification and the analysis of
    intrusion events (which will remain empty until you apply those methods
    on this object)
    
    ----------------Inputs
    file_name           : .pkl file path with profile data generated by 
                          main_ETL
    intrusion_type      : Normal (Deep), 
                          Mid (Mid-depth), 
                          Other (Deep;e.g., Winter)
    id_type             : Manual (You pick) or 
                          Imported (File generated from Manual)
    analysis_type       : GET_COEFFICIENTS (Coefficients estimation) or 
                          USE_COEFFICIENTS (Provide coefficients)
    coefficient_temp    : Coefficient for USE_COEFFICIENTS
    coefficient_salt    : Coefficient for USE_COEFFICIENTS
    save_manual         : ON for save the results of Manual
    manual_input        : .pkl file path for Imported

    ----------------Important class attributes
    metadata            : Dictionary used to contruct metadata tables
    identification      : Class(id_method(ABC))
    analysis            : Class(analysis_step(ABC))
    """

    intrusion_type      : str
    id_type             : str
    analysis_type       : str
    coefficient_temp    : int
    coefficient_salt    : int
    save_manual         : str
    manual_input        : str

    metadata            : Dict[str, Any]    = field(default_factory=dict)
    identification      : object            = field(default_factory=empty)
    analysis            : object            = field(default_factory=empty)

    dir_path = './data/PROCESSED/' # Processed data path
    dates_name = 'sample_timestamps' # Date field name based on ETL_data_loading

    def __post_init__(self)-> None:
        self.path: str = self.dir_path + self.file_name

        try:
            self.data: Dict[str, Any] = import_joblib(self.path)
            self.dates_stamp: List[int] = self.data[self.dates_name]
            self.dates: List[datetime] = timestamp2datetime_lists(
                                                               self.dates_stamp)
        except FileNotFoundError:
            raise FileNotFoundError(f"The file {self.path} does not exist.")
        except KeyError:
            raise KeyError(
                    f"The key '{self.dates_name}' was not found in the data.")
        except Exception as e:
            raise RuntimeError(
                            f"An error occurred while processing the file: {e}")
        
        self.coefficients : Tuple[int, int] = (self.coefficient_temp, 
                                              self.coefficient_salt)
        
        self.GenerateMetadata()

    def GenerateMetadata(self) -> None:
        """
        Recording Intrusion analysis characteristics as metadata
        """
    
        self.metadata['input_dataset' ] = self.path
        self.metadata['date_created'  ] = datetime.now().strftime(
                                                            "%Y-%m-%d %H:%M:%S")
        self.metadata['intrusion_type'] = self.intrusion_type
        self.metadata['analysis_type' ] = self.analysis_type
        self.metadata['coefficients'  ] = self.coefficients
        self.metadata['save_manual'   ] = self.save_manual
        self.metadata['manual_input'  ] = self.manual_input
        self.metadata['variables_used'] = str(['salinity', 'temperature'])
        self.metadata['date_error'    ] = 10
    

