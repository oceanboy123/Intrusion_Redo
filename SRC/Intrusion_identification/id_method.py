# Imports
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Dict, List, Any

# Blank and ABC classes
from misc.other.file_handling import blank
from Requests.request_info_analysis import RequestInfo_Analysis

@dataclass
class IntrusionID_Type(ABC):
    """
    Abstract object representing a ETL process method for identifying intrusions
    in profile data. 

    ----------------Important class attributes
    - manualID_dates    : Dates identified
    - table_IDeffects   : Table for intrusion effects ('intrusionID+effect.csv')
    - intrusions        : Table for characteristics of the Analysis request 
                          ('metadata_intrusions.csv')
    - effects           : Class(id_method(ABC))
    - uyears            : Years of data available
    - manual_input_type : IMPORTED or MANUAL
    - intrusion_type    : NORMAL or Other(Bottom Water) and MID (Mid Water)
    - manual_input      : Intrusion Identification file path (for IMPORTED)
    - cache_output      : Output path of temporary identification object
    """

    manualID_dates    : List[int] = field(init=False)
    table_IDeffects   : Dict[str, Any] = field(default_factory=dict)
    intrusions        : Dict[str, Any] = field(default_factory=dict)
    effects           : object = field(default_factory=blank)
    uyears            : Any = field(init=False)
    manual_input_type : str =  field(init=False)
    intrusion_type    : str =  field(init=False)
    manual_input      : str =  field(init=False)
    cache_output      : str = (
        '../data/CACHE/Processes/Analysis/temp_identification.pkl'
    )
    
    @abstractmethod
    def fill_request_info(self, dataset: RequestInfo_Analysis) -> None:
        """
        Extract required fields from RequestInfo
        """
        ...