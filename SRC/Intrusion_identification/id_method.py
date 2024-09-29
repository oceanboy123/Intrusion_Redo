from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Dict, List, Any
from datetime import datetime
from misc.other.file_handling import empty
from Requests.request_info_analysis import RequestInfo_Analysis

@dataclass
class IntrusionID_Type(ABC):
    """
    Abstract object representing a ETL process method for identifying intrusions
    in profile data. 

    ----------------Input
     intrusion_type :   Normal (Deep), 
                        Mid (Mid-depth), 
                        Other (Deep; e.g., Winter)
    """

    manualID_dates  : List[int] = field(default_factory=list)
    table_IDeffects : Dict[str, Any] = field(default_factory=dict)
    intrusions      : Dict[str, Any] = field(default_factory=dict)
    effects         : object = field(default_factory=empty)
    cache_output    : str = (
        '../data/CACHE/Processes/Analysis/temp_identification.pkl'
        )
    uyears          : Any = field(init=False)
    manual_input_type : str =  field(init=False)
    
    @abstractmethod
    def fill_request_info(self, dataset: RequestInfo_Analysis) -> None:
        """
        Extract required fields from RequestInfo
        """
        ...