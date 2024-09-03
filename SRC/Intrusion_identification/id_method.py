from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime

@dataclass
class id_method(ABC):
    """
    Abstract object representing a ETL process method for identifying intrusions
    in profile data. 

    ----------------Input
     intrusion_type :   Normal (Deep), 
                        Mid (Mid-depth), 
                        Other (Deep; e.g., Winter)
    """
    intrusion_type : str
    
    @abstractmethod
    def fill_request_info(self, dates: list[datetime]) -> None:
        """
        Extract required fields from RequestInfo
        """
        ...