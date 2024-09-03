from abc import ABC, abstractmethod
from dataclasses import dataclass

@dataclass
class id_method(ABC):
    """
    Abstract object representing a ETL process method for identifying intrusions
    in profile data. 

    """
    intrusion_type : str
    
    @abstractmethod
    def fill_request_info(self, dates) -> None:
        """
        Extract required fields from RequestInfo
        """
        ...