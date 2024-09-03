from abc import ABC, abstractmethod
from misc.request_arguments.request_info_ETL import RequestInfo_ETL
from dataclasses import dataclass

@dataclass
class ETL_method(ABC):
    """
    Abstract object representing a ETL method part of an ETL process

    ----------------Input
              data_info :   Request data
    """
    data_info : RequestInfo_ETL
    
    @abstractmethod
    def GenerateLog(self) -> None:
        """
        As named
        """
        ...
