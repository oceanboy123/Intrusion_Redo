from abc import ABC, abstractmethod
from dataclasses import dataclass
from misc.request_arguments.request_info_ETL import RequestInfo_ETL

@dataclass
class ETL_method(ABC):
    """
    Inputs
    - data_info     : Acquired using the RequestInfo_ETL(RequestInfo) class
    """
    data_info : RequestInfo_ETL
    
    @abstractmethod
    def GenerateLog(self) -> None:
        """
        As named
        """
        ...
