from abc import ABC, abstractmethod
from misc.request_arguments.request_info import RequestInfo
from dataclasses import dataclass

@dataclass
class ETL_method(ABC):
    data_info : RequestInfo
    
    @abstractmethod
    def GenerateLog(self) -> None:
        ...
