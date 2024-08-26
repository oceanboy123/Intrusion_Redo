from abc import ABC, abstractmethod
from misc.request_arguments.request_info import RequestInfo
from dataclasses import dataclass

@dataclass
class ETL_method(ABC):
    data_info : RequestInfo

    # @abstractmethod
    # def __str__(self):
    #     x = self.metadata
    #     string_separated = [f'{i}: {j}' for i,j in x.items()]
    #     return f'Request Info: {string_separated}'
    
    @abstractmethod
    def GenerateMetadata(self) -> None:
        pass
