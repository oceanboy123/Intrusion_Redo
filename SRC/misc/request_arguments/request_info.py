from abc import ABC, abstractmethod
from dataclasses import dataclass

@dataclass
class RequestInfo(ABC):
    """
    Abstract object representing a request to perform a process with data, and the data itself. 

    ----------------Input
              file_name :   data to be processed
    """
    file_name: str

    abstractmethod
    def GenerateMetadata(self) -> None:
        """
        Record request characteristics and metadata
        """
        ...
