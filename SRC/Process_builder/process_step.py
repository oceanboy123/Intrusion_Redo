from abc import ABC, abstractmethod
from dataclasses import dataclass
from misc.request_arguments.request_info import RequestInfo
from logging import Logger

@dataclass
class Step(ABC):

    data_info : RequestInfo
    
    @abstractmethod
    def run(self, RequestInfo) -> None:
        """
        As named
        """
        ...

    @abstractmethod
    def GenerateLog(self, logger: Logger) -> None:
        """
        As named
        """
        ...