from abc import ABC, abstractmethod
from misc.request_arguments.request_info import RequestInfo
from logging import Logger

class Step(ABC):
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