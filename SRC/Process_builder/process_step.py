from abc import ABC, abstractmethod
from logging import Logger

class Step(ABC):
    @abstractmethod
    def run(self) -> None:
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
