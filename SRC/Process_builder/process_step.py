from abc import ABC, abstractmethod

class Step(ABC):
    @abstractmethod
    def run(self) -> None:
        """
        As named
        """
        ...

    @abstractmethod
    def GenerateLog(self) -> None:
        """
        As named
        """
        ...