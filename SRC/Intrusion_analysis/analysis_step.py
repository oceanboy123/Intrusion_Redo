from abc import ABC, abstractmethod

class analysis_step(ABC):
    
    @abstractmethod
    def extract(self, dataset: object) -> None:
        ...

    @abstractmethod
    def run(self, dataset: object) -> None:
        ...