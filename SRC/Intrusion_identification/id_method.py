from abc import ABC, abstractmethod
from dataclasses import dataclass

@dataclass
class id_method(ABC):
    
    @abstractmethod
    def fill_request_info(self, dates) -> None:
        ...