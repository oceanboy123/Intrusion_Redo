from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Dict, Any

@dataclass
class RequestInfo(ABC):
    file_name: str

    # @abstractmethod
    # def __str__(self):
    #     x = self.metadata
    #     string_separated = [f'{i}: {j}' for i,j in x.items()]
    #     return f'Request Info: {string_separated}'