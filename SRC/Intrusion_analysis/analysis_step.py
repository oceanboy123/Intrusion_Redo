from abc import ABC, abstractmethod
from misc.request_arguments.request_info_analysis import RequestInfo_Analysis

class analysis_step(ABC):
    """
    Abstract object representing a step in the intrusion analysis process
    """

    @abstractmethod
    def extract(self, dataset: RequestInfo_Analysis) -> None:
        """
        Injects class into dataset
        """
        ...
