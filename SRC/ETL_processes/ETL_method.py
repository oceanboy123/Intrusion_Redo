from abc import ABC, abstractmethod
from dataclasses import dataclass
from misc.request_arguments.request_info_ETL import RequestInfo_ETL

@dataclass
class ETL_method(ABC):
    """
    Inputs
    - data_info::
        - raw_data: The raw data from which to extract target variables.
        - target_variables: List of target variable column names to extract.
        - date_format: The format of the date strings in the date column.
        - metadata: A dictionary to store metadata information.
        - nested_groups: Profiles grouped by date.
        - unique_depths: Unique depths across all profiles.
        - target_variables: List of target variable names.
        - deep_depth: Depth value defining 'deep' depths.
        - mid_depth: Depth range defining 'mid' depths.
        - lineage: Lineage information of the data processing steps.
    """
    data_info : RequestInfo_ETL
    
    @abstractmethod
    def GenerateLog(self) -> None:
        """
        As named
        """
        ...
