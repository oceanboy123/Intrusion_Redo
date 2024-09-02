import pandas as pd
from pandas import DataFrame
from dataclasses import dataclass, field
from typing import Dict, Any, List
from .ETL_method import ETL_method
from logging import Logger

@dataclass
class data_extraction(ETL_method):
    """
    Extracts data from target variables for each profile in DATA_INFO, 
    separates the profiles by day, and identifies the unique depths present in 
    all the profiles
    
    Inputs
    - data_info : Acquired using the RequestInfo_ETL(RequestInfo) class

    Important class attributes
    - target_data : Target Variables
    - unique_depths : As named
    - nested_groups : Daily Profiles
    """
    target_data : Dict[str, Any] = field(default_factory=dict)
    nested_groups : Dict[str, Any] = field(default_factory=dict)
    unique_depths : List[int] = field(default_factory=list)

    # Field names for tables
    original_datename = 'time_string'
    groupby_datename = 'Timestamp'


    def __post_init__(self) -> None:
        self.run()


    def get_target_data(self)-> None:
        """
        Records data from target variables as a DataFrame
        """
        
        target_data = self.data_info.raw_data.loc[:, 
                                                self.data_info.target_variables]

        dates_type_datetime = pd.to_datetime(target_data.iloc[:, 0], 
                                            format = self.data_info.date_format)
        
        dates_type_int = [days.timestamp() for days in dates_type_datetime]
        
        target_data[self.original_datename] = dates_type_datetime
        target_data[self.groupby_datename] = dates_type_int
        
        # DataFrame of target variables
        self.target_data: DataFrame = target_data

    
    def get_unique_depths(self) -> None:
        """
        Records unique depths from all profiles
        """

        unique_depths = list(set(list(self.target_data.iloc[:, 1])))
        unique_depths.sort()

        # Unique depths for all profiles
        self.unique_depths: list = unique_depths


    def group_data(self) -> None:
        """
        Group data by timestamps, in other words separate data by profile/day
        """

        grouped_by_date = self.target_data.groupby(self.groupby_datename)

        nested_groups = {}
        for group_name, group_data in grouped_by_date:
            nested_groups[group_name] = group_data

        # Separated profiles
        self.nested_groups: dict = nested_groups
        self.data_info.metadata['Profile_count'] = [len(self.nested_groups)]


    def run(self) -> None:
        """
        Steps: get_target_data -> get_unique_depths -> group_data
        """
        self.get_target_data()
        self.get_unique_depths()
        self.group_data()


    def GenerateLog(self, logger: Logger) -> None:
        """
        Log self.data_info.metadata
        """
        logger.info(f'{self.data_info.metadata}')