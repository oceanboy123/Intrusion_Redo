import pandas as pd
from dataclasses import dataclass, field
from typing import Dict, Any, List
from .ETL_method import ETL_method

@dataclass
class data_extraction(ETL_method):
    """
    TBD
    """
    target_data : Dict[str, Any] = field(default_factory=dict)
    nested_groups : Dict[str, Any] = field(default_factory=dict)
    unique_depths : List[int] = field(default_factory=list)

    original_datename = 'time_string'
    groupby_datename = 'Timestamp'

    def get_target_data(self)-> None:
        """Read data from csv file and records target variables
        as a pandas DataFrame"""
        
        # logger.info('\nExtrating target data')
        target_data = self.data_info.raw_data.loc[:, self.data_info.target_variables]

        dates_type_datetime = pd.to_datetime(target_data.iloc[:, 0], format = self.data_info.date_format)
        dates_type_int = [days.timestamp() for days in dates_type_datetime]
        
        target_data[self.original_datename] = dates_type_datetime
        target_data[self.groupby_datename] = dates_type_int
        
        # DataFrame of target variables
        self.target_data = target_data

    
    def get_unique_depths(self) -> None:
        unique_depths = list(set(list(self.target_data.iloc[:, 1])))
        unique_depths.sort()

        # Unique depths for all profiles
        self.unique_depths = unique_depths


    def group_data(self) -> None:
        """Group data by timestamps, aka separate data by profile"""

        # logger.info('\nUpdating target data and grouping by profile')
        grouped_by_date = self.target_data.groupby(self.groupby_datename)

        nested_groups = {}
        for group_name, group_data in grouped_by_date:
            nested_groups[group_name] = group_data

        # Separated profiles
        self.nested_groups = nested_groups
        self.data_info.metadata['Profile_count'] = [len(self.nested_groups)]

        # logger.debug(f'\nGrouped Data: \n{self.nested_groups[list(self.nested_groups.keys())[0]].head()}')


    def run(self) -> None:
        self.get_target_data()
        self.get_unique_depths()
        self.group_data()


    def GenerateMetadata(self) -> None:
        return "Metadata Generated"