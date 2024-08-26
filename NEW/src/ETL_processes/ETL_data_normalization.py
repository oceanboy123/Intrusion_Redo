import pandas as pd
import numpy as np
from dataclasses import dataclass, field
from typing import Dict, Any, List
from .ETL_method import ETL_method

@dataclass
class data_normalization(ETL_method):
    """
    TBD
    """
    data_extraction : ETL_method
    normalized_data : Dict[str, Any] = field(default_factory=dict)
    normalized_depth : List[int] = field(default_factory=list)
    normalized_dates : List[int] = field(default_factory=list)

    original_pressure_name = 'pressure'

    def normalize_depth_from_list(self, upress: list, data_frame):
        """Use list of unique depths from group_data() to normalize
        the depths available in a profile by creating new rows
        where expected depths are missing and filling with NaN
        values"""

        # Ensure that the columns to be filled with NaN are correctly identified
        num_columns = len(data_frame.columns)
        
        # Create a DataFrame containing all unique pressures (upress) with NaN values for missing depths
        missing_depths = [p for p in upress if p not in data_frame.iloc[:, 1].values]
        
        # Create a list of new rows with NaN values where depths are missing
        if missing_depths:
            missing_rows = pd.DataFrame({
                data_frame.columns[0]: data_frame.iloc[0, 0],  # Assuming the first column is a constant value like date or ID
                self.original_pressure_name: missing_depths,
                **{col: np.nan for col in data_frame.columns[2:-1]},  # Fill middle columns with NaN
                data_frame.columns[-1]: data_frame.iloc[0, -1]  # Assuming the last column is a constant value like a timestamp or group ID
            })

            # Concatenate the original DataFrame with the missing rows
            data_frame = pd.concat([data_frame, missing_rows], ignore_index=True)

        # Sort the DataFrame by the pressure column
        data_frame = data_frame.sort_values(by=self.original_pressure_name).reset_index(drop=True)
        
        return data_frame

    @staticmethod
    def check_duplicate_rows(data_frame):
        column_names = data_frame.columns.tolist()

        # Check for duplicated data
        column_index = 1
        seen = set()
        unique_data = []

        for row in data_frame.values.tolist():
            value = row[column_index]
            if value not in seen:
                seen.add(value)
                unique_data.append(row)

        data_frame = pd.DataFrame(unique_data, columns=column_names)

        return data_frame


    def normalize_length_data(self) -> None:
        # logger.info('\nNomalizing depths and filling with NaN')
        data = self.data_extraction.nested_groups
        for key, values in data.items():
            data_frame = values
            data_frame = self.normalize_depth_from_list(self.data_extraction.unique_depths, data_frame)
            data_frame = self.check_duplicate_rows(data_frame)

            data[key] = data_frame

        # Normalized data
        self.normalized_data: dict = data

        normalized_depths = data[list(data.keys())[0]][self.original_pressure_name].tolist()
        normalized_dates = list(data.keys())

        # Final list of unique depths
        self.normalized_dates: list = normalized_dates
        # Final list of unique dates
        self.normalized_depth: list = normalized_depths
        # logger.debug(f'\nNormalized Data: \n{data[list(data.keys())[0]].head()}')


    def GenerateMetadata(self) -> None:
        return "Metadata Generated"