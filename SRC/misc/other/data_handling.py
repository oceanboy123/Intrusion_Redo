import pandas as pd
from pandas import DataFrame

def check_duplicate_rows(data_frame: DataFrame, 
                         column_index: int = 1) -> DataFrame:
        """
        Removes duplicate rows from the DataFrame based on the values in the 
        specified column.
        """
        # Get the column name from the column index
        column_name = data_frame.columns[column_index]

        # Drop duplicates based on the specified column
        return data_frame.drop_duplicates(subset=[column_name], keep='first')