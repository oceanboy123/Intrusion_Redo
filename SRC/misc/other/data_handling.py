import pandas as pd
from pandas import DataFrame

def check_duplicate_rows(data_frame: DataFrame) -> DataFrame:
        """
        As named
        """
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