import numpy as np
from dataclasses import dataclass, field
from typing import List, Dict, Any
from .ETL_method import ETL_method

@dataclass
class timedepth_space(ETL_method):
    """
    TBD
    """
    data_normalization : ETL_method
    variables_matrices : List[int] = field(default_factory=list)

    def separate_target_variables(self, string_name: str):
        """Creates an NParray for one of the target variables where
        the y-axis represents depth and the x-axis represents
        date"""

        # logger.info('Creating Target Variable Matrices')
        all_columns = np.transpose([values[string_name] for key, values in self.data_normalization.normalized_data.items()])

        return all_columns
    

    def get_variable_matrices(self) -> None:
        """Creates NParrays for all target variables"""

        # NParray for all target variables
        self.variables_matrices = [self.separate_target_variables(names) for names in self.data_info.target_variables[2:]]

        # logger.debug(f'\nVariable Matrix: \n{pd.DataFrame(self.variables_matrices[0]).head()}')

    def GenerateMetadata(self) -> None:
        return "Metadata Generated"