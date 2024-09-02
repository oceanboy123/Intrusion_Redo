from numpy import ndarray

from .config import *


@dataclass
class timedepth_space(ETL_method):
    """
    Creates matrix for all target variables where the y-axis represents depth 
    and the x-axis represents date

    Inputs
    - data_info : Acquired using the RequestInfo_ETL(RequestInfo) class
    - data_normalization : Acquired using the data_normalization(ETL_method) class

    Important class attributes
    - variables_matrices : Matrices for each target variable [date, depth]
    """
    data_normalization : ETL_method
    variables_matrices : List[int] = field(default_factory=list)


    def __post_init__(self) -> None:
        self.run()


    def separate_target_variables(self, string_name: str) -> ndarray:
        """Creates matrix for one of the target variables"""
        profile = self.data_normalization.normalized_data.items()

        return np.transpose([values[string_name] for key, values in profile])
    

    def run(self) -> None:
        """Creates matrix for all target variables"""
        var_n = self.data_info.target_variables[2:]
        all_matrices = [self.separate_target_variables(names) for names in var_n]
        self.variables_matrices = all_matrices


    def GenerateLog(self, logger: Logger) -> None:
        """
        Log self.data_info.metadata
        """
        first_matrix = pd.DataFrame(self.variables_matrices[0])
        logger.info(f'\nVariable Matrix: \n{first_matrix.head()}')