from numpy import ndarray

from .config import *

@dataclass
class timedepth_space(ETL_method):
    """
    Creates matrix for all target variables where the y-axis represents depth 
    and the x-axis represents date

    Inputs
    - data_info             : An object containing data information. 
                              Acquired using the RequestInfo_ETL(RequestInfo) class
    - data_normalization    : An object containing data after normalization.
                              Acquired using a normalization ETL_method class

    Important class attributes
    - variables_matrices    : Dictionary of matrices for each target variable
    """
    data_normalization : ETL_method
    variables_matrices: Dict[str, ndarray] = field(init=False)


    def __post_init__(self) -> None:
        self.run()


    def create_variable_matrix(self, variable_name: str) -> ndarray:
        """
        Creates a 2D numpy array for a given target variable.
        """
        depths = self.data_normalization.normalized_depth
        dates = self.data_normalization.normalized_dates
        data_frames = self.data_normalization.normalized_data

        matrix_df = pd.DataFrame(index=depths, columns=dates, dtype=float)

        for date in dates:
            df = data_frames[date]
            df = df.set_index(self.data_info.target_variables[1])

            if variable_name not in df.columns:
                raise ValueError(f"Variable '{variable_name}"+
                                 " not found in DataFrame columns.")
            variable_series = df[variable_name]

            variable_series = variable_series.reindex(depths)
            matrix_df[date] = variable_series

        variable_matrix = matrix_df.values

        return variable_matrix

    def run(self) -> None:
        """
        Creates matrices for all target variables.
        """
        target_variables = [
            var for var in self.data_info.target_variables
            if var not in [self.data_info.target_variables[0], 
                           self.data_info.target_variables[1]]
        ]

        variables_matrices = {}
        for variable in target_variables:
            variable_matrix = self.create_variable_matrix(variable)
            variables_matrices[variable] = variable_matrix

        self.variables_matrices = variables_matrices

    def GenerateLog(self, logger: Logger) -> None:
        """
        Logs a sample of the variable matrix.

        """
        # Log the first variable matrix
        if self.variables_matrices:
            first_variable = next(iter(self.variables_matrices))
            first_matrix = self.variables_matrices[first_variable]

            sample_df = pd.DataFrame(
                first_matrix,
                index=self.data_normalization.normalized_depth,
                columns=self.data_normalization.normalized_dates
            ).head()

            logger.info(f'\nVariable Matrix for {first_variable}:\n{sample_df}')
        else:
            logger.info("No variable matrices were created.")