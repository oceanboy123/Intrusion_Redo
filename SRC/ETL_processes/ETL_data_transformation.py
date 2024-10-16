from .config import *

@dataclass
class Transform_Type(ABC):
    """
    Inputs
    - data_normalization : Acquired using the data_normalization(ETL_method) class
    - timedepth_space : Acquired using the timedepth_space(ETL_method) class

    Important class attributes
    - transform_data: Dictionary to store transformed data.
    """
    data_normalization  : ETL_method
    timedepth_space     : ETL_method
    transform_data      : Dict[str, Any]    = field(default_factory=dict)


@dataclass
class data_transformation(Transform_Type, ETL_method, metaclass=DocInheritMeta):
    """
    Performs data interpolation and calculates depth-averages for mid and bottom
    depths

    Use help() function for more information
    """

    # Transformation names per variable
    transformation_names = [
            '_interpolated_axis0',
            '_interpolated_axis10',
            '_diff_axis1_inter10',
            '_avg_diff1_inter10',
            '_avgmid_diff1_inter10'
        ]


    def __post_init__(self) -> None:
        self.run()


    def interpolation_2D(self, pandas_matrix: DataFrame) -> list[DataFrame]:
        """
        Performs interpolation on the target variable matrices, including 2D 
        interpolation of profile data to fill NaNs, mainly those added during 
        depth normalization.
        
        Only column interpolation is performed (axis=0).
        """
        # Column interpolation
        interpolated0_data = pandas_matrix.interpolate(axis=0, 
                                                       limit_direction='both'
                                                       ).replace(0, np.nan)

        # Calculate changes in variables from profile to profile
        inter0_diff = interpolated0_data.diff(axis=0)
        zero_diff = inter0_diff == 0

        mask_diff = zero_diff.shift(-1, fill_value=False)

        interpolated0_data = interpolated0_data.mask(mask_diff)
        interpolated0_data.loc[interpolated0_data.index[-1], :] = np.nan

        # Row interpolation (Not performed)
        interpolated_data = interpolated0_data

        # Difference along axis=1 (rows)
        inter_diff = interpolated_data.diff(axis=1)
        interpolated_diff_data = inter_diff.replace(0, np.nan)

        return [interpolated_data, interpolated_diff_data, interpolated0_data]


    def depth_averages(self, interpolation: list[DataFrame]) -> list[pd.Series]:
        """
        Identifies the depths and calculates the depth-averages for both mid
        and bottom time-series for the actual values and the n-th order 
        discrete difference along the date axis

        Returns:
        - List[pd.Series]: List containing:
            - avg_below_diff: Mean of differences at deep depths.
            - avg_mid_diff: Mean of differences at mid depths.
            - avg_below_value: Mean of values at deep depths.
            - avg_mid_value: Mean of values at mid depths.
        """
        deep_depth = self.data_info.deep_depth
        normalized_depths = np.array(self.data_normalization.normalized_depth)

        indices_below_deep = np.where(normalized_depths > deep_depth)[0]

        mid_depth_range = self.data_info.mid_depth
        indices_mid_depth = np.where(
            (normalized_depths > mid_depth_range[0]) & 
            (normalized_depths < mid_depth_range[1])
        )[0]

        interpolated_data = interpolation[0]
        interpolated_diff_data = interpolation[1]

        # Bottom depths averages
        avg_below_value = interpolated_data.iloc[indices_below_deep, 
                                                 :].mean(axis=0)
        avg_below_diff = interpolated_diff_data.iloc[indices_below_deep, 
                                                     :].mean(axis=0)

        # Mid depths averages
        avg_mid_value = interpolated_data.iloc[indices_mid_depth, 
                                               :].mean(axis=0)
        avg_mid_diff = interpolated_diff_data.iloc[indices_mid_depth, 
                                                   :].mean(axis=0)

        return [avg_below_diff, avg_mid_diff, avg_below_value, avg_mid_value]


    def run(self) -> None:
        """
        Executes the data transformation process:
        -   For each variable matrix, performs 2D interpolation and calculates 
            depth averages.
        """
        for var_name, matrix in self.timedepth_space.variables_matrices.items():
            pandas_matrix = pd.DataFrame(matrix)

            # 2D interpolation
            interpolated = self.interpolation_2D(pandas_matrix)

            # Calculate depth averages
            depth_avg = self.depth_averages(interpolated)

            t_names = self.transformation_names

            self.transform_data[var_name + t_names[0]] = interpolated[2]
            self.transform_data[var_name + t_names[1]] = interpolated[0]
            self.transform_data[var_name + t_names[2]] = interpolated[1]
            self.transform_data[var_name + t_names[3]] = depth_avg[0] 
            self.transform_data[var_name + t_names[4]] = depth_avg[1]
        

    def GenerateLog(self, logger: Logger) -> None:
        """
        Logs a sample of the transformed data.
        """
        if self.transform_data:
            sample_key = list(self.transform_data.keys())[3]
            transform_sample = self.transform_data[sample_key].head(20)
            logger.info(
                f'\nDeep Data Sample ({sample_key}):\n{transform_sample}'
                )
        else:
            logger.info(
                'No transformed data to display.'
                )