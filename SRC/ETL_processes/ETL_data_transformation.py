import numpy as np
import pandas as pd
from pandas import DataFrame
from dataclasses import dataclass, field
from typing import Dict, Any
from .ETL_method import ETL_method
from logging import Logger

@dataclass
class data_transformation(ETL_method):
    """
    Performs data interpolation and calculates depth-averages for mid and bottom
    depths

    Inputs
    - data_info : Acquired using the RequestInfo_ETL(RequestInfo) class
    - data_normalization : Acquired using the data_normalization(ETL_method) class
    - timedepth_space : Acquired using the timedepth_space(ETL_method) class

    Important class attributes
    - transform_data : Data after transformations
    """
    data_normalization : ETL_method
    timedepth_space : ETL_method
    transform_data : Dict[str, Any] = field(default_factory=dict)

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
        Performs interpolation on the target variable matrices.Incluiding 2D
        interpolation of profile data to fill NaNs, mainly the ones added for 
        depth normalization. 
        
        With no row interpolation, only column interpolation.
        """
        # Column interpolation
        interpolated0_data = pandas_matrix.interpolate(axis=0).replace(0, np.nan)

        # Calculate changes in variables from profile to profile
        inter0_diff = pd.DataFrame(np.diff(interpolated0_data, axis=0))
        zero_diff = inter0_diff == 0
        mask_diff = np.zeros_like(interpolated0_data, dtype=bool)
        mask_diff[1:] = zero_diff

        interpolated0_data[mask_diff] = np.nan

        # Row interpolation
        interpolated_data = interpolated0_data
        inter_diff = np.diff(interpolated_data, axis=1)
        interpolated_diff_data = pd.DataFrame(inter_diff).replace(0, np.nan)

        return [interpolated_data, interpolated_diff_data, interpolated0_data]


    def depth_averages(self, interpolation: list[DataFrame]) -> list[DataFrame]:
        """
        Identifies the depths and calculates the depth-averages for both mid
        and bottom time-series. For the actual values, and the n-th order 
        discrete difference along the date axis
        """
        deep = self.data_info.deep_depth
        normal_depths = np.array(self.data_normalization.normalized_depth)
        rows_bellow60 = list(np.where(normal_depths > deep)[0])

        mid = self.data_info.mid_depth
        rows_over35 = list(np.where(normal_depths < mid[1])[0])
        rows_under20 = list(np.where(normal_depths > mid[0])[0])
        rows_btw20_35 = sorted(list(set(rows_over35+rows_under20)))

        matrix = interpolation[0]
        matrix_diff = interpolation[1]
        
        # Bottom depths
        avg_below_value = matrix.iloc[rows_bellow60, :].mean(axis=0)
        avg_below = matrix_diff.iloc[rows_bellow60, :].mean(axis=0)

        # Mid depths
        avg_btw_value = matrix.iloc[rows_btw20_35, :].mean(axis=0)
        avg_btw = matrix_diff.iloc[rows_btw20_35, :].mean(axis=0)

        return [avg_below, avg_btw, avg_below_value, avg_btw_value]


    def run(self) -> None:
        """
        Steps: interpolation_2D -> depth_averages
        """
        transform_data = {}
        count = 2
        for matrix in self.timedepth_space.variables_matrices:
            pandas_matrix = pd.DataFrame(matrix)

            # 2D interpolation
            interpolated = self.interpolation_2D(pandas_matrix)

            # Calculate depth averages
            depth_avg = self.depth_averages(interpolated)

            t_names = self.transformation_names
            v_names = self.data_info.target_variables

            transform_data[v_names[count]+t_names[0]] = interpolated[2]
            transform_data[v_names[count]+t_names[1]] = interpolated[0]
            transform_data[v_names[count]+t_names[2]] = interpolated[1]
            transform_data[v_names[count]+t_names[3]] = depth_avg[0]
            transform_data[v_names[count]+t_names[4]] = depth_avg[1]
            
            count += 1

        self.transform_data: dict = transform_data
        

    def GenerateLog(self, logger: Logger) -> None:
        """
        Log transform_eg.head(20)
        """
        transform_eg = self.transform_data[list(self.transform_data.keys())[3]]
        logger.info(f'\nDeep Data: \n{transform_eg.head(20)}')