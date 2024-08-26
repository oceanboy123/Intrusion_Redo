import numpy as np
import pandas as pd
from dataclasses import dataclass, field
from typing import Dict, Any, List
from .ETL_method import ETL_method

@dataclass
class data_transformation(ETL_method):
    """
    TBD
    """
    data_normalization : ETL_method
    timedepth_space : ETL_method
    interpolated0_data : List[int] = field(default_factory=list)
    interpolated_data : List[int] = field(default_factory=list)
    interpolated_diff_data : List[int] = field(default_factory=list)
    avg_below : List[int] = field(default_factory=list)
    avg_btw : List[int] = field(default_factory=list) 
    transform_data : Dict[str, Any] = field(default_factory=dict)

    # Transformation names per variable
    transformation_names = [
            '_interpolated_axis0',
            '_interpolated_axis10',
            '_diff_axis1_inter10',
            '_avg_diff1_inter10',
            '_avgmid_diff1_inter10'
        ]

    def interpolation_2D(self, pandas_matrix) -> None:
        # Then check for interpolation where the is no change, this highlight
        # interpolation with NaNs such as near the ends of profiles.

        # Column interpolation
        interpolated0_data = pandas_matrix.interpolate(axis=0).replace(0, np.nan)

        # diff
        inter0_diff = pd.DataFrame(np.diff(interpolated0_data, axis=0))
        zero_diff = inter0_diff == 0
        mask_diff = np.zeros_like(interpolated0_data, dtype=bool)
        mask_diff[1:] = zero_diff

        interpolated0_data[mask_diff] = np.nan

        # Row interpolation
        self.interpolated_data = interpolated0_data
        self.interpolated_diff_data = pd.DataFrame(np.diff(self.interpolated_data, axis=1)).replace(0, np.nan)


    def depth_averages(self) -> None:
        normal_depths = np.array(self.data_normalization.normalized_depth)
        rows_bellow60 = list(np.where(normal_depths > self.data_info.deep_depth)[0])

        rows_over35 = list(np.where(normal_depths < self.data_info.mid_depth[1])[0])
        rows_under20 = list(np.where(normal_depths > self.data_info.mid_depth[0])[0])
        rows_btw20_35 = sorted(list(set(rows_over35+rows_under20)))

        matrix = self.interpolated_data
        matrix_diff = self.interpolated_diff_data

        self.avg_below_value = matrix.iloc[rows_bellow60, :].mean(axis=0) # Deep depth average
        self.avg_btw_value = matrix.iloc[rows_btw20_35, :].mean(axis=0) # Mid depth average
        self.avg_below = matrix_diff.iloc[rows_bellow60, :].mean(axis=0)
        self.avg_btw = matrix_diff.iloc[rows_btw20_35, :].mean(axis=0)


    def data_transformations(self) -> None:
        """Performs trasnformations of values. Incluiding 2D
        interpolation of profile data to fill NaNs, mainly the 
        ones added for depth normalization
        
        In addition, it also calculates depth averages based
        on the deep_depth and mid_depth variables, and calculates
        the n-th order discrete difference along the date axis
        for those depth averages"""

        # logger.info('Interpolating Data')

        transform_data = {}
        count = 2
        for matrix in self.timedepth_space.variables_matrices:
            pandas_matrix = pd.DataFrame(matrix)

            # 2D interpolation
            self.interpolation_2D(pandas_matrix)

            # Calculate depth averages
            self.depth_averages()

            t_names = self.transformation_names
            v_names = self.data_info.target_variables

            transform_data[v_names[count]+t_names[0]] = self.interpolated0_data
            transform_data[v_names[count]+t_names[1]] = self.interpolated_data
            transform_data[v_names[count]+t_names[2]] = self.interpolated_diff_data
            transform_data[v_names[count]+t_names[3]] = self.avg_below
            transform_data[v_names[count]+t_names[4]] = self.avg_btw
            
            count += 1

        # Transformed data ready for loading
        self.transform_data: dict = transform_data
        
        # logger.debug(f'\nInterpolated Data: \n{self.transform_data[list(self.transform_data.keys())[1]].head()}')
        # logger.debug(f'\nDeep Data: \n{self.transform_data[list(self.transform_data.keys())[3]].head(20)}')
        # logger.debug(f'\nMid Data: \n{self.transform_data[list(self.transform_data.keys())[4]].head(20)}')

    def GenerateMetadata(self) -> None:
        return "Metadata Generated"