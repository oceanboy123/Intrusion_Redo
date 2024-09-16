from .config import *
from misc.other.data_handling import check_duplicate_rows

@dataclass
class data_normalization(ETL_method):
    """
    Normalizes the data by ensuring all profiles have the same depths and checks
    for duplicated measurements.

    Inputs
     data_info          : Acquired using the RequestInfo_ETL(RequestInfo) class
     data_extraction    : Acquired using the data_extraction(ETL_method) class

    Important class attributes
     normalized_data    : As named
     normalized_depth   : Unique depths from all profiles
     normalized_dates   : Profile dates
    """
    data_extraction     : ETL_method
    normalized_data     : Dict[str, DataFrame]  = field(init=False)
    normalized_depths   : List[float]           = field(init=False)
    normalized_dates    : List[int]             = field(init=False)


    def __post_init__(self) -> None:
        self.original_pressure_name = self.data_info.target_variables[1]
        self.run()


    def normalize_depth_from_list(self, upress: List[float], 
                                  data_frame: DataFrame) -> DataFrame:
        """
        Ensures that the DataFrame has rows for all unique depths.
        Missing depths are added with NaN values for the measurement columns.

        Parameters:
        - unique_depths: List of unique depths to ensure in data_frame.
        - data_frame: DataFrame of a single profile.

        Returns:
        - DataFrame: The normalized DataFrame with all unique depths.
        """
        
        notu_press = data_frame.iloc[:, 1].values
        missing_depths = [p for p in upress if p not in notu_press]
        
        # Create a list of new rows with NaN values where depths are missing
        if missing_depths:
            missing_rows = pd.DataFrame({
                data_frame.columns[0]: data_frame.iloc[0, 0],  # Constant date
                self.original_pressure_name: missing_depths,  # Depth
                **{col: np.nan for col in data_frame.columns[2:-1]}, # Missing data
                data_frame.columns[-1]: data_frame.iloc[0, -1]  # Constant date
            })

            data_frame = pd.concat([data_frame, missing_rows], 
                                   ignore_index=True)

        data_frame.sort_values(by=self.original_pressure_name, inplace=True)
        data_frame.reset_index(drop=True, inplace=True)
        
        return data_frame


    def run(self) -> None:
        """
        Steps: 
        normalize_depth_from_list -> 
        check_duplicate_rows
        """
        data = self.data_extraction.nested_groups
        udepths = self.data_extraction.unique_depths
        for key, values in data.items():
            data_frame = values
            data_frame = self.normalize_depth_from_list(udepths, data_frame)
            data_frame = check_duplicate_rows(data_frame)

            data[key] = data_frame

        # Normalized data
        self.normalized_data: dict = data
        
        pname = self.original_pressure_name
        normalized_depths = data[list(data.keys())[0]][pname].tolist()
        normalized_dates = list(data.keys())

        self.normalized_dates: list = normalized_dates
        self.normalized_depth: list = normalized_depths


    def GenerateLog(self, logger: Logger) -> None:
        """
        Logs a sample of the normalized data.
        """
        data = self.normalized_data
        logger.info(f'\nNormalized Data: \n{data[list(data.keys())[0]].head()}')