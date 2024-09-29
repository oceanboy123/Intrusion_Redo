from .config import *
from misc.other.data_handling import check_duplicate_rows

@dataclass
class Normalize_Type(ABC):
    """
    Inputs:
    - data_extraction:  An instance of the DataExtraction class, which should 
                        have the following attributes:
                        - nested_groups: Profiles grouped by date.
                        - unique_depths: Unique depths across all profiles.

    Important class attributes
    - normalized_data:  Dictionary containing normalized 
                        profiles.
    - normalized_depths:List of unique depths after normalization.
    - normalized_dates: List of profile dates (timestamps)
    """
    data_extraction   : Extract_Type
    normalized_data   : Dict[str, DataFrame] = field(init=False)
    normalized_depths : List[float] = field(init=False)
    normalized_dates  : List[int] = field(init=False)
    required_data     : List[str] = []
    cache_output      : str = '../data/CACHE/Processes/ETL/temp_normalized.pkl'


@dataclass
class data_normalization(Normalize_Type, Step, metaclass=DocInheritMeta):
    """
    Normalizes the data by ensuring all profiles have the same depths and checks
    for duplicated measurements.

    Use help() function for more information
    """
    data_extraction   : ETL_method = field(init=False)
    normalized_data   : Dict[str, DataFrame] = field(init=False)
    normalized_depths : List[float] = field(init=False)
    normalized_dates  : List[int] = field(init=False)
    required_data     : List[str] = [
        '../data/CACHE/Processes/ETL/temp_extraction.pkl'
    ]
    cache_output      : str = '../data/CACHE/Processes/ETL/temp_normalized.pkl'


    def __post_init__(self, data_info: RequestInfo_ETL) -> None:
        self.data_extraction = import_joblib(self.required_data[0])
        self.original_pressure_name = data_info.target_variables[1]
        self.run()
        joblib.dump(self, self.cache_output)


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