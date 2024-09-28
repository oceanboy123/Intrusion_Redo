from .config import *


@dataclass
class Extract_Type(ABC):
    """
    Inputs
    - data_info::
        - raw_data: The raw data from which to extract target variables.
        - target_variables: List of target variable column names to extract.
        - date_format: The format of the date strings in the date column.
        - metadata: A dictionary to store metadata information.
        - nested_groups: Profiles grouped by date.
        - unique_depths: Unique depths across all profiles.
        - target_variables: List of target variable names.
        - deep_depth: Depth value defining 'deep' depths.
        - mid_depth: Depth range defining 'mid' depths.
        - lineage: Lineage information of the data processing steps.

    Important Class attributes
    - target_data:  DataFrame containing the target variables and 
                    additional date columns.
    - unique_depths:Sorted list of unique depths present in all 
                    profiles.
    - nested_groups:Dictionary mapping timestamps to 
                    DataFrames of daily profiles
    """
    target_data      : DataFrame                 = field(init=False)
    unique_depths    : List[float]               = field(init=False)
    nested_groups    : Dict[float, DataFrame]    = field(init=False)
    groupby_datename : str                       = 'Timestamp'


@dataclass
class data_extraction(Extract_Type, Step, metaclass=DocInheritMeta):
    """
    Extracts data from target variables for each profile in data_info, 
    separates the profiles by day, and identifies the unique depths present in 
    all the profiles

    Use help() function for more information
    """

    def __post_init__(self) -> None:
        self.original_datename  = self.data_info.target_variables[0]
        self.original_depthname = self.data_info.target_variables[1]
        self.run()


    def get_target_data(self)-> None:
        """
        Extracts target variables from raw data and processes date columns.
        """
        
        target_data = self.data_info.raw_data[
                            self.data_info.target_variables].copy()

        dates_type_datetime = pd.to_datetime(
            target_data[self.original_datename],
            format=self.data_info.date_format,
            errors='coerce'
        )
        
        # Check for parsing errors
        if dates_type_datetime.isnull().any():
            raise ValueError(
                "Some date strings could not be parsed.")

        dates_type_int = dates_type_datetime.astype('int64') // 10**9
        
        target_data[self.original_datename] = dates_type_datetime
        target_data[self.groupby_datename] = dates_type_int
        
        # DataFrame of target variables
        self.target_data: DataFrame = target_data

    
    def get_unique_depths(self) -> None:
        """
        Identifies unique depths from all profiles.
        """

        unique_depths = self.target_data[self.original_depthname].unique()
        unique_depths.sort()

        # Unique depths for all profiles
        self.unique_depths = unique_depths.tolist()


    def group_data(self) -> None:
        """
        Groups data by timestamps, effectively separating data by profile/day.

        Steps:
        - Groups target_data by the 'Timestamp' column.
        - Stores the grouped data in a dictionary.
        - Updates metadata with the profile count.
        """

        grouped_by_date = self.target_data.groupby(self.groupby_datename)

        # Separated profiles
        self.nested_groups = {group_name: group_data 
                              for group_name, group_data 
                              in grouped_by_date}
        self.data_info.metadata['profile_count'] = len(self.nested_groups)


    def run(self) -> None:
        """
        Steps: 
        get_target_data   -> 
        get_unique_depths -> 
        group_data
        """
        self.get_target_data()
        self.get_unique_depths()
        self.group_data()


    def GenerateLog(self, logger: Logger) -> None:
        """
        Logs the metadata information.
        """
        logger.info(f'{self.data_info.metadata}')