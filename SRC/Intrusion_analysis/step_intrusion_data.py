from .config import (
    # Imports
    ABC,
    Logger,
    dataclass,
    field,
    joblib,
    
    # Typing
    List,
    
    # Wrapper
    function_log,
    
    # ABC Class
    Step,
    RequestInfo_Analysis,
    
    # Custom Functions
    import_joblib,
    date_comparison,

    # Other
    bottom_avg_names,
    mid_avg_names
)

@dataclass
class IntrusionData_Type(ABC):
    """
    ----------------Important fields
    - manualID_indices        : Intrusion events indices
    - manualID_temp_effects   : Effects on temperature
    - manualID_salt_effects   : Effects on salt
    - required_data           : Input path of temporary required object
    - cache_output            : Output path of temporary identification object
    """
    manualID_indices      : List[int] = field(default_factory=list)
    manualID_temp_effects : List[int] = field(default_factory=list)
    manualID_salt_effects : List[int] = field(default_factory=list)
    required_data         : List[str] = [
        '../data/CACHE/Processes/Analysis/temp_identification.pkl'
    ]
    cache_output : str = '../data/CACHE/Processes/Analysis/temp_effects.pkl'

@function_log
@dataclass
class intrusion_data(Step, IntrusionData_Type):
    """
    The intrusion_data class allows you to retrieve the effects from the 
    intrusion events selected from the original data (dataset.data)
    """
    
    def __post_init__(self, dataset: RequestInfo_Analysis) -> None:
        self.identification = import_joblib(self.required_data[0])
        self.run(dataset)
        joblib.dump(self, self.cache_output)

    def get_original_indices(self, dataset: RequestInfo_Analysis) -> None:
        """
        Get the indices of the intrusions identified from the main data 
        (self.dates)
        """
        comparison_results = date_comparison(
            self.identification.manualID_dates, dataset.dates)
        compared_dates = comparison_results['Matched']
        intrusion_dates = [match[2] for match in compared_dates]
        self.manualID_indices = [i for i, dt1 in 
                                 enumerate(dataset.dates) 
                                 for _, dt2 in 
                                 enumerate(intrusion_dates) 
                                 if dt1 == dt2]

    def get_intrusion_effects(self, dataset: RequestInfo_Analysis) -> None:
        """
        Use the date indices from self.dates to identify the effects of those 
        intrusions in self.data
        """
        self.manualID_temp_effects = dataset.data[
                                    bottom_avg_names[0]][self.manualID_indices]
        self.manualID_salt_effects = dataset.data[
                                    bottom_avg_names[1]][self.manualID_indices]

        if dataset.intrusion_type.upper() == 'MID':
            # Selecting data based on mid-depts
            self.manualID_temp_effects = dataset.data[
                                        mid_avg_names[0]][self.manualID_indices]
            self.manualID_salt_effects = dataset.data[
                                        mid_avg_names[1]][self.manualID_indices]
    
    def run(self, dataset: RequestInfo_Analysis):
        """
        Steps: get_original_indices -> get_intrusion_effects
        """
        self.get_original_indices(dataset)
        self.get_intrusion_effects(dataset)

    def GenerateLog(self, logger: Logger) -> None:
        """
        Logs the metadata information.
        """
        logger.info('Intrusion Data Retrieved')