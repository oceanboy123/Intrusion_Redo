# TODO: Improve loggers for Identification and Analysis
from .config import (
    # Imports
    ABC,
    Logger,
    dataclass,
    field,
    pd,
    
    # Typing
    List,
    Dict,
    Any,
    
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
import numpy as np
import joblib
from datetime import datetime
from scipy.optimize import minimize

from misc.other.date_handling import timestamp2datetime_lists

@dataclass
class IntrusionCoeff_Type(ABC):
    """
    ----------------Important fields
    - OP_temp_coeff and 
      OP_salt_coeff           : Optimized coefficients for temperature and 
                                salinity. Used only for [GET_COEFFICIENTS]
    - OP_performance          : Performance of coefficients used based on the 
                                intrusion_id_performance() method
    - OP_performance_spec     : More specific performance parameters including
                                number of missed, extra, and found intrusion events
    - table_coefficients      : Table used to record coefficient data in 
                                'coefficients.csv'
    - table_coefficients_error: Intermediate table used to record specific 
                                coefficient data in .csv
    - estimatedID_dates       : Dates identified by algorithm
    - table_IDeffects         : Table for estimated intrusion effects 
                                ('intrusionID+effect.csv')
    - OF_range                : The range of values that the optimization
                                funtion will use
    - required_data           : Input path of temporary required object
    - cache_output            : Output path of temporary identification object
    - analysis_type           : [GET_COEFFICIENTS or USE_COEFFICIENTS]
    - coefficients            : Used only for [USE_COEFFICIENTS]
    """
    OP_temp_coeff            : int = field(default_factory=int)
    OP_salt_coeff            : int = field(default_factory=int)
    OP_performance           : int = field(default_factory=int)
    OP_performance_spec      : Dict[str, Any] = field(default_factory=dict)
    table_coefficients       : Dict[str, Any] = field(default_factory=dict)
    table_coefficients_error : Dict[str, Any] = field(default_factory=dict)
    estimatedID_dates        : List[int] = field(default_factory=list)
    table_IDeffects          : Dict[str, Any] = field(default_factory=dict)
    OF_range                 : List[float] = field(
        default_factory=lambda: [-1, 1]
    )
    required_data     : List[str] = field(
        default_factory=lambda: 
        ['data/CACHE/Processes/Analysis/temp_identification.pkl']
    )
    cache_output : str = 'data/CACHE/Processes/Analysis/temp_coefficient.pkl'

@function_log
@dataclass
class intrusion_analysis(Step, IntrusionCoeff_Type):
    """
    The intrusion_analysis class encompasses all the analysis methods that
    can be performed on the intrusion data. Where the two main methods are:

    ----------------Methods
        get_coefficients(): Allows you to estimate the optimal coefficients
                            fro intrusion identification using this script
                            [GET_COEFFICIENTS]
        use_coefficients(): Allows you to evaluate the performance of 
                            specific coefficients [USE_COEFFICIENTS]

    """
    
    def __init__(self, dataset: RequestInfo_Analysis) -> None:
        super().__init__()
        self.identification = import_joblib(self.required_data[0])
        self.analysis_type = dataset.analysis_type
        self.coefficients = dataset.coefficients
        self.run(dataset)
        joblib.dump(self, self.cache_output)

    def intrusion_identification(self,  lst: list[int], 
                            dataset: RequestInfo_Analysis) -> list[datetime]:
        """
        Uses given coefficients to identify intrusion events. These
        numbers represent the changes in these variables that should be flagged
        as intrusion events based on a depth-average value (either 60-botttom
        for deep, or mid depths that will depend on raw data transformation)
        """
        
        temp_intrusion_coeff, salt_intrusion_coeff = lst

        column_avgs_temp = dataset.data[bottom_avg_names[0]]
        column_avgs_salt = dataset.data[bottom_avg_names[1]]

        if dataset.intrusion_type.upper() == 'MID':
            column_avgs_temp = dataset.data[mid_avg_names[0]]
            column_avgs_salt = dataset.data[mid_avg_names[1]]


        intrusion_temp_indices = list(np.where(
                                column_avgs_temp > temp_intrusion_coeff)[0]+1)
        intrusion_salt_indices = list(np.where(
                                column_avgs_salt > salt_intrusion_coeff)[0]+1)

        all_timestamps = pd.DataFrame(dataset.dates_stamp)

        temp_intrusion_dates = all_timestamps.iloc[intrusion_temp_indices]
        salt_intrusion_dates = all_timestamps.iloc[intrusion_salt_indices]

        estimated_intrusion_dates = [value for value in 
                                     temp_intrusion_dates.values.tolist() 
                                     if value in 
                                     salt_intrusion_dates.values.tolist()]
        
        estimated_intrusion_dates = [item for sublist in 
                                     estimated_intrusion_dates 
                                     for item in sublist]
        
        estimated_intrusion_dates = timestamp2datetime_lists(
                                        estimated_intrusion_dates)

        return estimated_intrusion_dates


    def intrusion_id_performance(self, init_coeff: list, 
                                 dataset: RequestInfo_Analysis) -> int:
        """
        Compares the manually identified intrusion and the estimated intrusions
        to evaluate the coefficient performance
        """
        estimated_intrusion_dates = self.intrusion_identification(
                                                    init_coeff, dataset)
        real_intrusion_dates = self.identification.manualID_dates
        comparison_dates = date_comparison(real_intrusion_dates, 
                                           estimated_intrusion_dates)
            
        missed_id = comparison_dates['Only Manual']
        extra_id = comparison_dates['Only Estimated']

        # Performance Parameters
        if len(estimated_intrusion_dates) != 0:
            n_missed = len(missed_id) # Intrusions missed
            n_maybenot = len(extra_id) # Maybe false positives
            n_pseudofact = len(real_intrusion_dates) # Manual Identification
            n_estimated = len(estimated_intrusion_dates) # Estimated Id

            performance_parameter = ((n_missed +
                                      n_maybenot)
                                            /
                                     (n_pseudofact + 
                                      n_estimated))
        else:
            performance_parameter = 1

        return performance_parameter


    def get_coefficients(self, dataset: RequestInfo_Analysis) -> None:
        """
        Estimates optimized coeficients by iterarting through temperature 
        coefficients between self.OF_range and salinity coefficients between 0 
        to self.OF_range[1] and finding the combination with the best results 
        based on a performance parameter
        """
        real_intrusion_dates = self.identification.manualID_dates
        range = self.OF_range
        
        temp_range = np.arange(range[0],range[1],0.025)
        salt_range = np.arange(0,range[1],0.02)
        result_final = []

        # Minimize the performance parameter
        for temp_guess in temp_range:
            for salt_guess in salt_range:
                initial_guess = [temp_guess, salt_guess]
                result = minimize(self.intrusion_id_performance, 
                                  initial_guess, args = (dataset))
                result_final.append((result.x, result.fun))

        best_coefficients = min(result_final, key= lambda x: x[1])

        self.OP_performance = best_coefficients[1] 
        self.OP_temp_coeff = list(best_coefficients[0])[0] 
        self.OP_salt_coeff = list(best_coefficients[0])[1] 
        
        self.OP_performance_spec = date_comparison(
                                    real_intrusion_dates, 
                                    self.intrusion_identification(
                                        [self.OP_temp_coeff, self.OP_salt_coeff], 
                                        dataset)
                                        )
        

    def use_coefficients(self, dataset: RequestInfo_Analysis):
        """
        Evaluates the performance of a set of coefficients on dataset
        """
        self.OP_temp_coeff = self.coefficients[0] 
        self.OP_salt_coeff = self.coefficients[1]
        self.OP_performance = self.intrusion_id_performance(self.coefficients, 
                                                            dataset)
        
        self.OP_performance_spec = date_comparison(
                    self.identification.manualID_dates, 
                    self.intrusion_identification(self.coefficients, dataset))


    def run(self, dataset: RequestInfo_Analysis) -> None:
        """
        Steps: get_coefficients or  use_coefficients -> extract
        """
        if self.analysis_type.upper() == 'GET_COEFFICIENTS':
            self.get_coefficients(dataset)
        else:
            self.use_coefficients(dataset)

    
    def GenerateLog(self, logger: Logger) -> None:
        """
        Logs the metadata information.
        """
        logger.info('Main Intrusion Analysis Completed')