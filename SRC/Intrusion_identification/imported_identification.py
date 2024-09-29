from .config import *

@function_log
@dataclass
class imported_identification(Step, IntrusionID_Type):
    """
    Allows you to import a previous manual identification of intrusions to 
    perform analysis (.pkl file format)

    ----------------Inputs
          intrusion_type:   Normal (Deep), 
                            Mid (Mid-depth), 
                            Other (Deep; e.g., Winter)
            manual_input:   .pkl file path for Imported

   ----------------Important class attributes
    - manualID_dates : Dates identified
    - table_IDeffects : Table for intrusion effects ('intrusionID+effect.csv')
    - intrusions : Table for characteristics of the Analysis request 
                   ('metadata_intrusions.csv')
    - effects : Class(id_method(ABC))
    """

    def __post_init__(self) -> None:
        self.run()
        joblib.dump(self, self.cache_output)

    def fill_request_info(self, dataset: RequestInfo_Analysis) -> None:
        """
        Extract required fields from Analysis request
        """
        self.uyears  = np.unique([dt.year for dt in dataset.dates])
        self.manualID_dates = import_joblib(dataset.manual_input)
        self.manual_input_type = 'IMPORTED'

    def run(self, dataset: RequestInfo_Analysis):
        """
        Steps: fill_request_info
        """
        self.fill_request_info(dataset)

    def GenerateLog(self, logger: Logger) -> None:
        """
        Logs the metadata information.
        """
        ...