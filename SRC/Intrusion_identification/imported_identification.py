from .config import *

@function_log
@dataclass
class imported_identification(Step):
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
    manual_input : str

    manualID_dates : List[int] = field(default_factory=list)
    table_IDeffects : Dict[str, Any] = field(default_factory=dict)
    intrusions : Dict[str, Any] = field(default_factory=dict)
    effects : object = field(default_factory=empty)

    save : str = 'OFF'

    def fill_request_info(self, dates: list[datetime]) -> None:
        """
        Extract required fields from Analysis request
        """
        self.uyears  = np.unique([dt.year for dt in dates])
        self.manualID_dates = import_joblib(self.manual_input)
        self.manual_input_type = 'IMPORTED'
    

    def extract(self, dataset: RequestInfo_Analysis) -> None:
        """
        Injects class into dataset
        """
        dataset.identification = self
    

    def run(self, dataset: RequestInfo_Analysis):
        """
        Steps: fill_request_info -> extract
        """
        self.fill_request_info(dataset.dates)
        self.extract(dataset)

    def GenerateLog(self, logger: Logger) -> None:
        """
        Logs the metadata information.
        """
        ...