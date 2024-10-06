# TODO [Medium]: Implement intrusion type integrated in the intrusion identification save file
from .config import (
    # Imports
    dataclass,
    joblib,
    np,
    Logger,

    # ABC classes
    RequestInfo_Analysis,
    IntrusionID_Type,
    Step,

    # Custum Function
    import_joblib,

    # Wrapper
    function_log
)

@function_log
@dataclass
class imported_identification(Step, IntrusionID_Type):
    """
    Allows you to import a previous manual identification of intrusions to 
    perform analysis (.pkl file format)

    """

    def __post_init__(self) -> None:
        self.run()
        joblib.dump(self, self.cache_output)

    def fill_request_info(self, dataset: RequestInfo_Analysis) -> None:
        """
        Extract required fields from RequestInfo_Analysis object
        """
        self.uyears  = np.unique([dt.year for dt in dataset.dates])
        self.manual_input_type = 'IMPORTED'
        self.manual_input = dataset.manual_input
        self.manualID_dates = import_joblib(self.manual_input)
        self.intrusion_type = None


    def run(self, dataset: RequestInfo_Analysis):
        """
        Steps: fill_request_info
        """
        self.fill_request_info(dataset)

    def GenerateLog(self, logger: Logger) -> None:
        """
        Logs the metadata information.
        """
        logger.info('Intrusion Identification Imported')