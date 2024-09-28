from .config import *

# Transformation names based on ETL_data_loading
bottom_avg_names = ['sample_diff_row_temp', 'sample_diff_row_salt'] 
mid_avg_names = ['sample_diff_midrow_temp', 'sample_diff_midrow_salt'] 

@function_log
@dataclass
class intrusion_data(Step):
    """
    The intrusion_data class allows you to retrieve the effects from the 
    intrusion events selected from the original data (datatset.data). This 
    includes:

    ----------------Important fields
            manualID_indices: Intrusion events indices
       manualID_temp_effects: Effects on temperature
       manualID_salt_effects: Effects on salt

    NOTE: The results are saved in dataset.identification.effects
    """

    manualID_indices : List[int] = field(default_factory=list)
    manualID_temp_effects : List[int] = field(default_factory=list)
    manualID_salt_effects : List[int] = field(default_factory=list)
    
    def get_original_indices(self, dataset: RequestInfo_Analysis) -> None:
        """
        Get the indices of the intrusions identified from the main data 
        (self.dates)
        """
        comparison_results = date_comparison(
            dataset.identification.manualID_dates, dataset.dates)
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

        if dataset.identification.intrusion_type.upper() == 'MID':
            # Selecting data based on mid-depts
            self.manualID_temp_effects = dataset.data[
                                        mid_avg_names[0]][self.manualID_indices]
            self.manualID_salt_effects = dataset.data[
                                        mid_avg_names[1]][self.manualID_indices]

    def extract(self, dataset: RequestInfo_Analysis) -> None:
        """
        Injects class into dataset
        """
        dataset.identification.effects = self
    
    def run(self, dataset: RequestInfo_Analysis):
        """
        Steps: get_original_indices -> get_intrusion_effects -> extract
        """
        self.get_original_indices(dataset)
        self.get_intrusion_effects(dataset)
        self.extract(dataset)

    def GenerateLog(self, logger: Logger) -> None:
        """
        Logs the metadata information.
        """
        ...