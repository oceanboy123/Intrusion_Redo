# Imports
import time
import csv
from datetime import datetime, timedelta
import numpy as np
import matplotlib.dates as mdates
import matplotlib.pyplot as plt
import pandas as pd
from scipy.optimize import minimize
from misc import *
from dataclasses import dataclass, field
from typing import List, Dict, Any, Protocol, runtime_checkable

logger = create_logger('Intrusion_log', 'intrusion.log')

bottom_avg_names = ['sample_diff_row_temp', 'sample_diff_row_salt'] 
mid_avg_names = ['sample_diff_midrow_temp', 'sample_diff_midrow_salt'] 


# @runtime_checkable
# class method(Protocol):

#     intrusion_type : str

#     def fill_request_info(self, dataset) -> None:
#         ...

# @runtime_checkable
# class intrusion_method(Protocol):

#     manualID_indices : List[int] = field(default_factory=list)
#     manualID_temp_effects : List[int] = field(default_factory=list)
#     manualID_salt_effects : List[int] = field(default_factory=list)

# @runtime_checkable
# class main_analysis(Protocol):

#     OP_temp_coeff : int = field(default_factory=int)
#     OP_salt_coeff : int = field(default_factory=int)
#     OP_performance : int = field(default_factory=int)
#     OP_performance_spec : Dict[str, Any] = field(default_factory=dict)

def empty() -> None:
    ...

@dataclass
class dataset:
  path : str

  metadata_intrusions : Dict[str, Any] = field(default_factory=dict)
  identification : object = field(default_factory=empty)
  analysis : object = field(default_factory=empty)

  dates_name = 'sample_timestamps'
  
  def __post_init__(self) -> None:
    self.data = import_joblib(self.path)
    self.dates_stamp = self.data[self.dates_name]
    self.dates = timestamp2datetime_lists(self.dates_stamp)

@dataclass
class manual_identification:
    
    intrusion_type : str
    save : str

    manualID_dates : List[int] = field(default_factory=list)
    table_IDeffects : Dict[str, Any] = field(default_factory=dict)
    effects : object = field(default_factory=empty)

    lin = "-"*6+' ' # For printing purposes
    depth_name = 'sample_depth'
    temp_range = [0,10] 
    salt_range = [30.5,31.5] 
    oxy_range = [0,12]

    def fill_request_info(self, dataset) -> None:
        self.data = dataset.data
        self.dates = dataset.dates

        self.uyears  = np.unique([dt.year for dt in self.dates])
        self.manual_input_type = 'MANUAL'
        self.manual_input = 'N/A'

    @staticmethod
    def create_yearly_matrices(selected_data:dict, year_indices:dict[list]) -> dict:
        """Use separated indices from separate_yearly_dates() by year and create matrices
        with the data ready for plotting"""

        temp_dataframe = selected_data['sample_matrix_temp']
        salt_dataframe = selected_data['sample_matrix_salt']
        
        selected_data_temp = temp_dataframe.to_numpy()
        selected_data_salt = salt_dataframe.to_numpy()

        yearly_profiles_temp: dict = {}
        yearly_profiles_salt: dict = {}

        for year, indices in year_indices.items():
            yearly_profile_temp = selected_data_temp[:, indices]
            yearly_profiles_temp[year] = yearly_profile_temp

            yearly_profile_salt = selected_data_salt[:, indices]
            yearly_profiles_salt[year] = yearly_profile_salt

        return yearly_profiles_temp, yearly_profiles_salt

    def separate_yearly_profiles(self, dataset) -> dict[dict]:
        grouped_years = separate_yearly_dates(self.dates)

        # Create dictionary with yearly profiles indices
        by_year_indices = {year: [self.dates.index(dt) for dt in grouped_years[year]] 
                        for year in self.uyears}

        # Extract yearly profiles of temperature and salinity
        yearly_profiles_temp, yearly_profiles_salt = self.create_yearly_matrices(self.data, by_year_indices)
            
        return {'Yearly Temp Profile': yearly_profiles_temp, 
                'Yearly Salt Profile': yearly_profiles_salt, 
                'Indices by Year':by_year_indices}

    def plot_year_profiles(self, year_data: dict[dict], yr: int) -> dict:

        init_date_index = year_data['Indices by Year'][yr][0]
        last_date_index = year_data['Indices by Year'][yr][-1]
        datetime_list = self.dates[init_date_index:last_date_index]

        # Extract specific year data
        fig, axs = plt.subplots(2)
        year_temp_data = year_data['Yearly Temp Profile'][yr]
        year_salt_data = year_data['Yearly Salt Profile'][yr]

        # Temperature Plot
        xmesh,ymesh = np.meshgrid(datetime_list, self.data[self.depth_name])
        mesh0 = axs[0].pcolormesh(xmesh,ymesh,year_temp_data[:,:len(ymesh[0,:])], cmap='seismic')
        fig.colorbar(mesh0, ax=axs[0])
        axs[0].invert_yaxis()
        mesh0.set_clim(self.temp_range)
        axs[0].set_xticks([])

        # Salinity Plot
        mesh1 = axs[1].pcolormesh(xmesh,ymesh,year_salt_data[:,:len(ymesh[0,:])], cmap='seismic')
        fig.colorbar(mesh1, ax=axs[1])
        axs[1].invert_yaxis()
        mesh1.set_clim(self.salt_range)
        axs[1].xaxis.set_major_formatter(mdates.DateFormatter("%m"))

        fig.tight_layout()
        axs[0].text(0.02,0.85,str(yr), transform=axs[0].transAxes,fontsize=14,
                    verticalalignment='bottom',horizontalalignment='left',
                    bbox=dict(facecolor='white',alpha=0.5))

        return {
            'Figure':fig,
            'Axes':axs,
            'Mesh':[mesh0,mesh1]
        }

    @staticmethod
    def from_1970(date: int) -> datetime:
        """Converts points selected from plot to datetime"""

        reference_date = datetime(1970, 1, 1)

        delta = timedelta(days=date)
        datetime_obj = reference_date + delta

        return datetime_obj


    def user_intrusion_selection(self, dataset) -> None:
        logger.info(self.lin+'Intrusion identification in progress')

        yearly_profiles = self.separate_yearly_profiles(dataset)
        # Plots Temperature and Salinity profiles for user to select intrusion dates by year
        for yr in self.uyears:
            fig = self.plot_year_profiles(yearly_profiles, 
                                yr)

            fig['Figure'].canvas.mpl_connect('button_press_event', onclick)

            fig['Figure'].canvas.mpl_connect('key_press_event', onkey)

            plt.show()

        intrusion_dates = list(np.array(get_points())[:,0])
        self.manualID_dates = [self.from_1970(dt) for dt in intrusion_dates]
        
        logger.info(self.lin+'Intrusion identification completed')

    def save_identification(self, dataset) -> None:
        man_name = 'manualID_' + self.intrusion_type + str(int(time.time())) + '.pkl'
        save_joblib(self.manualID_dates, man_name)
        self.save = man_name

    def extract(self, dataset) -> None:
        dataset.identification = self
    
    def run(self, dataset):
        self.fill_request_info(dataset)
        self.user_intrusion_selection(dataset)

        if self.save != 'OFF':
            self.save_identification(dataset)
        
        self.extract(dataset)


@dataclass
class imported_identification:

    intrusion_type : str
    manual_input : str
    
    save : str = 'OFF'
    manualID_dates : List[int] = field(default_factory=list)
    table_IDeffects : Dict[str, Any] = field(default_factory=dict)
    effects : object = field(default_factory=empty)

    def fill_request_info(self, dataset) -> None:
        self.uyears  = np.unique([dt.year for dt in dataset.dates])
        self.manualID_dates = import_joblib(self.manual_input)
        self.manual_input_type = 'IMPORTED'
    
    def extract(self, dataset) -> None:
        dataset.identification = self
    
    def run(self, dataset):
        self.fill_request_info(dataset)
        self.extract(dataset)

@dataclass
class intrusion_data:

    manualID_indices : List[int] = field(default_factory=list)
    manualID_temp_effects : List[int] = field(default_factory=list)
    manualID_salt_effects : List[int] = field(default_factory=list)
    
    def get_original_indices(self, dataset) -> None:
        """Get the indices of the intrusions identified from the main data (self.dates)"""
        comparison_results = date_comparison(dataset.identification.manualID_dates, dataset.dates)
        compared_dates = comparison_results['Matched']
        intrusion_dates = [match[2] for match in compared_dates]
        self.manualID_indices = [i for i, dt1 in enumerate(dataset.dates) for j, dt2 in enumerate(intrusion_dates) if dt1 == dt2]


    def get_intrusion_effects(self, dataset) -> None:
        """Use the date indices from self.dates to identify the effects of those intrusions
        in self.data"""
        self.manualID_temp_effects = dataset.data[bottom_avg_names[0]][self.manualID_indices]
        self.manualID_salt_effects = dataset.data[bottom_avg_names[1]][self.manualID_indices]

        if dataset.identification.intrusion_type.upper() == 'MID':
            # Selecting data based on mid-depts
            self.manualID_temp_effects = dataset.data[mid_avg_names[0]][self.manualID_indices]
            self.manualID_salt_effects = dataset.data[mid_avg_names[1]][self.manualID_indices]

    def extract(self, dataset) -> None:
        dataset.identification.effects = self
    
    def run(self, dataset):
        self.get_original_indices(dataset)
        self.get_intrusion_effects(dataset)
        self.extract(dataset)


@dataclass
class intrusion_analysis:
    
    analysis_type : str
    coefficients : List[int]

    OP_temp_coeff : int = field(default_factory=int)
    OP_salt_coeff : int = field(default_factory=int)
    OP_performance : int = field(default_factory=int)
    OP_performance_spec : Dict[str, Any] = field(default_factory=dict)

    table_coefficients : Dict[str, Any] = field(default_factory=dict)
    table_coefficients_error : Dict[str, Any] = field(default_factory=dict)

    
    lin = "-"*6+' ' # For printing purposes
    OF_range = [-1, 1]  # The range of values that the optimization funtion 

    def intrusion_identification(self, lst: list[int], dataset) -> list[datetime]:
        """Uses given coefficients to identify intrusion events. These
        numbers represent the changes in these variables that should be flagged
        as intrusion events based on a depth-average value (either 60-botttom
        for deep, or mid depths that will depend on raw data transformation)"""
        
        temp_intrusion_coeff, salt_intrusion_coeff = lst

        column_avgs_temp = dataset.data[bottom_avg_names[0]]
        column_avgs_salt = dataset.data[bottom_avg_names[1]]

        if dataset.identification.intrusion_type.upper() == 'MID':
            column_avgs_temp = dataset.data[mid_avg_names[0]]
            column_avgs_salt = dataset.data[mid_avg_names[1]]

        intrusion_temp_indices = list(np.where(column_avgs_temp > temp_intrusion_coeff)[0]+1)
        intrusion_salt_indices = list(np.where(column_avgs_salt > salt_intrusion_coeff)[0]+1)

        all_timestamps = pd.DataFrame(dataset.dates_stamp)

        temp_intrusion_dates = all_timestamps.iloc[intrusion_temp_indices]
        salt_intrusion_dates = all_timestamps.iloc[intrusion_salt_indices]

        estimated_intrusion_dates = [value for value in temp_intrusion_dates.values.tolist() 
                                    if value in salt_intrusion_dates.values.tolist()]
        estimated_intrusion_dates = [item for sublist in estimated_intrusion_dates for item in sublist]
        estimated_intrusion_dates = timestamp2datetime_lists(estimated_intrusion_dates)

        return estimated_intrusion_dates


    def intrusion_id_performance(self, package: list) -> int:
        """Compares the manually identified intrusion and the estimated intrusions
        to evaluate the coefficient performance"""
        logger.debug(f'Package: {package}')
        dataset = package[0]
        lst = package[1]

        estimated_intrusion_dates = self.intrusion_identification(lst, dataset)
        real_intrusion_dates = dataset.identification.manualID_dates
        comparison_dates = date_comparison(real_intrusion_dates, estimated_intrusion_dates)
            
        missed_id = comparison_dates['Only Manual']
        extra_id = comparison_dates['Only Estimated']

        # Performance Parameters
        if len(estimated_intrusion_dates) != 0:
            missed_id_parameter = len(missed_id)/len(real_intrusion_dates)
            extra_id_parameter = len(extra_id)/len(estimated_intrusion_dates)

            performance_parameter = ((len(real_intrusion_dates) * missed_id_parameter +
                                      len(estimated_intrusion_dates) * extra_id_parameter)/
                                    (len(real_intrusion_dates)+len(estimated_intrusion_dates)))
        else:
            performance_parameter = 1

        return performance_parameter


    def estimate_coefficients(self, dataset) -> None:
        """Estimates optimized coeeficients by iterarting through temperature coefficients
        between self.OF_range and salinity coefficients between 0 to self.OF_range[1]
        and finding the combination with the best results based on a performance parameter"""

        logger.info(self.lin+'Estimating coefficients for optimized intrusion identification')

        real_intrusion_dates = dataset.identification.manualID_dates
        range = self.OF_range
        
        temp_range = np.arange(range[0],range[1],0.025)
        salt_range = np.arange(0,range[1],0.02)
        result_final = []

        # Minimize the performance parameter
        for temp_guess in temp_range:
            for salt_guess in salt_range:
                initial_guess = [dataset,[temp_guess, salt_guess]]
                logger.debug(f'Initial Guess: {initial_guess}')
                result = minimize(self.intrusion_id_performance, initial_guess)
                result_final.append((result.x, result.fun))

        best_coefficients = min(result_final, key= lambda x: x[1])

        self.OP_performance = best_coefficients[1] 
        self.OP_temp_coeff = list(best_coefficients[0])[0] 
        self.OP_salt_coeff = list(best_coefficients[0])[1] 
        
        self.OP_performance_spec = date_comparison(real_intrusion_dates, 
                                                self.intrusion_identification([self.OP_temp_coeff, self.OP_salt_coeff], dataset))
        

    def known_coefficients(self, dataset):

        self.OP_temp_coeff = self.coefficients[0] 
        self.OP_salt_coeff = self.coefficients[1]

        self.OP_performance = self.intrusion_id_performance([dataset,self.coefficients])
        
        self.OP_performance_spec = date_comparison(dataset.identification.manualID_dates, 
                                                self.intrusion_identification(self.coefficients, dataset))

    def extract(self, dataset) -> None:
        dataset.analysis = self

    def run(self, dataset):
        if self.analysis_type.upper() == 'GET_COEFFICIENTS':
            self.estimate_coefficients(dataset)
        else:
            self.known_coefficients(dataset)

        self.extract(dataset)

@dataclass
class meta:

    table_coefficients_error_comb : Dict[str, Any] = field(default_factory=dict)
    
    meta_path = '../data/PROCESSED/TABLES/'
    coeff_error_table = 'coefficients_error.csv'
    coeff_table = 'coefficients.csv'
    intrusions_table = 'intrusionID+effect.csv'
    meta_table = 'metadata_intrusions.csv'

    @staticmethod
    def count_csv_rows(path) -> int:
        """Count number of rows to identify the new recording's index"""
        with open(path,'r') as file:
            read = csv.reader(file)
            row_count = sum(1 for _ in read)

        return row_count
    

    def record_single(self, table, dicts) -> None:
        """Record single row metadata"""
        table_path = self.meta_path+table
        row_num1 = self.count_csv_rows(table_path)

        if row_num1 == 0:
            dicts['ID'] = 1
            dataf= pd.DataFrame(dicts)
            dataf.to_csv(table_path,mode='a', header=True, index=False)
        else:
            dicts['ID'] = row_num1
            dataf= pd.DataFrame(dicts)
            dataf.to_csv(table_path,mode='a', header=False, index=False)

    def integrate_metadata(self, dataset) -> None:
        dataset.metadata_intrusions['Input_dataset'] = dataset.path
        dataset.metadata_intrusions['Current_time'] = time.ctime()
        dataset.metadata_intrusions['Init_year'] = [dataset.identification.uyears[0]]
        # Record Final Year 
        dataset.metadata_intrusions['End_year'] = [dataset.identification.uyears[-1]]
        dataset.metadata_intrusions['Intrusion_type'] = [dataset.identification.intrusion_type]
        dataset.metadata_intrusions['manual_input_type'] = dataset.identification.manual_input_type
        dataset.metadata_intrusions['manual_input_path'] = dataset.identification.manual_input
        dataset.metadata_intrusions['manual_input_save'] = dataset.identification.save 
        dataset.metadata_intrusions['Variables_used'] = str(['salinity', 'temperature']) # Record Variables used

        dataset.identification.table_IDeffects['Dates'] = dataset.identification.manualID_dates
        dataset.identification.table_IDeffects['Index'] = dataset.identification.effects.manualID_indices # Record Intrusion indices
        dataset.identification.table_IDeffects['Temp_effects'] = dataset.identification.effects.manualID_temp_effects # Record Intrusion effects
        dataset.identification.table_IDeffects['Salt_effects'] = dataset.identification.effects.manualID_salt_effects

        dataset.analysis.table_coefficients['Temp_coefficient'] = [dataset.analysis.OP_temp_coeff] # Record Optimized tempewrature coefficient
        dataset.analysis.table_coefficients['Salt_coefficient'] = [dataset.analysis.OP_salt_coeff] # Record Optimized salinity coefficient
        dataset.analysis.table_coefficients['Performance'] = [dataset.analysis.OP_performance] # Record performnace of optimized coefficients

        result_comp = dataset.analysis.OP_performance_spec
        dataset.analysis.table_coefficients_error['Missed'] = result_comp['Only Manual'] # Record Intrusions missed based on manual
        dataset.analysis.table_coefficients_error['Extra'] = result_comp['Only Estimated'] # Record False positives based on manual
        dataset.analysis.table_coefficients_error['Found'] = result_comp['Matched'] # Record Correct identification based on manual
    

    def record_metadata(self, dataset) -> None:
        """Record all the metadata into their corresponding .csv file"""

        row_num = self.count_csv_rows(self.meta_path+self.meta_table)
        rows_intrusion = len(dataset.identification.table_IDeffects['Dates'])

        rows_missed = len(dataset.analysis.table_coefficients_error['Missed'])
        rows_extra = len(dataset.analysis.table_coefficients_error['Extra'])
        rows_found = len(dataset.analysis.table_coefficients_error['Found'])
        self.table_coefficients_error_comb['Type'] = ['Missed']*rows_missed + ['Extra']*rows_extra + ['Found']*rows_found
        self.table_coefficients_error_comb['Dates'] = list(dataset.analysis.table_coefficients_error['Missed']) + list(dataset.analysis.table_coefficients_error['Extra']) + [sub[-1] for sub in list(dataset.analysis.table_coefficients_error['Found'])]
        rows_error = len(self.table_coefficients_error_comb['Dates'])

        if row_num == 0:
            index = 1
            head = True
        else:
            index = row_num
            head = False

        dataset.identification.table_IDeffects['ID'] = [index]*rows_intrusion
        self.table_coefficients_error_comb['Error'] = [index]*rows_error
        dataf_ideffects = pd.DataFrame(dataset.identification.table_IDeffects)
        dataf_error = pd.DataFrame(self.table_coefficients_error_comb)
        dataf_ideffects.to_csv(self.meta_path+self.intrusions_table,mode='a', header=head, index=False)
        dataf_error.to_csv(self.meta_path+self.coeff_error_table,mode='a', header=head, index=False)
        
        self.record_single(self.meta_table, dataset.metadata_intrusions)
        self.record_single(self.coeff_table, dataset.analysis.table_coefficients)

    def run(self, dataset):
        self.integrate_metadata(dataset)
        self.record_metadata(dataset) 

def main() -> None:
  # Get command line arguments
    varsin = {
            'file_name': 'BBMP_salected_data0.pkl',
            'intrusion_type': 'NORMAL',
            'ID_type': 'MANUAL',
            'analysis_type': 'GET_COEFFICIENTS',
            'coefficient_temp': 0.5,
            'coefficient_salt': 0.5,
            'save_manual': 'OFF',
            'manual_input': 'manualID_MID1720009644.pkl'
        }

    file_name, intrusion_type, id_type, analysis_type, coefficient_temp, coefficient_salt, save_manual, manual_input = get_command_line_args(varsin)
    coefficients = [coefficient_temp, coefficient_salt]

    path_data = '../data/PROCESSED/'
    file_dirpath = path_data + file_name

    bbmp = dataset(file_dirpath)

    if id_type.upper() == 'MANUAL':
        intrusion_identification = manual_identification(intrusion_type, save_manual)
    else:
        intrusion_identification = imported_identification(intrusion_type, path_data +manual_input)

    intrusion_identification.run(bbmp)

    intrusion_effects = intrusion_data()
    intrusion_effects.run(bbmp)

    analysis = intrusion_analysis(analysis_type, coefficients)
    analysis.run(bbmp)

    data_meta = meta()
    data_meta.run(bbmp)


if __name__ == '__main__':
    main()






# class Intrusions:
    """Creates an Object representing a specific intrusion identification strategy
    and record metadata and data lineage"""

    # Internal Variables
    # lin = "-"*6+' ' # For printing purposes
    # dates_error = 10 # Allowed days between manual(by user) and estimate(by the algorithm) intrusion event 
    # OF_range = [-1, 1]  # The range of values that the optimization funtion will use for temperature and salinity coefficients

    # Range for plotting purposes
    # temp_range = [0, 10] 
    # salt_range = [30.5, 31.5] 
    # oxy_range = [0, 12] 

    ## Based on current schema
    # dates_name = 'sample_timestamps'
    # depth_name = 'sample_depth'
    # bottom_avg_names = ['sample_diff_row_temp', 'sample_diff_row_salt'] 
    # mid_avg_names = ['sample_diff_midrow_temp', 'sample_diff_midrow_salt'] 
    
    ## Table files
    # meta_path = '../data/PROCESSED/TABLES/'
    # coeff_error_table = 'coefficients_error.csv'
    # coeff_table = 'coefficients.csv'
    # intrusions_table = 'intrusionID+effect.csv'
    # meta_table = 'metadata_intrusions.csv'

    # def __init__(self, path: str) -> None:
        
        # # Metadata Tables {columns:metadata}
        # self.metadata_intrusions = {}
        # self.table_IDeffects = {}
        # self.table_coefficients = {}
        # self.table_coefficients_error = {}
        # self.table_coefficients_error_comb = {}

        # # Dates from the BBMP Profile Data
        # self.uyears= []

        # # Dates and Effects from Manually Identified Intrusion Events
        # self.manualID_dates = []
        # self.manualID_indices = []
        # self.manualID_temp_effects = []
        # self.manualID_salt_effects = []

        # # Automatic Intrusion Identification Coefficients and Performance
        # self.OP_performance = []
        # self.OP_temp_coeff = []
        # self.OP_salt_coeff = []
        # self.OP_Missed = []
        # self.OP_Extra = []
        # self.OP_Found = []

        # print(self.lin+'Importing Data')
        # self.metadata_intrusions['Input_dataset'] = path # Record Input
        # stat_info = os.stat(path)
        # self.metadata_intrusions['Date_created'] = time.ctime(stat_info.st_birthtime) # Record Time
        # self.data = import_joblib(path) # Import profile data
        # self.dates_stamp = self.data[self.dates_name]
        # self.dates = timestamp2datetime_lists(self.dates_stamp)

    
    # def separate_yearly_profiles(self) -> dict[dict]:
    #     self.dates_stamp = self.data[self.dates_name]
    #     self.dates = timestamp2datetime_lists(self.dates_stamp)

    #     grouped_years, self.uyears = separate_yearly_dates(self.dates)

    #     # Record Initial Year
    #     self.metadata_intrusions['Init_year'] = [self.uyears[0]]
    #     # Record Final Year 
    #     self.metadata_intrusions['End_year'] = [self.uyears[-1]]

    #     # Create dictionary with yearly profiles indices
    #     by_year_indices = {year: [self.dates.index(dt) for dt in grouped_years[year]] 
    #                     for year in self.uyears}

    #     # Extract yearly profiles of temperature and salinity
    #     yearly_profiles_temp, yearly_profiles_salt = create_yearly_matrices(self.data, by_year_indices)
            
    #     return {'Yearly Temp Profile': yearly_profiles_temp, 
    #             'Yearly Salt Profile': yearly_profiles_salt, 
    #             'Indices by Year':by_year_indices}


    # def plot_year_profiles(self, year_data: dict[dict], yr: int) -> dict:
    
    #     init_date_index = year_data['Indices by Year'][yr][0]
    #     last_date_index = year_data['Indices by Year'][yr][-1]
    #     datetime_list = self.dates[init_date_index:last_date_index]

    #     # Extract specific year data
    #     fig, axs = plt.subplots(2)
    #     year_temp_data = year_data['Yearly Temp Profile'][yr]
    #     year_salt_data = year_data['Yearly Salt Profile'][yr]

    #     # Temperature Plot
    #     xmesh,ymesh = np.meshgrid(datetime_list, self.data[self.depth_name])
    #     mesh0 = axs[0].pcolormesh(xmesh,ymesh,year_temp_data[:,:len(ymesh[0,:])], cmap='seismic')
    #     fig.colorbar(mesh0, ax=axs[0])
    #     axs[0].invert_yaxis()
    #     mesh0.set_clim(self.temp_range)
    #     axs[0].set_xticks([])

    #     # Salinity Plot
    #     mesh1 = axs[1].pcolormesh(xmesh,ymesh,year_salt_data[:,:len(ymesh[0,:])], cmap='seismic')
    #     fig.colorbar(mesh1, ax=axs[1])
    #     axs[1].invert_yaxis()
    #     mesh1.set_clim(self.salt_range)
    #     axs[1].xaxis.set_major_formatter(mdates.DateFormatter("%m"))

    #     fig.tight_layout()
    #     axs[0].text(0.02,0.85,str(yr), transform=axs[0].transAxes,fontsize=14,
    #                 verticalalignment='bottom',horizontalalignment='left',
    #                 bbox=dict(facecolor='white',alpha=0.5))

    #     return {
    #         'Figure':fig,
    #         'Axes':axs,
    #         'Mesh':[mesh0,mesh1]
    #     }

    # @staticmethod
    # def from_1970(date: int) -> datetime:
    #     """Converts points selected from plot to datetime"""

    #     reference_date = datetime(1970, 1, 1)

    #     delta = timedelta(days=date)
    #     datetime_obj = reference_date + delta

    #     return datetime_obj


    # def user_intrusion_selection(self, yearly_profiles: dict[any]) -> None:
    #     print(self.lin+'Intrusion identification in progress')

    #     # Plots Temperature and Salinity profiles for user to select intrusion dates by year
    #     for yr in self.uyears:
    #         fig = self.plot_year_profiles(yearly_profiles, 
    #                             yr)

    #         fig['Figure'].canvas.mpl_connect('button_press_event', onclick)

    #         fig['Figure'].canvas.mpl_connect('key_press_event', onkey)

    #         plt.show()

    #     intrusion_dates = list(np.array(get_points())[:,0])
    #     self.manualID_dates = [self.from_1970(dt) for dt in intrusion_dates]
        
    #     self.table_IDeffects['Dates'] = self.manualID_dates # Record intrusion dates
    #     print(self.lin+'Intrusion identification completed')


    # def date_comparison(dates1:list[datetime], dates2:list[datetime], dates_error=0) -> dict[list]:
    #     """Compares datetime lists for similar (within self.dates_error) dates"""
    #     def within_days(dtes1:datetime, dtes2:datetime) ->int:
    #         calc = abs((dtes2 - dtes1).days)
    #         return calc

    #     matching = []
    #     unmatched_md = []

    #     for dt1 in dates1:
    #         found_match = False
    #         single_match = []

    #         for dt2 in dates2:
    #             diff = within_days(dt1, dt2)
    #             if diff <= dates_error:
    #                 single_match.append([diff, dt1, dt2])
    #                 found_match = True
    #                 break
    #         # Intrusions Not Found
    #         if not found_match:
    #             unmatched_md.append(dt1)
    #         else:
    #             # Intrusions Found
    #             if len(single_match) > 1:
    #                 diff_list = [match[0] for match in single_match]
    #                 min_index = [idx for idx, value in enumerate(diff_list) if value == min(diff_list)]
    #                 matching.append([single_match[min_index]])
    #             else:
    #                 matching.append(single_match) 
        
    #     matching = [item for sublist in matching for item in sublist]
    #     matching_estimated = [sublist[2] for sublist in matching]

    #     set1 = set(dates2)
    #     set2 = set(matching_estimated)
    #     # Extra Intrusions
    #     unmatched_ed = list(set1-set2)

    #     return {
    #         'Matched':matching,
    #         'Only Manual':unmatched_md,
    #         'Only Estimated':unmatched_ed,
    #     }


    # def get_original_indices(self) -> None:
    #     """Get the indices of the intrusions identified from the main data (self.dates)"""
    #     comparison_results = date_comparison(self.manualID_dates, self.dates)
    #     compared_dates = comparison_results['Matched']
    #     intrusion_dates = [match[2] for match in compared_dates]
    #     self.manualID_indices = [i for i, dt1 in enumerate(self.dates) for j, dt2 in enumerate(intrusion_dates) if dt1 == dt2]
    #     self.table_IDeffects['Index'] = self.manualID_indices # Record Intrusion indices


    # def get_intrusion_effects(self) -> None:
    #     """Use the date indices from self.dates to identify the effects of those intrusions
    #     in self.data"""
    #     self.manualID_temp_effects = self.data[self.bottom_avg_names[0]][self.manualID_indices]
    #     self.manualID_salt_effects = self.data[self.bottom_avg_names[1]][self.manualID_indices]

    #     if self.manualID_type.upper() == 'MID':
    #         # Selecting data based on mid-depts
    #         self.manualID_temp_effects = self.data[self.mid_avg_names[0]][self.manualID_indices]
    #         self.manualID_salt_effects = self.data[self.mid_avg_names[1]][self.manualID_indices]
        
    #     self.table_IDeffects['Temp_effects'] = self.manualID_temp_effects # Record Intrusion effects
    #     self.table_IDeffects['Salt_effects'] = self.manualID_salt_effects

    #     self.metadata_intrusions['Variables_used'] = str(['salinity', 'temperature']) # Record Variables used

    
    # def intrusion_identification(self, lst: list[int]) -> list[datetime]:
    #     """Uses given coefficients to identify intrusion events. These
    #     numbers represent the changes in these variables that should be flagged
    #     as intrusion events based on a depth-average value (either 60-botttom
    #     for deep, or mid depths that will depend on raw data transformation)"""
        
    #     temp_intrusion_coeff, salt_intrusion_coeff = lst

    #     column_avgs_temp = self.data[self.bottom_avg_names[0]]
    #     column_avgs_salt = self.data[self.bottom_avg_names[1]]

    #     if self.manualID_type.upper() == 'MID':
    #         column_avgs_temp = self.data[self.mid_avg_names[0]]
    #         column_avgs_salt = self.data[self.mid_avg_names[1]]

    #     intrusion_temp_indices = list(np.where(column_avgs_temp > temp_intrusion_coeff)[0]+1)
    #     intrusion_salt_indices = list(np.where(column_avgs_salt > salt_intrusion_coeff)[0]+1)

    #     all_timestamps = pd.DataFrame(self.dates_stamp)

    #     temp_intrusion_dates = all_timestamps.iloc[intrusion_temp_indices]
    #     salt_intrusion_dates = all_timestamps.iloc[intrusion_salt_indices]

    #     estimated_intrusion_dates = [value for value in temp_intrusion_dates.values.tolist() 
    #                                 if value in salt_intrusion_dates.values.tolist()]
    #     estimated_intrusion_dates = [item for sublist in estimated_intrusion_dates for item in sublist]
    #     estimated_intrusion_dates = timestamp2datetime_lists(estimated_intrusion_dates)

    #     return estimated_intrusion_dates


    # def intrusion_id_performance(self, lst: list[int]) -> int:
    #     """Compares the manually identified intrusion and the estimated intrusions
    #     to evaluate the coefficient performance"""

    #     estimated_intrusion_dates = self.intrusion_identification(lst)
    #     real_intrusion_dates = self.manualID_dates
    #     comparison_dates = date_comparison(real_intrusion_dates, estimated_intrusion_dates)
            
    #     missed_id = comparison_dates['Only Manual']
    #     extra_id = comparison_dates['Only Estimated']

    #     # Performance Parameters
    #     if len(estimated_intrusion_dates) != 0:
    #         missed_id_parameter = len(missed_id)/len(real_intrusion_dates)
    #         extra_id_parameter = len(extra_id)/len(estimated_intrusion_dates)

    #         performance_parameter = ((len(real_intrusion_dates) * missed_id_parameter +
    #                                   len(estimated_intrusion_dates) * extra_id_parameter)/
    #                                 (len(real_intrusion_dates)+len(estimated_intrusion_dates)))
    #     else:
    #         performance_parameter = 1

    #     return performance_parameter


    # def estimate_coefficients(self) -> None:
    #     """Estimates optimized coeeficients by iterarting through temperature coefficients
    #     between self.OF_range and salinity coefficients between 0 to self.OF_range[1]
    #     and finding the combination with the best results based on a performance parameter"""

    #     print(self.lin+'Estimating coefficients for optimized intrusion identification')

    #     real_intrusion_dates = self.manualID_dates
    #     range = self.OF_range
        
    #     temp_range = np.arange(range[0],range[1],0.025)
    #     salt_range = np.arange(0,range[1],0.02)
    #     result_final = []

    #     # Minimize the performance parameter
    #     for temp_guess in temp_range:
    #         for salt_guess in salt_range:
    #             initial_guess = [temp_guess, salt_guess]
    #             result = minimize(self.intrusion_id_performance, initial_guess)
    #             result_final.append((result.x, result.fun))

    #     best_coefficients = min(result_final, key= lambda x: x[1])

    #     self.OP_performance = best_coefficients[1] 
    #     self.OP_temp_coeff = list(best_coefficients[0])[0] 
    #     self.OP_salt_coeff = list(best_coefficients[0])[1] 

    #     self.table_coefficients['Temp_coefficient'] = [self.OP_temp_coeff] # Record Optimized tempewrature coefficient
    #     self.table_coefficients['Salt_coefficient'] = [self.OP_salt_coeff] # Record Optimized salinity coefficient
    #     self.table_coefficients['Performance'] = [self.OP_performance] # Record performnace of optimized coefficients
        
    #     result_comp = date_comparison(real_intrusion_dates, 
    #                                             self.intrusion_identification([self.OP_temp_coeff, self.OP_salt_coeff]))
        
    #     self.OP_Missed = result_comp['Only Manual']
    #     self.OP_Extra = result_comp['Only Estimated']
    #     self.OP_Found = result_comp['Matched']

    #     self.table_coefficients_error['Missed'] = self.OP_Missed # Record Intrusions missed based on manual
    #     self.table_coefficients_error['Extra'] = self.OP_Extra # Record False positives based on manual
    #     self.table_coefficients_error['Found'] = self.OP_Found # Record Correct identification based on manual

    # @staticmethod
    # def count_csv_rows(path) -> int:
    #     """Count number of rows to identify the new recording's index"""
    #     with open(path,'r') as file:
    #         read = csv.reader(file)
    #         row_count = sum(1 for _ in read)

    #     return row_count
    

    # def record_single(self, table, dicts) -> None:
    #     """Record single row metadata"""
    #     table_path = self.meta_path+table
    #     row_num1 = self.count_csv_rows(table_path)

    #     if row_num1 == 0:
    #         dicts['ID'] = 1
    #         dataf= pd.DataFrame(dicts)
    #         dataf.to_csv(table_path,mode='a', header=True, index=False)
    #     else:
    #         dicts['ID'] = row_num1
    #         dataf= pd.DataFrame(dicts)
    #         dataf.to_csv(table_path,mode='a', header=False, index=False)

    
    # def automatedid(self, coefficients, manual_input):
    #     """Identify intrusion events based on coefficients for temperature
    #     and salinity, and based on a manual intrusion identification file
    #     """
    #     # Recording metadata
    #     self.metadata_intrusions['manual_input_type'] = 'N/A'
    #     self.metadata_intrusions['manual_input_path'] = 'N/A'
    #     self.metadata_intrusions['manual_input_path'] = manual_input
    #     intrusion_dates = import_joblib(manual_input)
    #     self.manualID_dates = intrusion_dates
    #     self.table_IDeffects['Dates'] = self.manualID_dates
    #     self.metadata_intrusions['manual_input_save'] = 'OFF'

    #     self.get_original_indices()
    #     self.get_intrusion_effects()

    #     self.table_coefficients['Temp_coefficient'] = [coefficients[0]]
    #     self.table_coefficients['Salt_coefficient'] = [coefficients[1]]

    #     self.OP_performance = self.intrusion_ID_performance(coefficients)
    #     result_comp = date_comparison(self.manualID_dates, 
    #                                             self.intrusion_identification(coefficients))
        
    #     self.table_coefficients['Performance'] = [self.OP_performance]
    #     self.table_coefficients_error['Missed'] = result_comp['Only Manual']
    #     self.table_coefficients_error['Extra'] = result_comp['Only Estimated']
    #     self.table_coefficients_error['Found'] = result_comp['Matched']

    
    # def record_metadata(self) -> None:
    #     """Record all the metadata into their corresponding .csv file"""

    #     row_num = self.count_csv_rows(self.meta_path+self.meta_table)
    #     rows_intrusion = len(self.table_IDeffects['Dates'])

    #     rows_missed = len(self.table_coefficients_error['Missed'])
    #     rows_extra = len(self.table_coefficients_error['Extra'])
    #     rows_found = len(self.table_coefficients_error['Found'])
    #     self.table_coefficients_error_comb['Type'] = ['Missed']*rows_missed + ['Extra']*rows_extra + ['Found']*rows_found
    #     self.table_coefficients_error_comb['Dates'] = list(self.table_coefficients_error['Missed']) + list(self.table_coefficients_error['Extra']) + [sub[-1] for sub in list(self.table_coefficients_error['Found'])]
    #     rows_error = len(self.table_coefficients_error_comb['Dates'])

    #     if row_num == 0:
    #         index = 1
    #         head = True
    #     else:
    #         index = row_num
    #         head = False

    #     self.table_IDeffects['ID'] = [index]*rows_intrusion
    #     self.table_coefficients_error_comb['Error'] = [index]*rows_error
    #     dataf_ideffects = pd.DataFrame(self.table_IDeffects)
    #     dataf_error = pd.DataFrame(self.table_coefficients_error_comb)
    #     dataf_ideffects.to_csv(self.meta_path+self.intrusions_table,mode='a', header=head, index=False)
    #     dataf_error.to_csv(self.meta_path+self.coeff_error_table,mode='a', header=head, index=False)
        
    #     self.record_single(self.meta_table, self.metadata_intrusions)
    #     self.record_single(self.coeff_table, self.table_coefficients)
    

# def main() -> None:
#     # Get command line arguments
#     varsin = {
#             'file_name': 'BBMP_salected_data0.pkl',
#             'intrusion_type': 'NORMAL',
#             'ID_type': 'MANUAL',
#             'manual_type': 'MANUAL',
#             'coefficients': [0.5, 0.5],
#             'save_manual': 'OFF',
#             'manual_input': 'manualID_MID1720009644.pkl'
#         }

#     file_name, intrusion_type, id_type, manual_type, coefficients, save_manual, manual_input = get_command_line_args(varsin)

#     path_data = '../data/PROCESSED/'
#     file_dirpath = path_data + file_name
    
#     # Initializing intrusion object
#     bbmp = Intrusions(file_dirpath)

#     yearly_profiles = bbmp.separate_yearly_profiles()

#     # Recording metadata
#     bbmp.metadata_intrusions['ID_type'] = id_type.upper()
#     bbmp.metadata_intrusions['Current_time'] = [int(time.time())]
#     bbmp.manualID_type = intrusion_type
#     bbmp.metadata_intrusions['Intrusion_type'] = bbmp.manualID_type

#     if id_type.upper() == 'MANUAL':
#         bbmp.metadata_intrusions['manual_input_type'] = manual_type.upper()
        
#         if manual_type.upper() == 'MANUAL':
#             # Manual identification through plots
#             bbmp.metadata_intrusions['manual_input_path'] = 'N/A'
#             bbmp.user_intrusion_selection(yearly_profiles)
            
#             if save_manual.upper() == 'ON':
#                 # Save manually identified intrusion
#                 man_name = 'manualID_' + bbmp.manualID_type + str(int(time.time())) + '.pkl'
#                 save_joblib(bbmp.manualID_dates, man_name)
#         else:
#             # Manual indeitification through importing file
#             manual_input_path = path_data + manual_input
#             bbmp.metadata_intrusions['manual_input_path'] = manual_input_path
#             intrusion_dates = import_joblib(manual_input_path)
#             bbmp.manualID_dates = intrusion_dates
#             bbmp.table_IDeffects['Dates'] = bbmp.manualID_dates
        
#         # Record Metadata
#         bbmp.metadata_intrusions['manual_input_save'] = save_manual.upper()
#         bbmp.get_original_indices()
#         bbmp.get_intrusion_effects()

#         # Estimate optimized coefficients
#         bbmp.estimate_coefficients()

#     else:
#         # Automated identification using specific coefficients
#         bbmp.automatedid(coefficients, path_data + manual_input)

#     # Record metadata in .csv files
#     bbmp.record_metadata()

#     # return bbmp

 
# if __name__ == '__main__':
#     main()

    