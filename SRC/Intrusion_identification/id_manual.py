import numpy as np
import matplotlib.dates as mdates
import matplotlib.pyplot as plt
import time
from dataclasses import dataclass, field
from typing import Dict, Any, List
from .id_method import id_method
from misc.other.logging import function_log
from misc.other.file_handling import *
from misc.other.plot_interact import *
from misc.other.date_handling import separate_yearly_dates
from datetime import datetime, timedelta

@function_log
@dataclass
class manual_identification(id_method):
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

    def fill_request_info(self, dates) -> None:
        self.dates = dates

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
        yearly_profiles_temp, yearly_profiles_salt = self.create_yearly_matrices(dataset.data, by_year_indices)
            
        return {'Yearly Temp Profile': yearly_profiles_temp, 
                'Yearly Salt Profile': yearly_profiles_salt, 
                'Indices by Year':by_year_indices}

    def plot_year_profiles(self, year_data: dict[dict], yr: int, dataset) -> dict:

        init_date_index = year_data['Indices by Year'][yr][0]
        last_date_index = year_data['Indices by Year'][yr][-1]
        datetime_list = self.dates[init_date_index:last_date_index]

        # Extract specific year data
        fig, axs = plt.subplots(2)
        year_temp_data = year_data['Yearly Temp Profile'][yr]
        year_salt_data = year_data['Yearly Salt Profile'][yr]

        # Temperature Plot
        xmesh,ymesh = np.meshgrid(datetime_list, dataset.data[self.depth_name])
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


    def user_intrusion_selection(self,dataset) -> None:
        # logger.info(self.lin+'Intrusion identification in progress')

        yearly_profiles = self.separate_yearly_profiles(dataset)
        # Plots Temperature and Salinity profiles for user to select intrusion dates by year
        for yr in self.uyears:
            fig = self.plot_year_profiles(yearly_profiles, 
                                yr, dataset)

            fig['Figure'].canvas.mpl_connect('button_press_event', onclick)

            fig['Figure'].canvas.mpl_connect('key_press_event', onkey)

            plt.show()

        intrusion_dates = list(np.array(get_points())[:,0])
        self.manualID_dates = [self.from_1970(dt) for dt in intrusion_dates]
        
        # logger.info(self.lin+'Intrusion identification completed')

    def save_identification(self) -> None:
        man_name = 'manualID_' + self.intrusion_type + str(int(time.time())) + '.pkl'
        save_joblib(self.manualID_dates, man_name)
        self.save = man_name

    def extract(self, dataset) -> None:
        dataset.identification = self
    
    def run(self, dataset):
        self.fill_request_info(dataset.dates)
        self.user_intrusion_selection(dataset)

        if self.save != 'OFF':
            self.save_identification()
        
        self.extract(dataset)