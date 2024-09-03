import pandas as pd
import matplotlib.dates as mdates
import matplotlib.pyplot as plt
from matplotlib import gridspec
import time
from misc.other.plot_interact import *
from misc.other.date_handling import separate_yearly_dates
from datetime import datetime, timedelta

from .config import *

# Transformation names based on ETL_data_loading
bottom_avg_names = ['sample_diff_row_temp', 'sample_diff_row_salt'] 
mid_avg_names = ['sample_diff_midrow_temp', 'sample_diff_midrow_salt'] 

@function_log
@dataclass
class manual_identification(id_method):
    """
    Allows you to manually identify the intrusions using time-depth temperature
    and salinity space, and a time series of the average change in salinity,
    temperature, and oxygen for every year of the available data in the provided
    dataset.

    ----------------Inputs
          intrusion_type:   Normal (Deep), 
                            Mid (Mid-depth), 
                            Other (Deep; e.g., Winter)
                    save:   ON for save the results of Manual

   ----------------Important class attributes
    - manualID_dates : Dates identified
    - table_IDeffects : Table for intrusion effects ('intrusionID+effect.csv')
    - effects : Class(id_method(ABC))
    """
    save : str

    manualID_dates : List[int] = field(default_factory=list)
    table_IDeffects : Dict[str, Any] = field(default_factory=dict)
    effects : object = field(default_factory=empty)

    depth_name = 'sample_depth' # Depth field name based on ETL_data_loading
    temp_range = [0,10] # Plotting ranges
    salt_range = [30.5,31.5] 
    oxy_range = [0,12]

    def __init__(self):
        self.manual_input = 'N/A'
        self.manual_input_type = 'MANUAL'

    def fill_request_info(self, dates: list[datetime]) -> None:
        """
        Extract required fields from identification request
        """
        self.dates = dates

        self.uyears  = np.unique([dt.year for dt in self.dates])

    @staticmethod
    def create_yearly_matrices(selected_data:dict, 
                               year_indices:dict[list]) -> list[dict]:
        """
        Separates indices obtained from separate_yearly_dates() and creates 
        matrices with the data ready for plotting (Time-Depth space)
        """
        yearly_profiles_temp: dict = {}
        yearly_profiles_salt: dict = {}

        selected_data_temp = selected_data['sample_matrix_temp'].to_numpy()
        selected_data_salt = selected_data['sample_matrix_salt'].to_numpy()

        for year, indices in year_indices.items():
            yearly_profiles_temp[year] = selected_data_temp[:, indices]
            yearly_profiles_salt[year] = selected_data_salt[:, indices]

        return [yearly_profiles_temp, yearly_profiles_salt]


    def separate_yearly_profiles(self, 
                                 dataset: RequestInfo_Analysis) -> dict[dict]:
        """
        Separates the profiles by year, so that create_yearly_matrices can 
        can create the Time-Depth space of each variable for each year
        """
        grouped_years = separate_yearly_dates(self.dates)

        # Create dictionary with yearly profiles indices
        by_year_indices = {
            year: [self.dates.index(dt) for dt in grouped_years[year]] 
                for year in self.uyears
                }

        # Extract yearly profiles of temperature and salinity
        yearly_profiles = self.create_yearly_matrices(dataset.data, 
                                                      by_year_indices)
        yearly_profiles_temp, yearly_profiles_salt = yearly_profiles
            
        return {'Yearly Temp Profile': yearly_profiles_temp, 
                'Yearly Salt Profile': yearly_profiles_salt, 
                'Indices by Year':by_year_indices}
    

    @staticmethod
    def vertical_line(datetime: datetime) -> None:
        """
        Creates vertical lines at the start of the year
        """
        years = pd.to_datetime(datetime).year.unique()
        if not len(years) == 1:
            for year in years:
                plt.axvline(
                    pd.Timestamp(f'{year}'), 
                    color='grey', 
                    linestyle='-', 
                    linewidth=1
                    )


    def format_data_plot(self, year_data: dict[dict], yr: int, 
                         dataset: RequestInfo_Analysis, yr2: int = 0, 
                         dtm = [datetime, datetime]) -> list[list]:
        """
        This function allows the user to plot 1 year, multiple years, or more
        specific time windows
        """
        yd = year_data
        dd = dataset.data
        byear_indices = yd['Indices by Year']

        # Check if this is a single year plot or multiple
        if yr2 == 0:
            yr2 = yr
            ini_year_indices = end_year_indices = byear_indices[yr] 
        else:
            ini_year_indices = byear_indices[yr]
            end_year_indices = byear_indices[yr2]

        # Check if plots is meant to be yearly(if dtm = False) or other time 
        # range (if dtm = True)
        if dtm:
            yr = dtm[0].year
            yr2 = dtm[1].year
            ini_year_indices = byear_indices[yr]
            end_year_indices = byear_indices[yr2]

            dt1 = self.dates.index(dtm[0])
            dt2 = self.dates.index(dtm[1])
            
            ini_year_day = ini_year_indices.index(dt1)
            end_year_day = end_year_indices.index(dt2)
        else:
            ini_year_day = 0
            end_year_day = -1

        # Get datetimes for the plot
        init_date_index = ini_year_indices[ini_year_day]
        last_date_index = end_year_indices[end_year_day]
        datetime_list = self.dates[init_date_index:last_date_index]

        # Get time series for the plot
        temp_line = dd[bottom_avg_names[0]][init_date_index:last_date_index]
        salt_line = dd[bottom_avg_names[1]][init_date_index:last_date_index]
        oxy_line = dd['sample_diff_row_oxy'][init_date_index:last_date_index]

        # Extract specified date data
        year_temp_data = yd['Yearly Temp Profile'][yr][ini_year_day:end_year_day]
        year_salt_data = yd['Yearly Salt Profile'][yr][ini_year_day:end_year_day]

        # Plot data is saved yearly, this creates a new ploting dataset with all 
        # the requested data, if required
        if not yr == yr2:
            count = -1
            temp_ini_index = 0
            temp_end_index = -1
            year_list = list(range(yr+1, yr2+1))
            for ano in year_list:
                count += 1
                if count == len(year_list):
                    temp_ini_index = ini_year_day
                    temp_end_index = end_year_day

                x = yd['Yearly Temp Profile'][ano][temp_ini_index:temp_end_index]
                y = yd['Yearly Salt Profile'][ano][temp_ini_index:temp_end_index]
                year_temp_data = np.concatenate((year_temp_data, 
                                                 x), 
                                                 axis= 1)
                year_salt_data = np.concatenate((year_salt_data, 
                                                 y), 
                                                 axis= 1)
        
        return [[dd], [yr, yr2],[datetime_list],
                [temp_line, salt_line, oxy_line],
                [year_temp_data, year_salt_data]]


    def plot_year_profiles(self, data_formated: list[list]) -> dict:
        """
        This function represents the plot characteristics and plots formatted 
        data
        """
        
        # Dates
        dd = data_formated[0][0]
        yr = data_formated[1][0]
        yr2 = data_formated[1][1]
        datetime_list = data_formated[2][0]

        # Time Series
        temp_line = data_formated[3][0]
        salt_line = data_formated[3][1]
        oxy_line = data_formated[3][2]

        # Time-Depth Space
        year_temp_data = data_formated[4][0]
        year_salt_data = data_formated[4][1]

        # Define grid specification with room for colorbars
        fig = plt.figure(figsize=(10,6))
        gs = gridspec.GridSpec(3, 2, width_ratios=[1, 0.05], 
                               height_ratios=[1, 1, 1], wspace=0.1)

        # Create subplots
        ax1 = fig.add_subplot(gs[0, 0])
        ax2 = fig.add_subplot(gs[1, 0])
        ax3 = fig.add_subplot(gs[2, 0])
        cax1 = fig.add_subplot(gs[0, 1])
        cax2 = fig.add_subplot(gs[1, 1])

        # Construct meshgrid and resize matrices
        xmesh, ymesh = np.meshgrid(datetime_list, dd[self.depth_name][0:-2])
        
        error = [len(xmesh[:, 0]), len(ymesh[0, :])]
        temp_matrix = year_temp_data[:error[0], :error[1]]
        salt_matrix = year_salt_data[:error[0], :error[1]]

        # Temperature Plot
        mesh0 = ax1.pcolormesh(xmesh, ymesh, temp_matrix, cmap='seismic', 
                               shading='nearest')
        cb0 = fig.colorbar(mesh0, cax=cax1)
        cb0.set_label('Temperature (°C)')
        ax1.invert_yaxis()
        mesh0.set_clim(self.temp_range)
        ax1.set_xticks([])
        ax1.set_ylabel("Depth (m)")

        # Salinity Plot
        mesh1 = ax2.pcolormesh(xmesh, ymesh, salt_matrix, cmap='seismic')
        cb1 = fig.colorbar(mesh1, cax=cax2)
        cb1.set_label('Salinity (PSU)')
        ax2.invert_yaxis()
        mesh1.set_clim(self.salt_range)
        ax2.xaxis.set_major_formatter(mdates.DateFormatter("%m"))
        ax2.set_xticks([])
        ax2.set_ylabel("Depth (m)")

        # Line plots for Temperature, Oxygen, and Salinity
        ax4 = ax3.twinx()  # Create a secondary y-axis for salinity

        # Plot Temperature and Oxygen on the primary y-axis
        ax3.plot(datetime_list, temp_line, color='r', label='Temperature')
        ax3.plot(datetime_list, oxy_line, color='g', label='Oxygen')

        # Plot Salinity on the secondary y-axis
        ax4.plot(datetime_list, salt_line, color='b', label='Salinity')

        # Labels, legends, and titles
        ax3.set_ylabel(r"$\overline{\Delta}$ Temperature (°C)"+
                       "\n" r"$\overline{\Delta}$ Oxygen (mg/L)")
        ax4.set_ylabel(r"$\overline{\Delta}$ Salinity (PSU)")
        ax3.set_xlabel("Month")
        ax3.xaxis.set_major_formatter(mdates.DateFormatter("%m"))

        self.vertical_line(datetime_list)

        ax3.legend(loc='upper left')
        ax4.legend(loc='upper right')

        # Set the same x-axis range for all plots
        common_xlim = [datetime_list[0], datetime_list[-1]]
        ax1.set_xlim(common_xlim)
        ax2.set_xlim(common_xlim)
        ax3.set_xlim(common_xlim)
        ax3.xaxis.set_major_formatter(mdates.DateFormatter("%m"))

        # Text box specifying the years plotted
        box_ = str(yr)
        if not yr == yr2:
            box_ = str(yr) + '-' + str(yr2)

        ax1.text(0.02, 0.80, box_, transform=ax1.transAxes, fontsize=14,
                verticalalignment='bottom', horizontalalignment='left',
                bbox=dict(facecolor='white', alpha=0.5))

        return {
            'Figure':fig,
            'Axes':ax1,
            'Mesh':[mesh0,mesh1]
        }


    def intrusion_plot(self, *args, **kargs) -> None:
        """
        To Be Tested. Unused for now
        """
        year_data = args[0]
        yr = args[1]
        dataset = args[2]
        yr2_ = kargs['yr2']
        dtm_ = kargs['dtm']

        formated_data = self.format_data_plot(year_data, yr, dataset, yr2= yr2_, 
                                              dtm= dtm_)
        self.plot_year_profiles(formated_data)



    @staticmethod
    def from_1970(date: int) -> datetime:
        """
        Converts points selected from plot to datetime
        """
        return datetime(1970, 1, 1) + timedelta(days=date)


    def user_intrusion_selection(self,dataset: RequestInfo_Analysis) -> None:
        """
        Triggers identification stage. Plots for user to select intrusion dates 
        by year. 
        """
        yearly_profiles = self.separate_yearly_profiles(dataset)
        for yr in self.uyears:
            format_data = self.format_data_plot(yearly_profiles, 
                                yr, dataset)
            fig = self.plot_year_profiles(format_data)

            fig['Figure'].canvas.mpl_connect('button_press_event', onclick)

            fig['Figure'].canvas.mpl_connect('key_press_event', onkey)

            plt.show()

        intrusion_dates = list(np.array(get_points())[:,0])
        self.manualID_dates = [self.from_1970(dt) for dt in intrusion_dates]
        

    def save_identification(self) -> None:
        """
        Saves manual identification as .pkl file witht the correct format
        'manualID_' + self.intrusion_type + now_time + '.pkl'
        """
        now_time = str(int(time.time()))
        man_name = 'manualID_' + self.intrusion_type + now_time + '.pkl'
        save_joblib(self.manualID_dates, man_name)
        self.save = man_name


    def extract(self, dataset: RequestInfo_Analysis) -> None:
        """
        Injects class into dataset
        """
        dataset.identification = self
    

    def run(self, dataset: RequestInfo_Analysis):
        """
        Steps: fill_request_info -> user_intrusion_selection -> 
        save_identification? -> extract
        """
        self.fill_request_info(dataset.dates)
        self.user_intrusion_selection(dataset)

        if self.save != 'OFF':
            self.save_identification()
        
        self.extract(dataset)