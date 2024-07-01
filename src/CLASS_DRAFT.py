import joblib
import os
from datetime import datetime,timedelta
import numpy as np
import matplotlib.dates as mdates
import matplotlib.pyplot as plt
import pandas as pd
from scipy.optimize import minimize

#--------------------------------------------------------------------------------------
#--------------------------------------------------------------------------------------

def import_joblib(file_path: str) -> any:

    data: any = joblib.load(file_path)
    
    return data

#--------------------------------------------------------------------------------------
#--------------------------------------------------------------------------------------

def timestamp2datetime_lists(lst:list[int]) -> list[datetime]:
    datetime_list:list[datetime] = [datetime.fromtimestamp(ts) for ts in lst]
    return datetime_list

#--------------------------------------------------------------------------------------
#--------------------------------------------------------------------------------------

def separate_yearly_dates(datetime_list:list[datetime]) -> dict[list]:
    years_extracted:list = np.unique([dt.year for dt in datetime_list])

    grouped_years:dict[list] = {year: [] for year in years_extracted}
    for i in datetime_list:
        grouped_years[i.year].append(i)

    return grouped_years, years_extracted

#--------------------------------------------------------------------------------------
#--------------------------------------------------------------------------------------

def create_yearly_matrices(selected_data:dict, year_indices:dict[list]) -> dict[dict]:
    Temp_dataframe = selected_data['sample_matrix_temp']
    Salt_dataframe = selected_data['sample_matrix_salt']
    
    selected_data_temp = Temp_dataframe.to_numpy()
    selected_data_salt = Salt_dataframe.to_numpy()

    yearly_profiles_temp: dict = {}
    yearly_profiles_salt: dict = {}

    for year, indices in year_indices.items():
        yearly_profile_temp = selected_data_temp[:, indices]
        yearly_profiles_temp[year] = yearly_profile_temp

        yearly_profile_salt = selected_data_salt[:, indices]
        yearly_profiles_salt[year] = yearly_profile_salt

    return yearly_profiles_temp, yearly_profiles_salt

#--------------------------------------------------------------------------------------
#--------------------------------------------------------------------------------------

points = []
def onclick(event):
    if event.button == 1:
        if event.inaxes is not None:
            x, y = event.xdata, event.ydata

            points.append((x, y))

            print(f"Point selected: ({x}, {y})")
            event.inaxes.plot(x, y, 'ro')
            event.canvas.draw()

#--------------------------------------------------------------------------------------
#--------------------------------------------------------------------------------------

def onkey(event):
    if event.key == ' ':
        plt.close(event.canvas.figure)

#--------------------------------------------------------------------------------------
#--------------------------------------------------------------------------------------

def get_points():
    return points

#--------------------------------------------------------------------------------------
#--------------------------------------------------------------------------------------

class intrusions:

    def __init__(self, PATH) -> None:
        self.metadata_intrusions = {}
        self.table_IDeffects = {}
        self.table_coefficients = {}
        self.table_coefficients_error = {}
        self.lin = "-"*6+' '
        self.manualID_type = ''
        print(self.lin+'Importing Data')
        self.dates_name = 'sample_timestamps'
        self.depth_name = 'sample_depth'
        self.temp_range = [0,10]
        self.salt_range = [30.5,31.5]
        self.oxy_range = [0,12]
        self.dates_error = 10
        self.bottom_avg_names = ['sample_diff_row_temp', 'sample_diff_row_salt']
        self.mid_avg_names = ['sample_diff_midrow_temp', 'sample_diff_midrow_salt']
        self.OF_range = [-1, 1]

        self.metadata_intrusions['Input_dataset'] = PATH
        self.data = import_joblib(PATH)

    
    def separate_yearly_profiles(self) -> dict[dict]:
        self.dates_stamp = self.data[self.dates_name]
        self.dates = timestamp2datetime_lists(self.dates_stamp)

        print(self.dates)

        grouped_years, self.uyears  = separate_yearly_dates(self.dates)

        self.metadata_intrusions['Init_year'] = [self.uyears[0]]
        self.metadata_intrusions['End_year'] = [self.uyears[-1]]

        by_year_indices = {year: [self.dates.index(dt) for dt in grouped_years[year]] 
                        for year in self.uyears}

        yearly_profiles_temp, yearly_profiles_salt = create_yearly_matrices(self.data, by_year_indices)
            
        return {'Yearly Temp Profile': yearly_profiles_temp, 
                'Yearly Salt Profile': yearly_profiles_salt, 
                'Indices by Year':by_year_indices}


    def plot_year_profiles(self, year_data: dict[dict], yr: int):
    
        init_date_index = year_data['Indices by Year'][yr][0]
        last_date_index = year_data['Indices by Year'][yr][-1]
        datetime_list = self.dates[init_date_index:last_date_index]

        fig, axs = plt.subplots(2)
        year_temp_data = year_data['Yearly Temp Profile'][yr]
        year_salt_data = year_data['Yearly Salt Profile'][yr]

        X,Y = np.meshgrid(datetime_list, self.data[self.depth_name])
        mesh0 = axs[0].pcolormesh(X,Y,year_temp_data[:,:len(Y[0,:])], cmap='seismic')
        fig.colorbar(mesh0, ax=axs[0])
        axs[0].invert_yaxis()
        mesh0.set_clim(self.temp_range)
        axs[0].set_xticks([])

        mesh1 = axs[1].pcolormesh(X,Y,year_salt_data[:,:len(Y[0,:])], cmap='seismic')
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


    def from_1970(date: int) -> datetime:
        reference_date = datetime(1970, 1, 1)

        delta = timedelta(days=date)
        datetime_obj = reference_date + delta

        return datetime_obj


    def user_intrusion_selection(self, yearly_profiles: dict[any]) -> None:
        print(self.lin+'Intrusion identification in progress')

        for yr in self.uyears:
            fig = self.plot_year_profiles(self.data, yearly_profiles, 
                                yr)

            cid_click = fig['Figure'].canvas.mpl_connect('button_press_event', onclick)

            cid_key = fig['Figure'].canvas.mpl_connect('key_press_event', onkey)

            plt.show()

        intrusion_dates = list(np.array(get_points())[:,0])
        self.manualID_dates = [self.from_1970(dt) for dt in intrusion_dates]\
        
        self.table_IDeffects['Dates'] = self.manualID_dates
        print(self.lin+'Intrusion identification completed')


    def intrusion_date_comparison(self, dates1, dates2) -> dict[list]:
        def within_days(dt1, dt2):
            calc = abs((dt2 - dt1).days)
            return calc

        matching = []
        unmatched_md = []

        for dt1 in dates1:
            found_match = False
            single_match = []
            for dt2 in dates2:
                diff = within_days(dt1, dt2)
                if diff <= self.dates_error:
                    single_match.append([diff, dt1, dt2])
                    found_match = True
                    break

            if not found_match:
                unmatched_md.append(dt1)
            else:
                if len(single_match) > 1:
                    diff_list = [match[0] for match in single_match]
                    min_index = [idx for idx, value in enumerate(diff_list) if value == min(diff_list)]
                    matching.append([single_match[min_index]])
                else:
                    matching.append(single_match) 
        
        matching = [item for sublist in matching for item in sublist]
        matching_estimated = [sublist[2] for sublist in matching]

        set1 = set(dates2)
        set2 = set(matching_estimated)
        unmatched_ed = set1-set2

        return {
            'Matched':matching,
            'Only Manual':unmatched_md,
            'Only Estimated':unmatched_ed,
        }


    def get_original_indices(self) -> None:

        comparison_results = self.intrusion_date_comparison(self.manualID_dates, self.dates)
        compared_dates = comparison_results['Matched']
        intrusion_dates = [match[2] for match in compared_dates]
        self.manualID_indices = [i for i, dt1 in enumerate(self.dates) for j, dt2 in enumerate(intrusion_dates) if dt1 == dt2]
        self.table_IDeffects['Index'] = self.manualID_indices


    def get_intrusion_effects(self) -> None:

        self.manualID_temp_effects = self.data[self.bottom_avg_names[0]][self.manualID_indices]
        self.manualID_salt_effects = self.data[self.bottom_avg_names[1]][self.manualID_indices]

        if self.manualID_type.upper() == 'MID':
            self.manualID_temp_effects = self.data[self.mid_avg_names[0]][self.manualID_indices]
            self.manualID_salt_effects = self.data[self.mid_avg_names[1]][self.manualID_indices]
        
        self.table_IDeffects['Temp_effects'] = self.manualID_temp_effects
        self.table_IDeffects['Salt_effects'] = self.manualID_salt_effects

    
    def intrusion_identification(self, lst: list[int]) -> list[datetime]:
        temp_intrusion_coeff, salt_intrusion_coeff = lst

        column_avgs_temp = self.data[self.bottom_avg_names[0]]
        column_avgs_salt = self.data[self.bottom_avg_names[1]]

        if self.manualID_type.upper() == 'MID':
            column_avgs_temp = self.data[self.mid_avg_names[0]]
            column_avgs_salt = self.data[self.mid_avg_names[1]]

        intrusion_temp_indices = list(np.where(column_avgs_temp > temp_intrusion_coeff)[0]+1)
        intrusion_salt_indices = list(np.where(column_avgs_salt > salt_intrusion_coeff)[0]+1)

        all_timestamps = pd.DataFrame(self.dates_stamp)

        temp_intrusion_dates = all_timestamps.iloc[intrusion_temp_indices]
        salt_intrusion_dates = all_timestamps.iloc[intrusion_salt_indices]

        estimated_intrusion_dates = [value for value in temp_intrusion_dates.values.tolist() 
                                    if value in salt_intrusion_dates.values.tolist()]
        estimated_intrusion_dates = [item for sublist in estimated_intrusion_dates for item in sublist]
        estimated_intrusion_dates = timestamp2datetime_lists(estimated_intrusion_dates)

        return estimated_intrusion_dates


    def intrusion_ID_performance(self, lst: list[int]):
        
        estimated_intrusion_dates = self.intrusion_identification(lst)

        real_intrusion_dates = self.manualID_dates
        comparison_dates = self.intrusion_date_comparison(real_intrusion_dates, estimated_intrusion_dates)
            
        missed_id = comparison_dates['Only Manual']
        extra_id = comparison_dates['Only Estimated']
        caught_id = comparison_dates['Matched']

        if len(estimated_intrusion_dates) != 0:
            missed_id_parameter = len(missed_id)/len(real_intrusion_dates)
            extra_id_parameter = len(extra_id)/len(estimated_intrusion_dates)

            performance_parameter = ((len(real_intrusion_dates)*(missed_id_parameter) + 
                                    len(estimated_intrusion_dates)*extra_id_parameter)/
                                    (len(real_intrusion_dates)+len(estimated_intrusion_dates)))
        else:
            performance_parameter = 1

        return performance_parameter


    def estimate_coefficients(self) -> None:
        print(self.lin+'Estimating coefficients for optimized intrusion identification')

        real_intrusion_dates = self.manualID_dates
        range = self.OF_range
        
        temp_range = np.arange(range[0],range[1],0.025)
        salt_range = np.arange(0,range[1],0.02)
        result_final = []

        for temp_guess in temp_range:
            for salt_guess in salt_range:
                initial_guess = [temp_guess, salt_guess]
                print(initial_guess)
                result = minimize(self.intrusion_ID_performance, initial_guess)
                result_final.append((result.x, result.fun))

        best_coefficients = min(result_final, key= lambda x: x[1])

        self.OP_performance = best_coefficients[1]
        self.OP_temp_coeff = list(best_coefficients[0])[0]
        self.OP_salt_coeff = list(best_coefficients[0])[1]

        self.table_coefficients['Temp_coefficient'] = [self.OP_temp_coeff]
        self.table_coefficients['Salt_coefficient'] = [self.OP_salt_coeff]
        self.table_coefficients['Performance'] = [self.OP_performance]

        result_comp = self.intrusion_date_comparison(real_intrusion_dates, 
                                                self.intrusion_identification([self.OP_temp_coeff, self.OP_salt_coeff]))
        
        self.OP_Missed = result_comp['Only Manual']
        self.OP_Extra = result_comp['Only Estimated']
        self.OP_Found = result_comp['Matched']

        self.table_coefficients_error['Missed'] = self.OP_Missed
        self.table_coefficients_error['Extra'] = self.OP_Extra
        self.table_coefficients_error['Found'] = self.OP_Found

    #def metadata(self):


def main(file_name, intrusion_type) -> intrusions:
    
    file_PATH = '../DATA/PROCESSED/' + file_name
    
    BBMP = intrusions(file_PATH)

    yearly_profiles = BBMP.separate_yearly_profiles

    BBMP.user_intrusion_selection(yearly_profiles)
    BBMP.manualID_type = intrusion_type
    BBMP.metadata_intrusions['Type'] = BBMP.manualID_type
    BBMP.get_original_indices
    BBMP.get_intrusion_effects
    BBMP.estimate_coefficients

    return BBMP


if __name__ == '__main__':
    Data = main('BBMP_salected_data_test.pkl', 'Normal')

    