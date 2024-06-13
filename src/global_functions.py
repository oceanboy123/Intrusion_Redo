import joblib
import os
from datetime import datetime,timedelta
import numpy as np
import matplotlib.dates as mdates
import matplotlib.pyplot as plt
import pandas as pd
from scipy.optimize import minimize


def save_joblib(file_name: str, data: any) -> None:
    directory: str = 'data'
    file_path = os.path.join(directory, file_name)
    joblib.dump(data, file_path)


def import_joblib(file_name: str) -> any:
    directory: str = 'data'
    file_path = os.path.join(directory, file_name)
    data: any = joblib.load(file_path)
    
    return data


def timestamp2datetime_lists(lst:list[int]) -> list[datetime]:
    datetime_list:list[datetime] = [datetime.fromtimestamp(ts) for ts in lst]
    return datetime_list


def separate_yearly_dates(datetime_list:list[datetime]) -> dict[list]:
    years_extracted:list = np.unique([dt.year for dt in datetime_list])

    grouped_years:dict[list] = {year: [] for year in years_extracted}
    for i in datetime_list:
        grouped_years[i.year].append(i)

    return grouped_years, years_extracted


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


def separate_yearly_profiles(selected_data: dict) -> dict[dict]:
    sample_datetimes = timestamp2datetime_lists(selected_data['sample_timestamps'])

    grouped_years, unq_yrs  = separate_yearly_dates(sample_datetimes)

    by_year_indices = {year: [sample_datetimes.index(dt) for dt in grouped_years[year]] 
                       for year in unq_yrs}

    yearly_profiles_temp, yearly_profiles_salt = create_yearly_matrices(selected_data, by_year_indices)
        
    return {'Yearly Temp Profile': yearly_profiles_temp, 
            'Yearly Salt Profile': yearly_profiles_salt, 
            'Indices by Year':by_year_indices}


def plot_year_profiles(original_data:dict[any], year_data: dict[dict], yr: int, ranges: list[list]):
    
    init_date_index = year_data['Indices by Year'][yr][0]
    last_date_index = year_data['Indices by Year'][yr][-1]
    timestamp = original_data['sample_timestamps'][init_date_index:last_date_index]
    datetime_list = timestamp2datetime_lists(timestamp)

    fig, axs = plt.subplots(2)
    year_temp_data = year_data['Yearly Temp Profile'][yr]
    year_salt_data = year_data['Yearly Salt Profile'][yr]

    X,Y = np.meshgrid(datetime_list, original_data['sample_depth'])
    mesh0 = axs[0].pcolormesh(X,Y,year_temp_data[:,:len(Y[0,:])], cmap='seismic')
    fig.colorbar(mesh0, ax=axs[0])
    axs[0].invert_yaxis()
    mesh0.set_clim(ranges[0])
    axs[0].set_xticks([])

    mesh1 = axs[1].pcolormesh(X,Y,year_salt_data[:,:len(Y[0,:])], cmap='seismic')
    fig.colorbar(mesh1, ax=axs[1])
    axs[1].invert_yaxis()
    mesh1.set_clim(ranges[1])
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


def intrusion_date_comparison(manual_dates: list[datetime], estimated_dates: list[datetime], A_days) -> dict[list]:
    def within_days(dt1, dt2):
        calc = abs((dt2 - dt1).days)
        return calc

    matching = []
    unmatched_md = []
    unmatched_ed = []

    for dt1 in manual_dates:
        found_match = False
        single_match = []
        for dt2 in estimated_dates:
            diff = within_days(dt1, dt2)
            if diff <= A_days:
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

    for dt2 in estimated_dates:
        found_match = False

        for dt1 in manual_dates:
            if within_days(dt1, dt2):
                found_match = True
                break
        
        if not found_match:
            unmatched_ed.append(dt2)

    matching = [item for sublist in matching for item in sublist]
    return {
        'Matched':matching,
        'Only Manual':unmatched_md,
        'Only Estimated':unmatched_ed,
    }


def intrusion_identification(lst: list[int],sample_data: dict[any], intrusion_name: str) -> list[datetime]:
    temp_intrusion_coeff, salt_intrusion_coeff = lst

    column_avgs_temp = sample_data['sample_diff_row_temp']
    column_avgs_salt = sample_data['sample_diff_row_salt']

    if intrusion_name == 'sample_mid_timestamps':
        column_avgs_temp = sample_data['sample_diff_midrow_temp']
        column_avgs_salt = sample_data['sample_diff_midrow_salt']

    intrusion_temp_indices = list(np.where(column_avgs_temp > temp_intrusion_coeff)[0]+1)
    intrusion_salt_indices = list(np.where(column_avgs_salt > salt_intrusion_coeff)[0]+1)

    data_dates_name = 'sample_timestamps'
    all_timestamps = pd.DataFrame(sample_data[data_dates_name])

    temp_intrusion_dates = all_timestamps.iloc[intrusion_temp_indices]
    salt_intrusion_dates = all_timestamps.iloc[intrusion_salt_indices]

    estimated_intrusion_dates = [value for value in temp_intrusion_dates.values.tolist() 
                                 if value in salt_intrusion_dates.values.tolist()]
    estimated_intrusion_dates = [item for sublist in estimated_intrusion_dates for item in sublist]
    estimated_intrusion_dates = timestamp2datetime_lists(estimated_intrusion_dates)

    return estimated_intrusion_dates


def identify_intrusion_type(sample_data,intrusion_type):
    if intrusion_type == 0:
        intrusion_name = 'sample_intrusion_timestamps'
    elif intrusion_type == 1:
        intrusion_name = 'sample_mid_timestamps'
    elif intrusion_type == 2:
        intrusion_name = 'sample_inverse_timestamps'
    else:
        intrusion_name = 'sample_TBD_timestamps'
    
    return intrusion_name

def intrusion_ID_performance(lst: list[int],sample_data: dict[any], intrusion_name: str):
    estimated_intrusion_dates = intrusion_identification(lst,sample_data, intrusion_name)

    real_intrusion_dates = sample_data(intrusion_name)
    comparison_dates = intrusion_date_comparison(real_intrusion_dates, estimated_intrusion_dates,10)
        
    missed_id = comparison_dates['Only Manual']
    extra_id = comparison_dates['Only Estimated']
    caught_id = comparison_dates['Matched']

    if len(estimated_intrusion_dates) != 0:
        missed_id_parameter = len(missed_id)/len(real_intrusion_dates)
        extra_id_parameter = len(extra_id)/len(estimated_intrusion_dates)

        performance_parameter = (2*(missed_id_parameter) + extra_id_parameter)/3
    else:
        performance_parameter = 1

    return performance_parameter


def estimate_coefficients(sample_data: dict[any], range: list[int], intrusion_name: str) -> dict[list[any]]:
    real_intrusion_dates = sample_data[intrusion_name]
    
    temp_range = np.arange(range[0],range[1],0.02)
    salt_range = np.arange(range[0],range[1],0.02)
    result_final = []

    for temp_guess in temp_range:
        for salt_guess in salt_range:
            initial_guess = [temp_guess, salt_guess]
            result = minimize(intrusion_ID_performance, initial_guess, args=(sample_data,intrusion_name,))
            result_final.append((result.x, result.fun))

    best_coefficients = min(result_final, key= lambda x: x[1])

    temp_coeff = list(best_coefficients[0])[0]
    salt_coeff = list(best_coefficients[0])[1]

    result_comp = intrusion_date_comparison(real_intrusion_dates, 
                                            intrusion_identification(sample_data, [temp_coeff, salt_coeff],intrusion_type ))

    return {
        'Estimated Coefficient':best_coefficients,
        'Intrusions Missed': result_comp['Only Manual'],
        'Intrusions Extra': result_comp['Only Estimated'],
        'Intrusions IDed': result_comp['Matched'] ,
    }


def plot_year(file_name: str, ranges: list[int], yr:int) -> None:
    range_1 = ranges[0]
    range_2 = ranges[1]
    selected_data = import_joblib(file_name)

    yearly_profiles = separate_yearly_profiles(selected_data)
    
    plot_year_profiles(selected_data, yearly_profiles, 
                        yr,[range_1, range_2])
    
def get_original_indices(all_dates: list[datetime], int_dates: list[datetime]) -> list[int]:
    #all_dates = timestamp2datetime_lists(selected_data['sample_timestamps'])
    #int_dates = selected_data['sample_TBD_timestamps']

    comparison_results = intrusion_date_comparison(int_dates, all_dates,10)
    compared_dates = comparison_results['Matched']
    intrusion_dates = [match[2] for match in compared_dates]
    intrusion_indices = [i for i, dt1 in enumerate(all_dates) for j, dt2 in enumerate(intrusion_dates) if dt1 == dt2]

    return intrusion_indices


def get_intrusion_effects(selected_data, int_indices):
    bottom_avg = ['sample_diff_row_temp', 'sample_diff_row_salt']
    mid_avg = ['sample_diff_midrow_temp', 'sample_diff_midrow_salt']
    intrusion_temp_A = selected_data[bottom_avg[0]][int_indices]
    intrusion_salt_A = selected_data[bottom_avg[1]][int_indices]

    if intrusion_type == 1:
        intrusion_temp_A = selected_data[mid_avg[0]][int_indices]
        intrusion_salt_A = selected_data[mid_avg[1]][int_indices]
    
    return intrusion_temp_A, intrusion_salt_A


points = []
def onclick(event):
    if event.button == 1:
        if event.inaxes is not None:
            x, y = event.xdata, event.ydata

            points.append((x, y))

            print(f"Point selected: ({x}, {y})")
            event.inaxes.plot(x, y, 'ro')
            event.canvas.draw()

def onkey(event):
    if event.key == ' ':
        plt.close(event.canvas.figure)

def get_points():
    return points

def import_selected_data(file_name: str) -> dict:
    if int(file_name) == 1:
        file_name = 'BBMP_salected_data0.pkl'
    elif int(file_name) == 2:
        file_name = 'BBMP_salected_data.pkl'

    selected_data = import_joblib(file_name)
    yearly_profiles = separate_yearly_profiles(selected_data)

    return selected_data, yearly_profiles


def user_intrusion_selection(selected_data: dict[any], yearly_profiles: dict[any], 
                             year_list: list) -> list[datetime]:
    range_1 = [0,10]
    range_2 = [30.5,31.5]
    range_3 = [0,12]

    for yr in year_list:
        fig = plot_year_profiles(selected_data, yearly_profiles, 
                            yr,[range_1, range_2])

        cid_click = fig['Figure'].canvas.mpl_connect('button_press_event', onclick)

        cid_key = fig['Figure'].canvas.mpl_connect('key_press_event', onkey)

        plt.show()

    intrusion_dates = list(np.array(get_points())[:,0])
    intrusion_datetimes = [from_1970(dt) for dt in intrusion_dates]

    return intrusion_datetimes


def name_version(file_name: str, version: str, data: any) -> None:
    file_fname = file_name + '_' + version + '.pkl'
    save_joblib(file_fname, data)
    print(f'Saved as {file_fname}')


if __name__ == '__main__':
    #file_name = input("Enter the file name for intrusion identification (include .pkl):   ")
    file_name = 2
    lin = "-"*6+' '

    print(lin+'Importing Data')
    selected_data, yearly_profiles = import_selected_data(file_name)

    data_dates_name = 'sample_timestamps'
    data_timestamps = selected_data[data_dates_name]
    data_datetimes = [datetime.fromtimestamp(stamp) for stamp in data_timestamps]
    year_list = list(set([dt.year for dt in data_datetimes]))

    print(lin+'Intrusion identification in progress')
    intrusion_datetimes = user_intrusion_selection(selected_data, yearly_profiles, year_list)
    print(lin+'Intrusion identification completed')

    #version = input('Version name (Keep it simple):   ')
    version = 'test'
    name_version('manual_intrusions', version, intrusion_datetimes)

    # intrusion_type: int = int(input('What type? Enter the number inside the brakets. Normal[0] / Mid[1] / Inverse[2]/ TBD[else]:   '))
    intrusion_type = 3
    intrusion_name = identify_intrusion_type(selected_data,intrusion_type)
    selected_data[intrusion_name] = intrusion_datetimes

    int_indices = get_original_indices(data_datetimes, intrusion_datetimes)
    intrusion_dex = intrusion_name + '_INDICES'
    selected_data[intrusion_dex] = int_indices

    intrusion_temp_A, intrusion_salt_A = get_intrusion_effects(selected_data, int_indices)
    intrusion_changes = intrusion_name + '_EFFECTS'
    selected_data[intrusion_changes] = {'Temp Effects': intrusion_temp_A,
                                    'Salt Effects': intrusion_salt_A}

    ranges = [-1, 1]
    print(lin+'Estimating coefficients for optimized intrusion identification')
    results = estimate_coefficients(selected_data, ranges, intrusion_name)

    intrusion_opt_results = intrusion_name + '_SEPARATED'
    selected_data[intrusion_opt_results] = [results['Intrusions Missed'], 
                                        results['Intrusions Extra'],
                                        results['Intrusions IDed']]

    print(lin+'Recording results in data file')
    selected_data['temperature_coeff'] = list(results['Estimated Coefficient'][0])[0]
    selected_data['sallinity_coeff'] = list(results['Estimated Coefficient'][0])[1]
    selected_data['Performance'] = results['Estimated Coefficient'][1]

    #desc = input("Write description:   ")
    #selected_data['Comments'] = desc

    #file_fname = input("Enter the file name for output data file (include .pkl):   ")
    file_fname = 'BBMP_salected_data_test.pkl'
    save_joblib(file_fname, selected_data)
    print(f'Saved as {file_fname}')