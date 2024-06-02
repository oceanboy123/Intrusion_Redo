import joblib
from datetime import datetime,timedelta
import numpy as np
import matplotlib.dates as mdates
import matplotlib.pyplot as plt
import pandas as pd
from scipy.optimize import minimize



def save_joblib(file_name, data):
    joblib.dump(data, file_name)
    


    
def import_joblib(file_name):
    data = joblib.load(file_name)
    
    return data




def separate_yearly_profiles(selected_data):
    sample_datetimes = [datetime.fromtimestamp(ts) for ts in selected_data['sample_timestamps']]
    years_extracted = np.unique([dt.year for dt in sample_datetimes])

    grouped_years = {year: [] for year in years_extracted}
    for i in sample_datetimes:
        grouped_years[i.year].append(i)

    by_year_indices = {year: [sample_datetimes.index(dt) for dt in grouped_years[year]] for year in years_extracted}

    selected_data_temp = selected_data['sample_matrix_temp'].to_numpy()
    selected_data_salt = selected_data['sample_matrix_salt'].to_numpy()

    yearly_profiles_temp = {}
    yearly_profiles_salt = {}

    for year, indices in by_year_indices.items():
        yearly_profile_temp = selected_data_temp[:, indices]
        yearly_profiles_temp[year] = yearly_profile_temp

        yearly_profile_salt = selected_data_salt[:, indices]
        yearly_profiles_salt[year] = yearly_profile_salt
        
    return {'Yearly Temp Profile': yearly_profiles_temp, 'Yearly Salt Profile': yearly_profiles_salt, 'Indices by Year':by_year_indices}




def plot_year_profiles(original_data, year_data, yr, ranges):
    year = yr
    timestamp = original_data['sample_timestamps'][year_data['Indices by Year'][year][0]:year_data['Indices by Year'][year][-1]]
    datetime_list = [datetime.fromtimestamp(stamp) for stamp in timestamp]

    fig, axs = plt.subplots(2)

    X,Y = np.meshgrid(datetime_list, original_data['sample_depth'])
    mesh0 = axs[0].pcolormesh(X,Y,year_data['Yearly Temp Profile'][year][:,:len(Y[0,:])], cmap='seismic')
    cbar0 = fig.colorbar(mesh0, ax=axs[0])
    axs[0].invert_yaxis()
    mesh0.set_clim(ranges[0])
    axs[0].set_xticks([])

    mesh1 = axs[1].pcolormesh(X,Y,year_data['Yearly Salt Profile'][year][:,:len(Y[0,:])], cmap='seismic')
    cbar1 = fig.colorbar(mesh1, ax=axs[1])
    axs[1].invert_yaxis()
    mesh1.set_clim(ranges[1])
    axs[1].xaxis.set_major_formatter(mdates.DateFormatter("%m"))

    fig.tight_layout()
    year_box = axs[0].text(0.02,0.85,str(year), transform=axs[0].transAxes,fontsize=14,verticalalignment='bottom',horizontalalignment='left',bbox=dict(facecolor='white',alpha=0.5))

    return {
        'Figure':fig,
        'Axes':axs,
        'Mesh':[mesh0,mesh1]
    }




def from_1970(date):
    reference_date = datetime(1970, 1, 1)

    delta = timedelta(days=date)
    datetime_obj = reference_date + delta

    return datetime_obj




def intrusion_date_comparison(manual_dates, estimated_dates):
    def within_days(date1, date2):
        return abs((dt2 - dt1).days) <= 10

    matching = []
    unmatched_md = []
    unmatched_ed = []

    for dt1 in manual_dates:
        found_match = False

        for dt2 in estimated_dates:
            if within_days(dt1, dt2):
                matching.append((dt1, dt2))
                found_match = True
                break

        if not found_match:
            unmatched_md.append(dt1)

    for dt2 in estimated_dates:
        found_match = False

        for dt1 in manual_dates:
            if within_days(dt1, dt2):
                found_match = True
                break
        
        if not found_match:
            unmatched_ed.append(dt2)



    return {
        'Matched':matching,
        'Only Manual':unmatched_md,
        'Only Estimated':unmatched_ed,
    }




def estimate_coefficients(sample_data):
    def intrusion_ID_performance(lst,sample_data):
        temp_intrusion_coeff, salt_intrusion_coeff = lst

        temp_intrusion_dates = pd.DataFrame(sample_data['sample_timestamps']).iloc[list(np.where(sample_data['sample_diff_row_temp'] > temp_intrusion_coeff)[0]+1)]
        salt_intrusion_dates = pd.DataFrame(sample_data['sample_timestamps']).iloc[list(np.where(sample_data['sample_diff_row_salt'] > salt_intrusion_coeff)[0]+1)]

        estimated_intrusion_dates = [value for value in temp_intrusion_dates.values.tolist() if value in salt_intrusion_dates.values.tolist()]
        estimated_intrusion_dates = [item for sublist in estimated_intrusion_dates for item in sublist]
        estimated_intrusion_dates = [datetime.fromtimestamp(dt) for dt in estimated_intrusion_dates]
        real_intrusion_dates = sample_data['sample_intrusion_timestamps']
        comparison_dates = intrusion_date_comparison(real_intrusion_dates, estimated_intrusion_dates)
        missed_id = comparison_dates['Only Manual']
        extra_id = comparison_dates['Only Estimated']


        if len(estimated_intrusion_dates) != 0:
            missed_id_parameter = len(missed_id)/len(real_intrusion_dates)
            extra_id_parameter = len(extra_id)/len(estimated_intrusion_dates)

            performance_parameter = (2*(missed_id_parameter) + extra_id_parameter)/3
        else:
            performance_parameter = 1

        return performance_parameter

    temp_range = np.arange(0,1,0.01)
    salt_range = np.arange(0,1,0.01)
    result_final = []

    for temp_guess in temp_range:
        for salt_guess in salt_range:
            initial_guess = [temp_guess, salt_guess]
            result = minimize(intrusion_ID_performance, initial_guess, args=(sample_data,))
            result_final.append((result.x, result.fun))

    best_coefficients = min(result_final, key= lambda x: x[1])
    return best_coefficients




def intrusion_identification(temp_values, salt_values, dates, coefficients):
    temp_coeff = coefficients[0]
    salt_coeff = coefficients[1]
    temperature_intrusion_dates = pd.DataFrame(dates).iloc[list(np.where(temp_values > temp_coeff)[0])]
    salinity_intrusion_dates = pd.DataFrame(dates).iloc[list(np.where(salt_values > salt_coeff)[0])]

    Both_intrusion_dates = [value for value in temperature_intrusion_dates.values.tolist() if value in salinity_intrusion_dates.values.tolist()]
    
    return Both_intrusion_dates