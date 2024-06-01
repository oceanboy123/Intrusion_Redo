import joblib
from datetime import datetime,timedelta
import numpy as np
import matplotlib.dates as mdates
import matplotlib.pyplot as plt

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