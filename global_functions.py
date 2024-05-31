import joblib
from datetime import datetime
import numpy as np

joblib.dump(selected_data,'joblib_test.pkl')

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
        
    return {'Yearly Temp Profile': yearly_profiles_temp, 'Yearly Salt Profile': yearly_profiles_salt}
