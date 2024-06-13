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

bottom_avg = ['sample_diff_row_temp', 'sample_diff_row_salt']
mid_avg = ['sample_diff_midrow_temp', 'sample_diff_midrow_salt']
intrusion_temp_A = selected_data[bottom_avg[0]][int_indices]
intrusion_salt_A = selected_data[bottom_avg[1]][int_indices]

if intrusion_type == 1:
    intrusion_temp_A = selected_data[mid_avg[0]][int_indices]
    intrusion_salt_A = selected_data[mid_avg[1]][int_indices]

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