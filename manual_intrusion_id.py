import global_functions as gf

file_name = 'BBMP_salected_data.pkl'

selected_data = gf.import_joblib(file_name)

yearly_profiles = gf.separate_yearly_profiles(selected_data)

range_1 = [0,10]
range_2 = [30.5,31.5]
gf.plot_year_profiles(selected_data, yearly_profiles, 
                      2023,[range_1, range_2])

