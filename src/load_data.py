import global_functions as gf

file_name = input("Enter the file name of the data to be loaded (include .pkl):   ")
data_name = input("Enter the data file name (include .pkl):   ")
key_name = input("Enter the key name:   ")

intrusion_data = gf.import_joblib(file_name)
selected_data = gf.import_joblib(data_name)

selected_data[key_name] = intrusion_data

save_name = input("Enter the new file name of the data (include .pkl):   ")
gf.save_joblib(save_name, selected_data)
