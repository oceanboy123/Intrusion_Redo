import matplotlib.pyplot as plt
import global_functions as gf

file_name = 'BBMP_salected_data.pkl'

selected_data = gf.import_joblib(file_name)

yearly_profiles = gf.separate_yearly_profiles(selected_data)

plt.imshow(yearly_profiles['Yearly Temp Profile'][2024], cmap = 'jet')
plt.colorbar()
plt.show()