import matplotlib.pyplot as plt
import numpy as np

file_name = 'BBMP_salected_data01.pkl'

selected_data = import_joblib(file_name)

yearly_profiles = separate_yearly_profiles(selected_data)

plt.imshow(yearly_profiles['Yearly Temp Profile'][2024], cmap = 'jet')
plt.colorbar()
plt.show()