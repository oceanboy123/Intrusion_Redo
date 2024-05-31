import matplotlib.pyplot as plt
import numpy as np

file_name = 'BBMP_salected_data.pkl'

selected_data = import_pkl(file_name)

yearly_profiles = separate_yearly_profiles(selected_data)

#plt.imshow(selected_data['sample_matrix_temp'].to_numpy(), cmap = 'jet')
#plt.colorbar()
#plt.show()