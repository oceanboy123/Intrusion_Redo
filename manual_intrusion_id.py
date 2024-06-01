import matplotlib.pyplot as plt
import global_functions as gf
import numpy as np

file_name = 'BBMP_salected_data.pkl'

selected_data = gf.import_joblib(file_name)

yearly_profiles = gf.separate_yearly_profiles(selected_data)

X,Y = np.meshgrid(selected_data['sample_timestamps'], selected_data['sample_depth'])





#fig, (ax1, ax2) = plt.subplots(2,1)
#plt.gcf().set_size_inches(6,30)
#im1 = ax1.imshow(yearly_profiles['Yearly Temp Profile'][2023], cmap = 'jet')
#im2 = ax2.imshow(yearly_profiles['Yearly Salt Profile'][2023], cmap = 'jet')
#fig.tight_layout()
#cbar1 = fig.colorbar(im1, ax=ax1)
#cbar2 = fig.colorbar(im2, ax=ax2)
#plt.show()