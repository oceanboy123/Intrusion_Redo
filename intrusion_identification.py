import pandas as pd
import numpy as np

def intrusion_identification(temp_values, salt_values, dates, coefficients):
    temp_coeff = coefficients[0]
    salt_coeff = coefficients[1]
    temperature_intrusion_dates = pd.DataFrame(dates).iloc[list(np.where(temp_values > temp_coeff)[0])]
    salinity_intrusion_dates = pd.DataFrame(dates).iloc[list(np.where(salt_values > salt_coeff)[0])]

    Both_intrusion_dates = [value for value in temperature_intrusion_dates.values.tolist() if value in salinity_intrusion_dates.values.tolist()]
    
    return Both_intrusion_dates


#plt.imshow(temperature_m_inter_12.to_numpy(), cmap = 'jet')
#plt.colorbar()
#plt.show()

#    Found out there are timestamps that are different and therefore
#    have multiple measuerements for the same day. Making some profiles
#    have a lot of repited depths. 
#
#sizess = []
#for key, values in normalized_data.items():
#    sizess.append(len(pd.DataFrame(values)))
#
#not_good_indices = [index for index, value in enumerate(sizess) if value != 276]
#wrong_data_length = pd.DataFrame(normalized_data.keys()).iloc[not_good_indices]