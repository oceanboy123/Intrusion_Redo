import pandas as pd
import numpy as np

def intrusion_identification(temp_values, salt_values, dates, coefficients):
    temp_coeff = coefficients[0]
    salt_coeff = coefficients[1]
    temperature_intrusion_dates = pd.DataFrame(dates).iloc[list(np.where(temp_values > temp_coeff)[0])]
    salinity_intrusion_dates = pd.DataFrame(dates).iloc[list(np.where(salt_values > salt_coeff)[0])]

    Both_intrusion_dates = [value for value in temperature_intrusion_dates.values.tolist() if value in salinity_intrusion_dates.values.tolist()]
    
    return Both_intrusion_dates