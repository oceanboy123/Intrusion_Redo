######################################################
#         Author:  Edmundo Garcia
#   Date Created:  29/05/2024
#   Program Name:  Coefficient Estimator for
#                  Intrusion Identification

#    Description:  TBD
#

#        Version:  1.0
#  Date Modified:  29/05/2024
######################################################

import numpy as np
import pandas as pd
from scipy.optimize import minimize

def estimate_coefficients(sample_data):
    def intrusion_ID_performance(lst,sample_data):
        temp_intrusion_coeff, salt_intrusion_coeff = lst

        temp_intrusion_dates = pd.DataFrame(sample_data['sample_timestamps']).iloc[list(np.where(sample_data['sample_diff_row_temp'] > temp_intrusion_coeff)[0]+1)]
        salt_intrusion_dates = pd.DataFrame(sample_data['sample_timestamps']).iloc[list(np.where(sample_data['sample_diff_row_salt'] > salt_intrusion_coeff)[0]+1)]

        estimated_intrusion_dates = [value for value in temp_intrusion_dates.values.tolist() if value in salt_intrusion_dates.values.tolist()]
        estimated_intrusion_dates = [item for sublist in estimated_intrusion_dates for item in sublist]
        real_intrusion_dates = sample_data['sample_intrusion_timestamps']
        extra_id = [x for x in estimated_intrusion_dates if x not in real_intrusion_dates]
        missed_id = [x for x in real_intrusion_dates if x not in estimated_intrusion_dates]

        if len(estimated_intrusion_dates) != 0:
            missed_id_parameter = len(missed_id)/len(real_intrusion_dates)
            extra_id_parameter = len(extra_id)/len(estimated_intrusion_dates)

            performance_parameter = (missed_id_parameter + extra_id_parameter)/2
        else:
            performance_parameter = 1

        return performance_parameter

    temp_range = np.arange(0,1,0.01)
    salt_range = np.arange(0,1,0.01)
    result_final = []

    for temp_guess in temp_range:
        for salt_guess in salt_range:
            initial_guess = [temp_guess, salt_guess]
            result = minimize(intrusion_ID_performance, initial_guess, args=(sample_data,))
            result_final.append((result.x, result.fun))

    best_coefficients = min(result_final, key= lambda x: x[1])
    return best_coefficients
