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

def intrusion_date_comparison(manual_dates, estimated_dates):
    def within_days(date1, date2):
        return abs((dt2 - dt1).days) <= 10

    matching = []
    unmatched_md = []
    unmatched_ed = []

    for dt1 in manual_dates:
        found_match = False

        for dt2 in estimated_dates:
            if within_days(dt1, dt2):
                matching.append((dt1, dt2))
                found_match = True
                break

        if not found_match:
            unmatched_md.append(dt1)

    for dt2 in estimated_dates:
        found_match = False

        for dt1 in manual_dates:
            if within_days(dt1, dt2):
                found_match = True
                break
        
        if not found_match:
            unmatched_ed.append(dt2)



    return {
        'Matched':matching,
        'Only Manual':unmatched_md,
        'Only Estimated':unmatched_ed,
    }



def estimate_coefficients(sample_data):
    def intrusion_ID_performance(lst,sample_data):
        temp_intrusion_coeff, salt_intrusion_coeff = lst

        temp_intrusion_dates = pd.DataFrame(sample_data['sample_timestamps']).iloc[list(np.where(sample_data['sample_diff_row_temp'] > temp_intrusion_coeff)[0]+1)]
        salt_intrusion_dates = pd.DataFrame(sample_data['sample_timestamps']).iloc[list(np.where(sample_data['sample_diff_row_salt'] > salt_intrusion_coeff)[0]+1)]

        estimated_intrusion_dates = [value for value in temp_intrusion_dates.values.tolist() if value in salt_intrusion_dates.values.tolist()]
        estimated_intrusion_dates = [item for sublist in estimated_intrusion_dates for item in sublist]
        estimated_intrusion_dates = [datetime.fromtimestamp(dt) for dt in estimated_intrusion_dates]
        real_intrusion_dates = sample_data['sample_intrusion_timestamps']
        comparison_dates = intrusion_date_comparison(real_intrusion_dates, estimated_intrusion_dates)
        missed_id = comparison_dates['Only Manual']
        extra_id = comparison_dates['Only Estimated']


        if len(estimated_intrusion_dates) != 0:
            missed_id_parameter = len(missed_id)/len(real_intrusion_dates)
            extra_id_parameter = len(extra_id)/len(estimated_intrusion_dates)

            performance_parameter = (2*(missed_id_parameter) + extra_id_parameter)/3
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


    
