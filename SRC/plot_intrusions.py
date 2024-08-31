from Intrusion_identification.manual_identification import manual_identification
from misc.request_arguments.request_info_analysis import RequestInfo_Analysis

file_name= 'BBMP_salected_data0.pkl'
intrusion_type= 'NORMAL'
id_type= 'MANUAL'
analysis_type= 'GET_COEFFICIENTS'
coefficient_temp= 0.5
coefficient_salt= 0.5
save_manual= 'OFF'
manual_input= 'manualID_NORMAL1724797813.pkl'


request = RequestInfo_Analysis(
                            file_name = file_name,
                            intrusion_type = intrusion_type, 
                            id_type = id_type,
                            analysis_type = analysis_type,
                            coefficient_temp = coefficient_temp,
                            coefficient_salt = coefficient_salt,
                            save_manual = save_manual,
                            manual_input = manual_input
                            )

XX = manual_identification('NORMAL','OFF')
XX.fill_request_info(request.dates)
yearly_profiles = XX.separate_yearly_profiles(request)
fig = XX.plot_year_profiles(yearly_profiles, 2021, request, yr2= 2024)
