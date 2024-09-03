import matplotlib.pyplot as plt

from Intrusion_identification.manual_identification import manual_identification
from misc.request_arguments.request_info_analysis import RequestInfo_Analysis
from config import get_command_line_args

def main() -> None:
    """
    ETL Process:    -> Set up
                    -> Schema Formating
                    -> Plotting template
    """
    # -> Set up
    varsin = {
                'file_name': 'BBMP_salected_data0.pkl',
                'initial_yr': 2018,
                'final_yr': 0,
                'datetimes': [],
                }
        
    raw_name, i_year, f_year, dtm_ = get_command_line_args(varsin)
    file_name= raw_name
    intrusion_type= 'NORMAL'
    id_type= 'MANUAL'
    analysis_type= 'GET_COEFFICIENTS'
    coefficient_temp= 0.5
    coefficient_salt= 0.5
    save_manual= 'OFF'
    manual_input= 'manualID_NORMAL1724797813.pkl'

    request = RequestInfo_Analysis(
                                file_name = file_name
    )

    XX = manual_identification('NORMAL','OFF')
    XX.fill_request_info(request.dates)

    # -> Schema Formating
    yearly_profiles = XX.separate_yearly_profiles(request)
    formated_data = XX.format_data_plot(yearly_profiles, i_year, request, yr2= f_year, dtm=dtm_)

    # -> Plotting template
    XX.plot_year_profiles(formated_data)
    plt.show()


if __name__ == '__main__':
    main()