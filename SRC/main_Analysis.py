from misc.request_arguments.request_info_analysis import RequestInfo_Analysis
from misc.request_arguments.get_cmdline_args import get_command_line_args
from Intrusion_analysis import intrusion_analysis, intrusion_data, meta
from misc.other.logging import create_logger

def main() -> None:
    logger = create_logger()
    # varsin = {
    #         'file_name': 'BBMP_salected_data0.pkl',
    #         'intrusion_type': 'NORMAL',
    #         'ID_type': 'MANUAL',
    #         'analysis_type': 'GET_COEFFICIENTS',
    #         'coefficient_temp': 0.5,
    #         'coefficient_salt': 0.5,
    #         'save_manual': 'OFF',
    #         'manual_input': 'manualID_MID1720009644.pkl'
    #     }
        
    # file_name, intrusion_type, id_type, analysis_type, coefficient_temp, coefficient_salt, save_manual, manual_input = get_command_line_args(varsin)
    file_name= 'BBMP_salected_data0.pkl'
    intrusion_type= 'NORMAL'
    id_type= 'MANUAL'
    analysis_type= 'GET_COEFFICIENTS'
    coefficient_temp= 0.5
    coefficient_salt= 0.5
    save_manual= 'OFF'
    manual_input= 'manualID_MID1720009644.pkl'
    coefficients = [coefficient_temp, coefficient_salt]

    logger.info(f'File: {file_name} - Intrusion Type: {intrusion_type} - ID type: {id_type} - Analysis type: {analysis_type} - Coefficients: {coefficients} - Save Intrusions: {save_manual} - Manual Intrusion Input: {manual_input}')

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
    
    extraction = data_extraction(data_info= request)
    extraction.run()

    normalization = data_normalization(
                                    data_info= request,
                                    data_extraction= extraction
                                    )
    normalization.normalize_length_data()

    matrices = timedepth_space(
                            data_info= request, 
                            data_normalization= normalization
                            )
    matrices.get_variable_matrices()

    transformation = data_transformation(
                                    data_info= request, 
                                    data_normalization= normalization, 
                                    timedepth_space= matrices
                                    )
    transformation.data_transformations()

    load = data_loading(
                        data_info= request, 
                        data_normalization= normalization, 
                        data_transformation= transformation
                        )
    load.run()


if __name__ == '__main__':
    main()