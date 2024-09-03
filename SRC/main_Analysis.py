from misc.request_arguments.request_info_analysis import RequestInfo_Analysis
from misc.request_arguments.get_cmdline_args import get_command_line_args
from Intrusion_analysis import intrusion_analysis, intrusion_data, meta
from Intrusion_identification import manual_identification, imported_identification
from misc.other.logging import create_logger

def main() -> None:
    path_data = './data/PROCESSED/'
    logger = create_logger()
    varsin = {
            'file_name': 'BBMP_salected_data0.pkl',
            'intrusion_type': 'NORMAL',
            'ID_type': 'MANUAL',
            'analysis_type': 'GET_COEFFICIENTS',
            'coefficient_temp': 0.5,
            'coefficient_salt': 0.5,
            'save_manual': 'OFF',
            'manual_input': 'manualID_NORMAL1724797813.pkl'
        }
        
    _ = (file_name, 
         intrusion_type, 
         id_type, analysis_type, 
         coefficient_temp, 
         coefficient_salt, 
         save_manual, 
         manual_input) = get_command_line_args(varsin)

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
    
    if id_type.upper() == 'MANUAL':
        intrusion_identification = manual_identification(intrusion_type, save_manual)
    else:
        intrusion_identification = imported_identification(intrusion_type, path_data +manual_input)

    intrusion_identification.run(request)
    logger.info(f'Intrusions Identified: {intrusion_identification.manualID_dates}')

    intrusion_effects = intrusion_data()
    intrusion_effects.run(request)

    analysis = intrusion_analysis(analysis_type, coefficients)
    analysis.run(request)

    logger.info(f'Coefficients Used [temp, salt]: {[analysis.OP_temp_coeff, analysis.OP_salt_coeff]} - Performance: {analysis.OP_performance} - # of missed intrusion: {len(analysis.OP_performance_spec['Only Manual'])} - # of extra intrusions: {len(analysis.OP_performance_spec['Only Estimated'])}')

    data_meta = meta()
    data_meta.run(request)


if __name__ == '__main__':
    main()