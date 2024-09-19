from misc.request_arguments.request_info_analysis import RequestInfo_Analysis
from Intrusion_analysis import intrusion_analysis, intrusion_data, meta
from Intrusion_identification import (manual_identification, 
                                      imported_identification)
from config import create_logger, get_command_line_args

def main() -> None:
    """
    ETL Process:    -> Get CMD line arguments
                    -> Create RequestInfo
                    -> Intrusion Identification
                    -> Retrieve Intrusion effects 
                    -> Apply Intrusion Analysis
                    -> Generate Metadata
    """
    logger = create_logger()

    path_data = './data/PROCESSED/'
    varsin = {
            'file_name': 'BBMP_selected_data0.pkl',
            'intrusion_type': 'NORMAL',
            'ID_type': 'MANUAL',
            'analysis_type': 'GET_COEFFICIENTS',
            'coefficient_temp': 0.5,
            'coefficient_salt': 0.5,
            'save_manual': 'OFF',
            'manual_input': 'manualID_NORMAL1724797813.pkl'
        }
    
    # -----------> Get CMD line arguments
    _ = (file_name, 
         intrusion_type, 
         id_type, analysis_type, 
         coefficient_temp, 
         coefficient_salt, 
         save_manual, 
         manual_input) = get_command_line_args(varsin)

    coefficients = [coefficient_temp, coefficient_salt]

    logger.info(f'File: {file_name} - Intrusion Type: {intrusion_type} - '+
                f'ID type: {id_type} - Analysis type: {analysis_type} - '+
                f'Coefficients: {coefficients} - '+
                f'Save Intrusions: {save_manual} - '+
                f'Manual Intrusion Input: {manual_input}')

    # -----------> Create RequestInfo
    # RequestInfo_Analysis(...) -> 
    #           .metadata : Analysis Table
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
    
    # -----------> Intrusion Identification
    # RequestInfo_Analysis(...).id_method(...)->
    #           .manualID_dates : Dates identified
    #           .table_IDeffects : Manual Effects Table
    #           .intrusions : Analysis Request Metadata
    if id_type.upper() == 'MANUAL':
        manual_identification(intrusion_type, save_manual).run(request)
    else:
        imported_identification(intrusion_type, path_data + manual_input
                                ).run(request)
    logger.info(
        f'Intrusions Identified: {request.identification.manualID_dates}')

    # -----------> Retrieve Intrusion effects 
    intrusion_data().run(request)

    # -----------> Apply Intrusion Analysis
    # RequestInfo_Analysis(...).analysis_step(...)->
    #           .table_coefficients: Coefficient Table
    #           .table_IDeffects : Estimated Effects Table
    #           .estimatedID_dates : Dates estimated
    intrusion_analysis(analysis_type, coefficients).run(request)

    analysis_ = request.analysis
    logger.info(
        f'Coefficients Used [temp, salt]: '+
            f'{[analysis_.OP_temp_coeff, analysis_.OP_salt_coeff]} - '+
        f'Performance: {analysis_.OP_performance} - '+
        f'# of missed intrusion: '+
            f'{len(analysis_.OP_performance_spec['Only Manual'])} - '+
        f'# of extra intrusions: '+
            f'{len(analysis_.OP_performance_spec['Only Estimated'])}'
            )
    
    # -----------> Generate Metadata
    # meta.table_coefficients_error_comb : More specific coefficient results
    meta().run(request)


if __name__ == '__main__':
    main()