from analysis_factory import analysis_factory
from id_factory import id_factory
from misc.request_arguments.request_info_analysis import RequestInfo_Analysis
from misc.other.logging import *
from misc.request_arguments.get_cmdline_args import get_command_line_args


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
    id_type= 'IMPORTED'
    analysis_type= 'USE_COEFFICIENTS'
    coefficient_temp= 0.5
    coefficient_salt= 0.5
    save_manual= 'OFF'
    manual_input= 'manualID_NORMAL1724797813.pkl'
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
                            manual_input = manual_input,
                            )

    factory_id = id_factory()
    varss = {
            'intrusion_type': request.intrusion_type,
            }
    
    if id_type.upper() == 'MANUAL':
        varss['save_manual'] = save_manual
        intrusion_identification = factory_id.create('manual_identification', **varss)
    else:
        varss['manual_input'] = manual_input
        intrusion_identification = factory_id.create('imported_identification', **varss)

    intrusion_identification.run(request)
    logger.info(f'Intrusions Identified: {intrusion_identification.manualID_dates}')

    factory_analysis = analysis_factory()
    
    step_list = [
        'intrusion_data',
        'intrusion_analysis',
        'meta',
    ]

    count = 0
    for step in step_list:
        count += 1
        varins = {}
        if count == 2:
            varins = {
                'analysis_type': analysis_type,
                'coefficients': coefficients                
            }

        method_product = factory_analysis.create(step, **varins)
        method_product.run(request)

        # if count == 2:
        #     logger.info(f'Coefficients Used [temp, salt]: {[method_product.OP_temp_coeff, method_product.OP_salt_coeff]} - Performance: {method_product.OP_performance} - # of missed intrusion: {len(method_product.OP_performance_spec['Only Manual'])} - # of extra intrusions: {len(method_product.OP_performance_spec['Only Estimated'])}')


if __name__ == '__main__':
    main()