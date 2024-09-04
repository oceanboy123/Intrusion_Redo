from misc.request_arguments.request_info_ETL import RequestInfo_ETL
from ETL_processes import (data_extraction, data_normalization, timedepth_space, 
                           data_transformation, data_loading)
from config import create_logger, get_command_line_args

def main() -> None:
    """
    ETL Process:    -> Get CMD line arguments
                    -> Create RequestInfo
                    -> Extraction
                    -> Normalization and Modeling 
                    -> Construct Time Depth Space
                    -> Interpolation and Depth-Averages
                    -> Conform to Schema and Record Metadata
    """
    logger = create_logger()
    
    varsin = {
            'file_name': 'bbmp_aggregated_profiles.csv',
            'deep_depth': 60,
            'mid_depths_top': 20,
            'mid_depths_bottom': 35,
            'date_format': '%Y-%m-%d %H:%M:%S',
            }
    
    # -> Get CMD line arguments
    _ = (raw_name, 
        deep_depth, 
        mid_depth1, 
        mid_depth2, 
        date_format) = get_command_line_args(varsin)

    # -> Create RequestInfo
    request = RequestInfo_ETL(
                            file_name= raw_name, 
                            deep_depth= deep_depth,
                            mid_depth1= mid_depth1,
                            mid_depth2= mid_depth2,
                            date_format= date_format
                            )
    
    # -> Extraction
    extraction = data_extraction(data_info= request)
    extraction.GenerateLog(logger)

    # -> Normalization and Modeling 
    normalization = data_normalization(
                                    data_info= request,
                                    data_extraction= extraction
                                    )
    normalization.GenerateLog(logger)

    # -> Construct Time Depth Space
    matrices = timedepth_space(
                            data_info= request, 
                            data_normalization= normalization
                            )
    matrices.GenerateLog(logger)

    # -> Interpolation and Depth-Averages
    transformation = data_transformation(
                                    data_info= request, 
                                    data_normalization= normalization, 
                                    timedepth_space= matrices
                                    )
    transformation.GenerateLog(logger)

    # -> Conform to Schema and Record Metadata
    load = data_loading(
                        data_info= request, 
                        data_normalization= normalization, 
                        data_transformation= transformation
                        )
    load.GenerateLog(logger)


if __name__ == '__main__':
    main()