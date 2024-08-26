# import sys
# sys.path.insert(0, r'D:\Projects\Intrusion_Redo\NEW\src\misc')


from misc.request_arguments.request_info_ETL import RequestInfo_ETL
from misc.request_arguments.get_cmdline_args import get_command_line_args
from ETL_processes import data_extraction, data_normalization, timedepth_space, data_transformation, data_loading

def main() -> None:
    varsin = {
            'file_name': 'bbmp_aggregated_profiles.csv',
            'deep_depth': 60,
            'mid_depths_top': 20,
            'mid_depths_bottom': 35,
            'date_format': '%Y-%m-%d %H:%M:%S',
            }
    
    raw_name, deep_depth, mid_depth1, mid_depth2, date_format = get_command_line_args(varsin)

    request = RequestInfo_ETL(
                            file_name= raw_name, 
                            deep_depth= deep_depth,
                            mid_depth1= mid_depth1,
                            mid_depth2= mid_depth2,
                            date_format= date_format
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