from Factory_method.ETL_factory import ETL_factory
from misc.request_arguments.request_info_ETL import RequestInfo_ETL
from misc.request_arguments.get_cmdline_args import get_command_line_args


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

    method_list = [
        'data_extraction',
        'data_normalization',
        'timedepth_space',
        'data_transformation',
        'data_loading',
    ]

    varins = {
        'data_info' : request
    }

    factory = ETL_factory()

    count = 0
    for method in method_list:
        count += 1
        method_product = factory.create(method, **varins)
        method_product.run()
        if count > 1 and count != 3:
            varins.popitem()

        varins[method] = method_product


if __name__ == '__main__':
    main()