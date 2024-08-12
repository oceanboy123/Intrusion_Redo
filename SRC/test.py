from misc import create_logger
import argparse

def test(varsin:dict, logger) -> list:
    parser = argparse.ArgumentParser(description='Arguments')

    # Command line arguments
    arguments = list(varsin.keys())
    for ar in arguments:
        parser.add_argument('--'+ ar, type= type(varsin[ar]), help='NaN')

    # Parse and read arguments and assign them to variables if exists
    args, _ = parser.parse_known_args()

    output = []
    for gs in arguments:
        output.append(varsin[gs])
        
        attribute_value = getattr(args, gs, None)
        if attribute_value != None and isinstance(attribute_value, type(varsin[gs])):

            if isinstance(attribute_value, list):
                output[-1] = [int(attribute_value[0]), int(attribute_value[-1])]
            else:
                output[-1] = attribute_value

    return tuple(output)

def main() -> None:
    logger = create_logger('test_log', 'test.log')

    logger.info("This is an info message")


    varsin = {
            'file_name': 'bbmp_aggregated_profiles.csv',
            'deep_depth': 60,
            'mid_depths_top': 20,
            'mid_depths_bottom': 35,
            'date_format': '%Y-%m-%d %H:%M:%S',
        }
    
    tu, tus, m1, m2, to = test(varsin, logger) 
    coe = [m1, m2]
    for c in coe:
        print(c)

if __name__ == '__main__':
    main()