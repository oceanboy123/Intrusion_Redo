import argparse

def get_command_line_args(varsin:dict[str, str | int]) -> tuple:
    """
    Get arguments from command-line
    """

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
        if attribute_value != None :

            output[-1] = attribute_value

    return tuple(output)