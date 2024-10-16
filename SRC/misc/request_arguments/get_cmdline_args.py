import argparse
from typing import Dict, Union, Tuple

def get_command_line_args(varsin: Dict[str, Union[str, int]]) -> Tuple:
    """
    Gets arguments from command-line. Uses varsin to create the defaults for
    each input argument.

    ----------------Inputs
    varsin :   Default values for each argument
    """
    parser = argparse.ArgumentParser(description=
                                     'Command-line arguments parser')

    # Command line arguments
    for arg, default_value in varsin.items():
        # Determine type based on default value
        arg_type = type(default_value)
        parser.add_argument('--' + arg, type=arg_type, default=default_value,
                            help=f'Default: {default_value}')

    # Parse arguments
    args = parser.parse_args()
    output = []

    for arg in varsin.keys():
        value = getattr(args, arg)
        output.append(value)
            
    return tuple(output)