import logging
import argparse

def create_logger(name: str, log_file : str=None, level=logging.DEBUG):
    """
    Create and return a logger object.

    :param name: Name of the logger.
    :param log_file: File path to log output (optional).
    :param level: Logging level (e.g., logging.DEBUG).
    :return: Configured logger object.
    """
    # Create a logger
    logger = logging.getLogger(name)
    logger.setLevel(level)

    # Create console handler
    console_handler = logging.StreamHandler()
    console_handler.setLevel(level)

    # Create a formatter and set it for the handler
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    console_handler.setFormatter(formatter)

    # Add the console handler to the logger
    logger.addHandler(console_handler)

    # If a log file is specified, create a file handler
    if log_file:
        file_handler = logging.FileHandler(log_file)
        file_handler.setLevel(level)
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

    return logger


def get_command_line_args(varsin:dict[str, str | int]) -> list:
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
        if attribute_value is not None:

            output[-1] = attribute_value

    return tuple(output)