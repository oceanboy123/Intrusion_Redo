import logging
import inspect
from logging import Logger
from typing import get_type_hints, Callable, Any
from functools import wraps

logger = logging.getLogger(__name__)


def create_logger(log_file=None, level=logging.DEBUG) -> Logger:
    """
    Create and return a personalized logger object.
    """
    # Create a logger
    logger: Logger = logging.getLogger(__name__)
    logger.setLevel(level)

    # Create console handler
    console_handler = logging.StreamHandler()
    console_handler.setLevel(level)

    # Create a formatter and set it for the handler
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s')
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


def proper_logging(funq: Callable[..., Any], inp : list[vars]) -> None:
    """
    Logging function 
    """
    type = 'Class'

    if inspect.isfunction(funq):
        type = 'Function'

    funq_name = funq.__name__

    type_hints = get_type_hints(funq)

    if 'return' in type_hints:
        return_type = type_hints['return']
    else:
        return_type = 'Unknown'

    print('')
    logger.debug(
        f'Type: {type} - Name: {funq_name} - Inputs: {inp}' + 
        f' - Expected Outputs: {return_type}')


def function_log(funq: Callable[..., Any]):
    """
    Decorator function for logging purposes
    """
    @wraps(funq)
    def wrapper(*args):
        inp = list(args)
        proper_logging(funq, inp)
        result = funq(*args)
        return result
    return wrapper