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


def proper_logging(funq: Callable[..., Any], args : list[Any]) -> None:
    """
    Logging function to log function type, name, input arguments, and expected 
    return type.
    """
    # Determine if it's a function or a method
    if inspect.isfunction(funq):
        type = 'Function'
    elif inspect.ismethod(funq):
        type = 'Instance Method'
    elif inspect.isclass(funq):
        type = 'Class'
    else:
        type = 'Unknown'
        
    funq_name = funq.__name__

    # Get the signature for input types and return type
    signature = inspect.signature(funq)
    params = signature.parameters
    return_type = (signature.return_annotation 
                   if signature.return_annotation != inspect.Signature.empty 
                   else 'Unknown')

    # Log function type, name, and input arguments
    input_info = ', '.join(f'{name}={value}' 
                           for name, value in zip(params, args))

    print('')
    logger.debug(
        f'Type: {type} - Name: {funq_name} - Inputs: {input_info}' + 
        f' - Expected Outputs: {return_type}')


def function_log(funq: Callable[..., Any]):
    """
    Decorator function for logging purposes, logs input arguments and function 
    details.
    """
    @wraps(funq)
    def wrapper(*args, **kwargs):
        proper_logging(funq, list(args) + list(kwargs.values()))
        result = funq(*args, **kwargs)
        return result
    return wrapper