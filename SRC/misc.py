import os
import sys
import logging
import argparse
import joblib
from datetime import datetime, timedelta
import numpy as np
import matplotlib.pyplot as plt
from functools import wraps
from typing import get_type_hints
import inspect

logger = logging.getLogger(__name__)

def create_logger(log_file=None, level=logging.DEBUG):
    """
    Create and return a logger object.
    """
    # Create a logger
    logger = logging.getLogger(__name__)
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


def proper_logging(funq: object, inp : list[vars]) -> None:
    """
    Logging function used within a wrapper function for a decorator function
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
    logger.debug(f'Type: {type} - Name: {funq_name} - Inputs: {inp} - Expected Outputs: {return_type}')


def function_log(funq):
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


def get_command_line_args(varsin:dict[str, str | int]) -> list:
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
        if attribute_value is not None:

            output[-1] = attribute_value

    return tuple(output)


def save_joblib(data:any,file_name: str) -> any:
    file_path = '../data/PROCESSED/' + file_name
    joblib.dump(data, file_path)
    

def import_joblib(file_path: str) -> any:
    data: any = joblib.load(file_path)
    return data


def timestamp2datetime_lists(lst:list[int]) -> list[datetime]:
    """
    Takes a list of timestapms and converst it to a list of datetimes
    """
    datetime_list:list[datetime] = [datetime.fromtimestamp(ts) for ts in lst]
    return datetime_list


def separate_yearly_dates(datetime_list:list[datetime]) -> dict[list]:
    """
    Separates a list of datetimes by year by separating indices
    """
    years_extracted:list = np.unique([dt.year for dt in datetime_list])

    grouped_years:dict[list] = {year: [] for year in years_extracted}
    for i in datetime_list:
        grouped_years[i.year].append(i)

    return grouped_years


points = []   # Creates list to save the points selected
def onclick(event):
    """
    Allows you to select point from plots
    """

    if event.button == 1:
        if event.inaxes is not None:
            x, y = event.xdata, event.ydata

            points.append((x, y))


def onkey(event):
    """
    Terminates point selection stage
    """

    if event.key == ' ':
        plt.close(event.canvas.figure)


def get_points():
    return points

def date_comparison(dates1:list[datetime], dates2:list[datetime], dates_error=10) -> dict[list]:
    """
    Compares datetime lists for similar (within self.dates_error) dates
    """
    
    def within_days(dtes1:datetime, dtes2:datetime) ->int:
        calc = abs((dtes2 - dtes1).days)
        return calc

    matching = []
    unmatched_md = []

    for dt1 in dates1:
        found_match = False
        single_match = []

        for dt2 in dates2:
            diff = within_days(dt1, dt2)
            if diff <= dates_error:
                single_match.append([diff, dt1, dt2])
                found_match = True
                break
        # Intrusions Not Found
        if not found_match:
            unmatched_md.append(dt1)
        else:
            # Intrusions Found
            if len(single_match) > 1:
                diff_list = [match[0] for match in single_match]
                min_index = [idx for idx, value in enumerate(diff_list) if value == min(diff_list)]
                matching.append([single_match[min_index]])
            else:
                matching.append(single_match) 
    
    matching = [item for sublist in matching for item in sublist]
    matching_estimated = [sublist[2] for sublist in matching]

    set1 = set(dates2)
    set2 = set(matching_estimated)
    # Extra Intrusions
    unmatched_ed = list(set1-set2)

    return {
        'Matched':matching,
        'Only Manual':unmatched_md,
        'Only Estimated':unmatched_ed,
    }