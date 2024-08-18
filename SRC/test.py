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
import types
from dataclasses import dataclass, field
from typing import List, Dict, Any
from misc import *

# logger = create_logger()

def empty() -> None:
    ...

def proper_logging(funq: object, inp : List[Any]) -> None:
    if isinstance(funq, types.MethodType):
        class_name = funq.__self__.__class__.__name__
        funq_name = funq.__name__
    else:
        class_name = 'NaN'
        funq_name = funq.__name__

    type_hints = get_type_hints(funq)

    if 'return' in type_hints:
        return_type = type_hints['return']
    else:
        return_type = 'Unknown'

    logger.debug(f'Class: {class_name} - Function: {funq_name} - Inputs: {inp} - Expected Outputs: {return_type}')


def function_log(funq):
    @wraps(funq)
    def wrapper(*args):
        inp = list(args)
        proper_logging(funq, inp)
        result = funq(*args)
        return result
    return wrapper

@function_log
@dataclass
class dataset:
    path : str

    data : Dict[str, Any] = field(default_factory=dict)
    metadata_intrusions : Dict[str, Any] = field(default_factory=dict)
    identification : object = field(default_factory=empty)
    analysis : object = field(default_factory=empty)
    dates_stamp: List[int] = field(default_factory=list)
    dates: List[datetime] = field(default_factory=list)

    dates_name = 'sample_timestamps'
    
    def __post_init__(self) -> None:
        self.data = import_joblib(self.path)
        self.dates_stamp = self.data[self.dates_name]
        self.dates = timestamp2datetime_lists(self.dates_stamp)

    @function_log
    def lol():
        return 5+3

@function_log
def easy(e,r) ->int:
    return e+2**r

def main():
    file_name= 'BBMP_salected_data0.pkl'
    path_data = '../data/PROCESSED/'
    file_dirpath = path_data + file_name

    bbmp = dataset(file_dirpath)
    dd = dataset.lol()
    df = easy(2,5)

    print(f'{bbmp},{df}')

if __name__ == '__main__':
    bbmp = main()
