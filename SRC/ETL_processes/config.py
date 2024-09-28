import numpy as np
import pandas as pd

from pandas import DataFrame
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Dict, Any, List
from logging import Logger

from .ETL_method import ETL_method
from .ETL_data_extraction import Extract_Type
from .ETL_data_loading import Load_Type
from .ETL_data_normalization import Normalize_Type
from .ETL_data_transformation import Transform_Type
from .ETL_timedepth import Matrices_Type
from misc.other.metadata_handling import DocInheritMeta
from Process_builder.process_step import Step