import numpy as np
import pandas as pd

from pandas import DataFrame
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Dict, Any, List
from logging import Logger

from .ETL_method import ETL_method
from misc.other.metadata_handling import DocInheritMeta