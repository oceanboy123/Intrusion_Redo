import numpy as np
from dataclasses import dataclass, field
from typing import Dict, Any, List
from datetime import datetime, timedelta
from misc.other.logging import function_log
from misc.other.file_handling import *
from .id_method import id_method
from misc.request_arguments.request_info_analysis import RequestInfo_Analysis
from logging import Logger