from abc import ABC
import time

from typing import List, Dict, Callable, Union
from config import (
    create_logger,
    get_command_line_args, 
    RequestInfo, 
    Step
)

class Process:
    """
    This class initiates the process' attributes. All of which are used in the 
    ProcessBuilder class.

    ----- ATTRIBUTES
    input           : File name of the input data
    start_at        : Start date and time
    duration        : Time taken for execution
    steps           : Process's steps
    meta_steps      : Metadata recording steps
    variables_used  : Variable used for process
    output          : File name of the output data
    id              : Execution ID for metadata purposes
    process_data    : Step-by-step execution results

    _cmdargs        : Inputs from CMD line
    _request        : Process request characteristics
    _metadata       : Data lineage of process. Saved locally too.
    __defaultargs__ : Default arguments for CMD parsing
    """
    def __init__(self):
        self.input: str= None
        self.start_at: int = time.time()
        self.duration: int = None
        self.steps: str = None
        self.meta_step = None
        self.id: int = None
        self.process_data: List[Step] = None
        self._cmdargs: Dict = None
        self._request: RequestInfo = None
        self._metadata: Dict = None

    def generate_info(self) -> str:
        components = []
        if self.input:
            components.append(
                f"File Name: {self.input}")
        if self.start_at:
            components.append(
                f"Start time: {self.start_at}")
        if self.duration:
            components.append(
                f"Process Duration: {self.duration}")
        if self.steps:
            components.append(f"Process Steps: {self.steps}")
        if self.output:
            components.append(
                f"Ouput Name: {self.output}")
        if self.id:
            components.append(f"ID #: {self.id}")

        return components
    
    def __str__(self):
        return ',  '.join(self.generate_info())
    
    def __repr__(self) -> str:
        return '\n'.join(self.generate_info())
    


class ProcessBuilder(ABC):
    """
    Build interface for processes.
    NOTE: ** marks the attributes initiated within the Concrete Builder

    ----- ATTRIBUTES
    process          : Process' attributes
    logger           : Logging object
    cmd**            : Function for parsing CMD arguments
    request**        : Class that consolidates and retrieves data
    steps**          : Classes that defines the process' steps
    meta_steps**     : Classes that defines the loading and recording steps
    __defaultargs__**: Default arguments for CMD parsing

    ----- MEHOTDS
    get_cmdargs()    : Parses CMD line arguments
    create_request() : Prepares process request
    runmain()        : Runs the main body of the process
    record_process() : Saves files locally and updates metadata tables
    get_results()    : Returns Process class with attributes defined
    """
    def __init__(self) -> None:
        self.process = Process()
        self.logger = create_logger()
        self.cmd = get_command_line_args

    def get_cmdargs(self) -> None:
        self.process._cmdargs = self.cmd(self.__defaultargs__)

    def create_request(self) -> None:
        self.process._request = self.request(**self.process._cmdargs)
        self.process.input = self.process._request.file_name

    def runsteps(self, classes: List[Callable[...]]) -> List[Step]:
        objs = []
        for class_ in classes:
            temp_obj = class_(self.process._request)
            temp_obj.GenerateLog(self.logger)
            objs.append(temp_obj)
        
        return objs
    
    def runmain(self) -> None:
        self.process.steps = self.steps
        self.process.process_data = self.runsteps(self.steps)

    def record_process(self) -> None:
        self.process._metadata = self.runsteps(self.meta_steps)
        self.process.id = self.process._metadata.intrusion_id
        # TODO: Add this to Metadata Abstraction
        self.process.duration = time.time() - self._builder.process.start_at

    def get_results(self) -> None:
        return self.process
  


class ProcessDirector:
    """
    Directs the process that should be executed

    ----- ATTRIBUTES
    _builder : Build interface

    ----- MEHOTDS
    execute()     : Executes ProcessBuilder methods (except get_results)
    get_results() : Returns Process class with attributes defined
    """
    def __init__(self, builder: ProcessBuilder):
        self._builder = builder

    def execute(self):
        self._builder.get_cmdargs()
        self._builder.create_request()
        self._builder.runmain()
        self._builder.record_process()

    def get_results(self: Process):
        return self._builder.get_results()