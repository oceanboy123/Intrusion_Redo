from abc import ABC, abstractmethod


class Process:
    def __init__(self):
        self.input: str= None
        self.start_at: int = None
        self.duration: int = None
        self.steps: str = None
        self.variables_used: str = None
        self.output: str = None
        self.id: int = None

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
        if self.variables_used:
            components.append(
                f"Variables Used: {self.variables_used}")
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
    def __init__(self):
        self.process = Process()

    @abstractmethod
    def get_cmdargs(self):
        pass

    @abstractmethod
    def create_request(self):
        pass

    @abstractmethod
    def main_process(self):
        pass

    @abstractmethod
    def record_process(self):
        pass

    def get_process(self):
        return self.process
  


class ComputerDirector:
    def __init__(self, builder: ProcessBuilder):
        self._builder = builder

    def construct_computer(self):
        self._builder.get_cmdargs()
        self._builder.create_request()
        self._builder.main_process()
        self._builder.record_process()

    def get_process(self):
        return self._builder.get_process()