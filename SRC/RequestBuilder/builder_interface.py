from abc import ABC, abstractmethod

class Computer:
    def __init__(self):
        self.cpu = None
        self.ram = None
        self.storage = None
        self.graphics_card = None
        self.operating_system = None

    def __str__(self):
        components = []
        if self.cpu:
            components.append(f"CPU: {self.cpu}")
        if self.ram:
            components.append(f"RAM: {self.ram}")
        if self.storage:
            components.append(f"Storage: {self.storage}")
        if self.graphics_card:
            components.append(f"Graphics Card: {self.graphics_card}")
        if self.operating_system:
            components.append(f"OS: {self.operating_system}")
        return ', '.join(components)
    


class ComputerBuilder(ABC):
    def __init__(self):
        self.computer = Computer()

    @abstractmethod
    def build_cpu(self):
        pass

    @abstractmethod
    def build_ram(self):
        pass

    @abstractmethod
    def build_storage(self):
        pass

    @abstractmethod
    def build_graphics_card(self):
        pass

    @abstractmethod
    def install_operating_system(self):
        pass

    def get_computer(self):
        return self.computer
  


class ComputerDirector:
    def __init__(self, builder):
        self._builder = builder

    def construct_computer(self):
        self._builder.build_cpu()
        self._builder.build_ram()
        self._builder.build_storage()
        self._builder.build_graphics_card()
        self._builder.install_operating_system()

    def get_computer(self):
        return self._builder.get_computer()