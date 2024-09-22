from .builder_interface import ComputerBuilder

class OfficeComputerBuilder(ComputerBuilder):
    def build_cpu(self):
        self.computer.cpu = "Intel Core i5"

    def build_ram(self):
        self.computer.ram = "16GB DDR4"

    def build_storage(self):
        self.computer.storage = "512GB SSD"

    def build_graphics_card(self):
        self.computer.graphics_card = "Integrated Graphics"

    def install_operating_system(self):
        self.computer.operating_system = "Windows 11 Home"
