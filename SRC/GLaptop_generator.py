from Process_builder.builder_interface import ComputerDirector
from Process_builder.concrete_builder import OfficeComputerBuilder

def main():

    # Build an Office Computer
    office_builder = OfficeComputerBuilder()
    director = ComputerDirector(office_builder)
    director.construct_computer()
    office_computer = director.get_computer()
    print("Office Computer Configured:")
    print(office_computer)

if __name__ == "__main__":
    main()