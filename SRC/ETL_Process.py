from Process_builder.builder_interface import ProcessDirector
from Process_builder.concrete_builder import DataETL

def main():

    # Build an Office Computer
    builder = DataETL()
    director = ProcessDirector(builder)
    director.execute()
    Process = director.get_results()
    print("Process Executed Successfully:")
    print(Process)

if __name__ == "__main__":
    main()