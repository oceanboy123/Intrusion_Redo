from Process_builder.builder_interface import ProcessDirector
from Process_builder.concrete_builder import IntrusionAnalysis

def main(): 

    # Build an Office Computer
    builder = IntrusionAnalysis(identification=1)
    director = ProcessDirector(builder)
    director.execute()
    Process = director.get_results()
    print(builder.name + " Process Executed Successfully:")
    print(Process)

if __name__ == "__main__":
    main()