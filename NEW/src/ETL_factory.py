from inspect import getmembers, isclass, isabstract
import ETL_processes

class ETL_factory(object):
    ETL_implementation = {}
    
    def __init__(self):
        self.load_ETL_methods()

    def load_ETL_methods(self):
        implementations = getmembers(ETL_processes, lambda m: isclass(m) and not isabstract(m))
        for name, _type in implementations:
            if isclass(_type) and issubclass(_type, ETL_processes.ETL_method):
                self.ETL_implementation[name] = _type

    def create(self, method: str):
        if method in self.ETL_implementation:
            return self.ETL_implementation[method]()
        else:
            raise ValueError(f'{method.upper()} is not currently supported as a ETL process')