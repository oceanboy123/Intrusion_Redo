from inspect import getmembers, isclass, isabstract
import Intrusion_analysis

class analysis_factory(object):
    analysis_implementation = {}
    
    def __init__(self):
        self.load_analysis_methods()

    def load_analysis_methods(self):
        implementations = getmembers(Intrusion_analysis, lambda m: isclass(m) and not isabstract(m))
        for name, _type in implementations:
            if isclass(_type) and issubclass(_type, Intrusion_analysis.analysis_step):
                self.analysis_implementation[name] = _type

    def create(self, method: str, **kargs):
        if method in self.analysis_implementation:
            return self.analysis_implementation[method](**kargs)
        else:
            raise ValueError(f'{method.upper()} is not currently supported as an Intrusion Analysis')