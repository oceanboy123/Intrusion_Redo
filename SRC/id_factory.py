from inspect import getmembers, isclass, isabstract
import Intrusion_identification

class id_factory(object):
    id_implementation = {}
    
    def __init__(self):
        self.load_id_methods()

    def load_id_methods(self):
        implementations = getmembers(Intrusion_identification, lambda m: isclass(m) and not isabstract(m))
        for name, _type in implementations:
            if isclass(_type) and issubclass(_type, Intrusion_identification.id_method):
                self.id_implementation[name] = _type

    def create(self, method: str, **kargs):
        if method in self.id_implementation:
            return self.id_implementation[method](**kargs)
        else:
            raise ValueError(f'{method.upper()} is not currently supported as a Intrusion Identification')