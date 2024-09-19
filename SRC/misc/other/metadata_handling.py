from abc import ABCMeta

class DocInheritMeta(ABCMeta):
    def __new__(mcs, name, bases, attrs):
        # Create the new class using abc.ABCMeta
        new_cls = super().__new__(mcs, name, bases, attrs)

        # Combine class docstrings
        docstrings = []
        # Collect docstrings from base classes in MRO
        for base in reversed(new_cls.__mro__[1:-1]):  # Exclude 'object' and the new class itself
            if base.__doc__:
                docstrings.append(f"{base.__name__}:\n{base.__doc__}")
        # Include the child class's own docstring if it exists
        if new_cls.__doc__:
            docstrings.append(f"{new_cls.__name__}:\n{new_cls.__doc__}")

        # Combine all docstrings into one
        combined_docstring = '\n\n'.join(docstrings)
        new_cls.__doc__ = combined_docstring

        return new_cls
