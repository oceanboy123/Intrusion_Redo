import joblib


def save_joblib(data:any,file_name: str) -> any:
    """
    Preferred src save function
    """
    file_path = './data/PROCESSED/' + file_name
    joblib.dump(data, file_path)
    

def import_joblib(file_path: str) -> any:
    """
    Preferred src import function
    """
    return joblib.load(file_path)


def empty() -> None:
    """
    Class object
    """
    ...