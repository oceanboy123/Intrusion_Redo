import joblib

def save_joblib(data:any,file_name: str) -> any:
    file_path = '../data/PROCESSED/' + file_name
    joblib.dump(data, file_path)
    

def import_joblib(file_path: str) -> any:
    data: any = joblib.load(file_path)
    return data

def empty() -> None:
    ...