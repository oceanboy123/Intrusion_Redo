import joblib
import csv


def count_csv_rows(path: str) -> int:
        """
        Count number of rows to identify the new recording's index
        """
        with open(path,'r') as file:
            read = csv.reader(file)
            row_count = sum(1 for _ in read)

        return row_count


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