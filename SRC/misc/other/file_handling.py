import joblib
import csv
import os

def count_csv_rows(path: str) -> int:
    """
    Count the number of rows in a CSV file.
    """
    with open(path,'r') as file:
        read = csv.reader(file)
        row_count = sum(1 for _ in read)

    return row_count


def save_joblib(data:any,file_name: str) -> any:
    """
    Saves data to a file using joblib, ensuring the directory exists.
    """
    file_path = './data/PROCESSED/' + file_name

    # Ensure the directory exists
    os.makedirs(os.path.dirname(file_path), exist_ok=True)

    joblib.dump(data, file_path)
    

def import_joblib(file_path: str) -> any:
    """
    Loads and returns data from a joblib file.
    """
    return joblib.load(file_path)


def blank() -> None:
    """
    Placeholder function, can be used to define a class or other logic later.
    """
    ...