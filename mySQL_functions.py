import json
import mysql.connector
from mysql.connector import Error

def start_my_connection():
    start_connection('127.0.0.1', 'root', 'Levi@data14', 'intrusion')




def start_connection(HOST, USER, PASS, DB):
    try:
        global connection
        connection = mysql.connector.connect(
            host=HOST,
            user=USER,
            password=PASS,
            database=DB,
        )

        if connection.is_connected():
        
            global row_pointer
            row_pointer = connection.cursor()
    except Error as e:
        print(f"Error: {e}")




def close_connection():
    row_pointer.close()
    connection.close()




def print_query(QUERY):

    row_pointer.execute(QUERY)

    results = row_pointer.fetchall()
    for i in results:
        print(i)




def data_query(QUERY, DATA):

    input_request = QUERY
    input_data = DATA

    row_pointer.execute(input, input_data)
    connection.commit()




def prepare_json_mySQL(file_name):
    with open(file_name+'.json') as file: data = json.load(file)

    columns = list(data[0].keys())
    column_def =  ', '.join([f"{columns} VARCHAR(255)" for column in columns])

    return column_def




def create_table_json(column_def, table_name):
    table_query = f"CREATE TABLE IF NOT EXISTS {table_name} ({column_def})"
    row_pointer.execute(table_query)
    connection.commit()

