# connections/sql_connection.py

import pyodbc

def get_sql_connection(details):
    connection_string = (
        f"DRIVER={{ODBC Driver 17 for SQL Server}};"
        f"SERVER={details['host']},{details['port']};"
        f"DATABASE={details['database']};"
        f"UID={details['username']};"
        f"PWD={details['password']};"
    )
    return pyodbc.connect(connection_string)
