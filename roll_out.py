import psycopg2
from psycopg2 import Error
import sys

if __name__ == "__main__":
    file_path = sys.argv[1]
    host = sys.argv[2]
    port = sys.argv[3]
    database = sys.argv[4]
    user = sys.argv[5]
    password = sys.argv[6]
    with open(file_path) as f:
        create_query = f.read()
    print(create_query)
    connection = None
    try:
        connection = psycopg2.connect(user=user,
                                      password=password,
                                      host=host,
                                      port=port,
                                      database=database)

        cursor = connection.cursor()
        # SQL query to create a new table
        create_table_query = '''CREATE TABLE mobile
              (ID INT PRIMARY KEY     NOT NULL,
              MODEL           TEXT    NOT NULL,
              PRICE         REAL); '''
        # Execute a command: this creates a new table
        cursor.execute(create_table_query)
        connection.commit()
        print("Table created successfully in PostgreSQL ")

    except (Exception, Error) as error:
        print("Error while connecting to PostgreSQL", error)
    finally:
        if connection:
            cursor.close()
            connection.close()
            print("PostgreSQL connection is closed")