import psycopg2
from psycopg2 import Error
import sys


if __name__ == "__main__":
    file_path = sys.argv[1]
    splited_path = file_path.split("/")
    schema, table = splited_path[-2], splited_path[-1]
    host = sys.argv[2]
    port = sys.argv[3]
    database = sys.argv[4]
    user = sys.argv[5]
    password = sys.argv[6]
    with open(file_path) as f:
        query_creating = f.read()
    print(f"Query for creating table:\n{query_creating}")
    query_existed = f"""
    select exists (
    select from 
        pg_tables
    where 
        schemaname = {schema} and 
        tablename  = {table}
    )
    """
    query_empty = f"""
    select count(1) from {schema}.{table} limit 1
    """
    query_drop = f"""
    drop table {schema}.{table}
    """
    try:
        with psycopg2.connect(user=user, password=password, host=host, port=port, database=database) as connection:
            print("PostgreSQL connection is opened")
            with connection.cursor() as cursor:
                print("PostgreSQL cursor is opened")
                cursor.execute(query_existed)
                is_exists = "True" == cursor.fetchone()[0]
                is_empty = True
                if is_exists:
                    cursor.execute(query_empty)
                    is_empty = int(cursor.fetchone()[0]) == 0

                if not is_exists:
                    cursor.execute(query_creating)
                    connection.commit()
                    print("Table was created.\n")
                elif is_exists and is_empty:
                    cursor.execute(query_drop)
                    cursor.execute(query_creating)
                    connection.commit()
                    print("Table was recreated.\n")
                else:
                    print("Table is exists and not empty.\n")
    except (Exception, Error) as error:
        print("Error while connecting to PostgreSQL", error)
