import psycopg2
import sys
import os


def roll_out_ddl(file_path: str):
    splited_path = file_path.split("/")
    schema, table = splited_path[-2], splited_path[-1]
    host = os.environ['PGHOST']
    port = os.environ['PGPORT']
    database = os.environ['PGDATABASE']
    user = os.environ['PGUSER']
    password = os.environ['PGPASSWORD']
    print(f"""
    host: {host}
    port: {port}
    database: {database}
    user: {user}
    password: {password}
    """)
    with open(file_path) as f:
        query_creating = f.read()
    print(f"Query for creating table:\n{query_creating}\n")
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
    with psycopg2.connect(user=user, password=password, host=host, port=port, database=database) as connection:
        print("PostgreSQL connection is opened\n")
        with connection.cursor() as cursor:
            print("PostgreSQL cursor is opened\n")
            cursor.execute(query_existed)
            is_exists = cursor.fetchone()[0]
            is_empty = True
            if is_exists:
                cursor.execute(query_empty)
                is_empty = bool(cursor.fetchone()[0])

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


if __name__ == "__main__":
    x = input()
    if x:
        print(x)
    else:
        print('empty input')