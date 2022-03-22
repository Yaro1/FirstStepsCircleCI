import psycopg2
import os


def roll_out_ddl(file_path: str):
    splited_path = file_path.split("/")
    schema, table = splited_path[-2], splited_path[-1].split(".")[0]
    host = os.environ['PGHOST']
    port = os.environ['PGPORT']
    database = os.environ['PGDATABASE']
    user = os.environ['PGUSER']
    password = os.environ['PGPASSWORD']
    # host = "localhost"
    # port = 5432
    # database = "postgres"
    # user = "postgres"
    # password = "docker"
    # schema = "public"
    with open(file_path) as f:
        query_creating = f.read()
    query_existed = f"""
        select exists (
        select from 
            pg_tables
        where 
            schemaname = '{schema}' and 
            tablename  = '{table}'
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
            print(f"Check is table already exists.\n{query_existed}\n")
            cursor.execute(query_existed)
            is_exists = cursor.fetchone()[0]
            is_empty = True
            if is_exists:
                print(f"Check is table empty.\n{query_empty}\n")
                cursor.execute(query_empty)
                is_empty = not bool(int(cursor.fetchone()[0]))

            if not is_exists:
                print(f"Create table:\n{query_creating}\n")
                cursor.execute(query_creating)
                connection.commit()
                print("Table was created.\n")
            elif is_exists and is_empty:
                print(f"Drop table:\n{query_drop}\n")
                cursor.execute(query_drop)
                print(f"Recreate table:\n{query_creating}\n")
                cursor.execute(query_creating)
                connection.commit()
                print("Table was recreated.\n")
            else:
                print("Table is exists and not empty.\n")


if __name__ == "__main__":
    while True:
        file_path = input()
        if not file_path:
            break
        elif 'ddl/' in file_path:
            roll_out_ddl(file_path)
