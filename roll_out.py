import psycopg2
import os
import logging
logging.basicConfig(level=logging.INFO)


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
            select 
              from pg_tables
             where schemaname = '{schema}'
               and tablename  = '{table}'
        )
        """
    query_empty = f"""
        select count(1)
          from {schema}.{table}
         limit 1
        """
    query_drop = f"""
        drop table {schema}.{table}
        """
    with psycopg2.connect(user=user, password=password, host=host, port=port, database=database) as connection:
        with connection.cursor() as cursor:
            logging.info(f"\nCheck is table already exists.\n{query_existed}\n")
            cursor.execute(query_existed)
            is_exists = cursor.fetchone()[0]
            is_empty = True
            if is_exists:
                logging.info(f"\nCheck is table empty.\n{query_empty}\n")
                cursor.execute(query_empty)
                is_empty = not bool(int(cursor.fetchone()[0]))

            if not is_exists:
                logging.info(f"\nCreate table:\n{query_creating}\n")
                cursor.execute(query_creating)
                connection.commit()
                logging.info("\nTable was created.\n")
            elif is_exists and is_empty:
                logging.info(f"\nDrop table:\n{query_drop}\n")
                cursor.execute(query_drop)
                logging.info(f"\nRecreate table:\n{query_creating}\n")
                cursor.execute(query_creating)
                connection.commit()
                logging.info("\nTable was recreated.\n")
            else:
                logging.info("\nTable is exists and not empty.\n")


if __name__ == "__main__":
    while True:
        file_path = input()
        if not file_path:
            break
        elif 'ddl/' in file_path:
            roll_out_ddl(file_path)
