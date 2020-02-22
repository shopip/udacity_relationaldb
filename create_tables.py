import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def create_database():

    # Create the spartifydb database.
    # If it already exists, Drop it first and create the database.
    #
    # Returns:
    #     cursor(psycopg2.cursor): The psycopg2 cursor
    #     connection(psycopg2.connection): The sparkifydb connection


    # connect to student database
    conn = psycopg2.connect(
        "host=127.0.0.1 dbname=student user=student password=student"
    )

    #auto commit is true so that we dont have to commit transactioon after each query.
    conn.set_session(autocommit=True)
    cur = conn.cursor()

    # create sparkify database with UTF8 encoding
    cur.execute("DROP DATABASE IF EXISTS sparkifydb")
    # template0 to add new encoding and locale system
    cur.execute(
        "CREATE DATABASE sparkifydb WITH ENCODING 'utf8' TEMPLATE template0"
    )

    # close connection to default database
    conn.close()

    # connect to sparkify database
    conn = psycopg2.connect(
        "host=127.0.0.1 dbname=sparkifydb user=student password=student"
    )
    cur = conn.cursor()

    return cur, conn


def drop_tables(cur, conn):
    # Dropping table those are already exists
    # Reading queries from drop table from the file sql queries
    # Args:
    #     cur (psycopg2.cursor): The psycopg2 cursor
    #     conn (psycopg2.connection): The Database connection

    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    # Dropping table those are already exists
    # Reading queries from drop table from the file sql queries
    # Args:
    #     cur (psycopg2.cursor): The psycopg2 cursor
    #     conn (psycopg2.connection): The Database connection

    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    cur, conn = create_database()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()

# this will only execute if it runs as a main, if other imports it, this function will not be executed.
if __name__ == "__main__":
    main()
