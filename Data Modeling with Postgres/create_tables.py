#Import PostgreSQL database driver and SQL create and drop table module to perform operations on PostgreSQL using python
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def create_database():
    """
    - Creates and connects to the sparkifydb
    - Returns the connection and cursor to sparkifydb
    """
    
    # Connect to default database
    conn = psycopg2.connect("host=127.0.0.1 dbname=studentdb user=student password=student")
    conn.set_session(autocommit=True)
    cur = conn.cursor()
    
    # Create sparkify database with UTF8 encoding
    cur.execute("DROP DATABASE IF EXISTS sparkifydb")
    cur.execute("CREATE DATABASE sparkifydb WITH ENCODING 'utf8' TEMPLATE template0")

    # Close connection to default database
    conn.close()    
    
    # Connect to sparkify database
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()
    
    return cur, conn


def drop_tables(cur, conn):
    """
    This function DROPS each table using the queries in drop_table_queries list created sql_queris.py scripts.
    """
    for query_list in drop_table_queries:
        cur.execute(query_list)
        conn.commit()


def create_tables(cur, conn):
    """
    This function CREATES each table using the queries in create_table_queries created sql_queris.py scripts. 
    """
    for query_list in create_table_queries:
        cur.execute(query_list)
        conn.commit()


def main():
    """
    This main function creates and connect to the database, do all drop/create tables and finally closes connection.
    The top down approach are as follows
    
    1. Establishes connection to default database studentdb using create_database() function.
    2. Drops (if exists) and Creates the sparkify database using create_database() function.
    3. Close connection to default database
    4. Establishes connection to sparkify database and gets cursor to it.
    5. Drops all the tables using drop_tables(cur, conn) function
    6. Creates all tables needed using create_tables(cur, conn) function and  
    7. Finally, closes the connection. 
    """
    cur, conn = create_database()
    
    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()