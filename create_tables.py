import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def create_database():
    # connect to default database
    try:
        conn = psycopg2.connect("host=127.0.0.1 dbname=studentdb user=student password=student")
        conn.set_session(autocommit=True)
    except psycopg2.Error as e:
        print("Error: Cannot connect to 'studentdb' database as student user")
        print(e)
    
    try:
        cur = conn.cursor()
    except psycopg2.Error as e:
        print("Error: Cannot get cursor to the database")
        print(e)
    
    # create sparkify database with UTF8 encoding
    try:
        cur.execute("DROP DATABASE IF EXISTS sparkifydb")
        cur.execute("CREATE DATABASE sparkifydb WITH ENCODING 'utf8' TEMPLATE template0")
    except psycopg2.Error as e:
        print("Error: Cannot create 'sparkifydb' database")
        print(e)

    # close connection to default database
    conn.close()    
    
    # connect to sparkify database
    try:
        conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
        cur = conn.cursor()
    except psycopg2.Error as e:
        print("Error: Cannot connect to 'sparkifydb' as student user")
        print(e)
    
    return cur, conn


def drop_tables(cur, conn):
    for query in drop_table_queries:
        try:
            cur.execute(query)
        except psycopg2.Error as e:
            print("Error: Dropping table: ", (query))
            print(e)
        finally:
            conn.commit()


def create_tables(cur, conn):
    for query in create_table_queries:
        try:
            cur.execute(query)
        except psycopg2.Error as e:
            print("Error: Creating table: ", (query))
        finally:
            conn.commit()


def main():
    cur, conn = create_database()
    
    create_tables(cur, conn)
#    drop_tables(cur, conn) # drop must be done separately

    try:
        conn.close()
    except psycopg2.Error as e:
        print("Error: Closing connection to database")


if __name__ == "__main__":
    main()
