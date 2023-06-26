import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries

def create_tables(cur, conn):
    """
    Create the fact and dimension tables in the Redshift database.

    Args:
        cur: Cursor object to execute SQL queries.
        conn: Connection object to interact with the database.

    Returns:
        None
    """

def drop_tables(cur, conn):
    """
    Drops all the tables if they exist.
    """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """
    Creates all the required tables.
    """
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def verify_tables(cur, conn):
    """
    Verify if the tables were created successfully.
    """
    for query in create_table_queries:
        table_name = query.split()[5]  # Extract the table name from the query
        verify_query = f"SELECT COUNT(*) FROM information_schema.tables WHERE table_name = '{table_name}'"
        cur.execute(verify_query)
        result = cur.fetchone()[0]
        if result > 0:
            print("Table creation verified: {}".format(table_name))
        else:
            print("Table creation verification failed: {}".format(table_name))



def main():
    # Read the AWS Redshift cluster configuration from dwh.cfg
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    # Connect to the Redshift cluster
    conn = psycopg2.connect(
        "host={} dbname={} user={} password={} port={}".format(
            config.get('CLUSTER', 'HOST'),
            config.get('CLUSTER', 'DB_NAME'),
            config.get('CLUSTER', 'DB_USER'),
            config.get('CLUSTER', 'DB_PASSWORD'),
            config.get('CLUSTER', 'DB_PORT')
        )
    )
    cur = conn.cursor()

    # Drop existing tables if req'd
    drop_tables(cur, conn)

    # Create the new tables
    create_tables(cur, conn)
    
    # Verify the proper table creation
    verify_tables(cur, conn)

    # Close the database connection
    conn.close()


if __name__ == "__main__":
    main()
