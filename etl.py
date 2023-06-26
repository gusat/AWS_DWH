import re
import psycopg2
import configparser
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """
    Load data from S3 to staging tables on Redshift.
    """
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """
    Load data from staging tables to analytics tables on Redshift.
    """
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def verify_tables(cur, conn):
    """
    Verify if the tables were created successfully.
    """
    for query in copy_table_queries:
        match = re.search(r"CREATE TABLE IF NOT EXISTS (\w+)", query)
        if match:
            table_name = match.group(1)
            verify_query = f"SELECT COUNT(*) FROM information_schema.tables WHERE table_name = '{table_name}'"
            cur.execute(verify_query)
            result = cur.fetchone()[0]
            if result > 0:
                print("Table creation verified: {}".format(table_name))
            else:
                print("Table creation verification failed: {}".format(table_name))



def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(
        config['CLUSTER']['HOST'],
        config['CLUSTER']['DB_NAME'],
        config['CLUSTER']['DB_USER'],
        config['CLUSTER']['DB_PASSWORD'],
        config['CLUSTER']['DB_PORT']
    ))
    cur = conn.cursor()

    load_staging_tables(cur, conn)
    insert_tables(cur, conn)
    verify_tables(cur, conn)

    conn.close()

if __name__ == "__main__":
    main()
