import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """Load log_data & song_data from S3 Bucket and insert into staging_events &   staging_songs"""
    for query in copy_table_queries:
        print('Loading query data: '+query)
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """INSERT data from staging tables to 
the star schema, dimension and fact tables"""
    for query in insert_table_queries:
        print('Processing query:'+query)
        cur.execute(query)
        conn.commit()


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')
  
    print('Connecting to redshift')
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    print('Connected to redshift')
    cur = conn.cursor()
    
    print('Loading staging tables')
    #load_staging_tables(cur, conn)
    
    print('Transform from staging')
    insert_tables(cur, conn)

    conn.close()
    print('ETL Ended')


if __name__ == "__main__":
    main()