import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries



def drop_tables(cur, conn):
    """Drop tables from DB if they already exist"""
    for query in drop_table_queries:
        print('Executing drop: '+query)
        try:
            cur.execute(query)
            conn.commit()
        except psycopg2.Error as e:
            print('Error: dropping table:' + query)
            print(e)
    print('Tables dropped')
    
    


def create_tables(cur, conn):
    """Create tables (songplays, users, artists, songs, time)"""
    for query in create_table_queries:
        print('Executing create: '+query)
        try:
            cur.execute(query)
            conn.commit()
        except psycopg2.Error as e:
            print('Error: Creating table:' + query)
            print(e)
        print('Table created')


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    print('Connecting to redshift')
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    print('Connected to redshift')
    cur = conn.cursor()

    print('Dropping existing tables if any')
    drop_tables(cur, conn)
    
    print('Creating tables')
    create_tables(cur, conn)

    conn.close()
    print('Create table Ended')


if __name__ == "__main__":
    main()

