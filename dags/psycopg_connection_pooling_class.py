import psycopg2
from psycopg2.pool import ThreadedConnectionPool

class PGConnectionPoolWrapper:
    def __init__(self, minconn=1, maxconn=10):
        self.pool = ThreadedConnectionPool(
            minconn, maxconn,
            dbname='llmdp',
            user='airflow',
            password='airflow',
            host='localhost',
            port='5432'
        )
        
    def get_conn(self):
        return self.pool.getconn()
    
    def put_conn(self, conn):
        self.pool.putconn(conn)
        
    def close_all(self):
        self.pool.closeall()
