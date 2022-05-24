import psycopg2

class Connection:
    
    def __init__(self, db_name = None):
        self.connection = self.open(db_name)
    
    def open(self, db_name):
        #establishing the connection
        conn = psycopg2.connect(dbname=db_name, user='postgres', password='admin', host='127.0.0.1', port= '5432')
        conn.autocommit = True
        return conn 
    
    def close(self):
        self.connection.close()
        
    def create_cursor(self):
        return self.connection.cursor()

    