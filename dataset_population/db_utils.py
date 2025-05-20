import sqlalchemy as db

from config import DB_CONNECTION_URL    


def run_sql_query(query):
    engine = db.create_engine(DB_CONNECTION_URL) # parameter echo=True for sqlalchemy logging

    sql = db.text(query)
    
    with engine.connect() as conn:
        results = conn.execute(sql)
        links = [row[1] for row in results]

        return links
    

if __name__ == '__main__':
    run_sql_query('SELECT * from PROJECT')
