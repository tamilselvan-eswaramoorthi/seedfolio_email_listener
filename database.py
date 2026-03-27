import urllib.parse
from sqlmodel import create_engine, Session, SQLModel
from sqlalchemy.pool import QueuePool
from config import Config
from models import EmailTasks

class Database:
    def __init__(self):
        driver = urllib.parse.quote_plus("ODBC Driver 18 for SQL Server")
        password = urllib.parse.quote_plus(Config.DB_PASSWORD)
        db_conn_str = (
            f"mssql+pyodbc://{Config.DB_USER}:{password}"
            f"@{Config.DB_HOST}:{Config.DB_PORT}/{Config.DB_NAME}"
            f"?driver={driver}"
            f"&Encrypt=yes&TrustServerCertificate=yes&Connection Timeout=30"
        )
        self.engine = create_engine(db_conn_str, 
                                    echo=False,
                                    poolclass=QueuePool,
                                    pool_size=5,
                                    max_overflow=10,
                                    pool_timeout=30,
                                    pool_recycle=1800
        )

        self.create_db_and_tables()

    def create_db_and_tables(self):
        SQLModel.metadata.create_all(self.engine)

    def get_session(self):
        return Session(self.engine)
    
    def bulk_save(self, records):
        with self.get_session() as session:
            session.bulk_save_objects(records)
            session.commit()

db_handler = Database()
