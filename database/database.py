import urllib.parse
from sqlmodel import create_engine, Session, SQLModel
from sqlalchemy.pool import QueuePool
from config import Config

class Database:
    def __init__(self):
        driver = urllib.parse.quote_plus("ODBC Driver 18 for SQL Server")
        password = urllib.parse.quote_plus(Config.DB_PASSWORD)
        if Config.DB_HOST.endswith(".database.windows.net"):
            db_conn_str = (
                f"mssql+pyodbc://{Config.DB_USER}:{password}"
                f"@{Config.DB_HOST}:{Config.DB_PORT}/{Config.DB_NAME}"
                f"?driver={driver}"
                f"&Encrypt=yes&TrustServerCertificate=yes&Connection Timeout=30"
            )
        elif ".supabase.co" in Config.DB_HOST or ".pooler.supabase.com" in Config.DB_HOST or Config.DB_USER == "postgres":
            db_conn_str = (
                f"postgresql+psycopg2://{Config.DB_USER}:{password}"
                f"@{Config.DB_HOST}:{Config.DB_PORT}/{Config.DB_NAME}?sslmode=require"
            )
        else:
            # Local MySQL connection string
            db_conn_str = (
                f"mysql+pymysql://{Config.DB_USER}:{password}"
                f"@{Config.DB_HOST}:{Config.DB_PORT}/{Config.DB_NAME}"
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
