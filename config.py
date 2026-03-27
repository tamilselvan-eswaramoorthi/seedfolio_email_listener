import os
import json

class Config:
    DB_HOST = os.getenv('DB_HOST', '').strip()
    DB_PORT = os.getenv('DB_PORT', '1433').strip()
    DB_NAME = os.getenv('DB_NAME', '').strip()
    DB_USER = os.getenv('DB_USER', '').strip()
    DB_PASSWORD = os.getenv('DB_PASSWORD', '').strip()
    DB_SCHEMA = os.getenv('DB_SCHEMA', 'dbo').strip()

    AUTH_JSON = os.getenv("AUTH_JSON", "{}")

    TOPIC_ID = os.getenv("PUB_SUB_TOPIC")

    USER_EMAIL = os.getenv("USER_EMAIL") 