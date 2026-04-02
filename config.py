import os
import json
import base64

class Config:
    DB_HOST = os.getenv('DB_HOST', '').strip()
    DB_PORT = os.getenv('DB_PORT', '1433').strip()
    DB_NAME = os.getenv('DB_NAME', '').strip()
    DB_USER = os.getenv('DB_USER', '').strip()
    DB_PASSWORD = os.getenv('DB_PASSWORD', '').strip()
    DB_SCHEMA = os.getenv('DB_SCHEMA', 'dbo').strip()

    _auth_json_raw = os.getenv("AUTH_JSON", "{}")
    try:
        if _auth_json_raw == "{}":
             AUTH_JSON = {}
        else:
             AUTH_JSON = json.loads(base64.b64decode(_auth_json_raw))
    except Exception as e:
        print(f"Error decoding AUTH_JSON: {e}")
        AUTH_JSON = {}

    TOPIC_ID = os.getenv("PUB_SUB_TOPIC", "")
    USER_EMAIL = os.getenv("USER_EMAIL", "")
    TRANSACTIONS_USER_ID = os.getenv("TRANSACTIONS_USER_ID", "55e09d20-8fa3-4ddd-88f6-2fc24ab324c9")