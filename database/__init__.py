from .models import User, Transaction, Holdings, GoogleOAuthToken, Stock, IPO, EmailTasks, Demerger, StockSplit, Bonus  # noqa: F401
from .database import Database

try:
    db_handler = Database()
except Exception as e:
    print(f"Error initializing database: {e}")
    db_handler = None