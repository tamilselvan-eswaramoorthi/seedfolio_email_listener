from .models import User, Transaction, Holdings, GoogleOAuthToken, Stock, IPO, EmailTasks, Demerger, StockSplit, Bonus  # noqa: F401
from .database import Database

db_handler = Database() 