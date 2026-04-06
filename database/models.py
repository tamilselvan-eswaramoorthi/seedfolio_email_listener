from typing import Optional, List
from sqlmodel import Field, SQLModel, Relationship
from datetime import datetime, date

class User(SQLModel, table=True):
    user_id: str = Field(primary_key=True, max_length=36)
    username: str = Field(unique=True, max_length=255)
    email: str = Field(unique=True, max_length=255)
    pan_card: str = Field(unique=True, max_length=10)
    hashed_password: str = Field(max_length=255)
    created_at: datetime = Field(default_factory=datetime.now, nullable=False)
    updated_at: datetime = Field(default_factory=datetime.now, nullable=False)
    last_login: Optional[datetime] = Field(default=None)
    transactions: List["Transaction"] = Relationship(back_populates="user")
    holdings: List["Holdings"] = Relationship(back_populates="user")
    google_token: Optional["GoogleOAuthToken"] = Relationship(back_populates="user")

class Transaction(SQLModel, table=True):
    transaction_id: str = Field(primary_key=True, max_length=36)
    user_id: Optional[str] = Field(default=None, foreign_key="user.user_id", max_length=36)
    transaction_datetime: datetime = Field(default_factory=datetime.now, nullable=False)
    stock_symbol: str = Field(default="", max_length=20)
    stock_name: str = Field(default="", max_length=255)
    transaction_type: str = Field(default="", max_length=10)
    quantity: int = Field(default=0)
    price: float = Field(default=0.0)
    exchange: str = Field(default="", max_length=10) 
    broker: str = Field(default="", max_length=50)
    commission_local: float = Field(default=0.0)
    realized_pl: float = Field(default=0.0)
    inferred: bool = Field(default=False)
    user: Optional[User] = Relationship(back_populates="transactions")

class Holdings(SQLModel, table=True):
    holding_id: str = Field(primary_key=True, max_length=36)
    user_id: Optional[str] = Field(default=None, foreign_key="user.user_id", max_length=36)
    holding_datetime: datetime = Field(default_factory=datetime.now, nullable=False)
    stock_symbol: str = Field(default="", max_length=20)
    company_name: str = Field(default="", max_length=255)
    quantity: int = Field(default=0)
    avg_buy: float = Field(default=0.0)
    realized_pl: float = Field(default=0.0)
    user: Optional[User] = Relationship(back_populates="holdings")

class GoogleOAuthToken(SQLModel, table=True):
    id: str = Field(primary_key=True, max_length=36)
    user_id: str = Field(foreign_key="user.user_id", max_length=36, unique=True)
    token: str = Field(max_length=500)
    refresh_token: Optional[str] = Field(default=None, max_length=500)
    token_uri: str = Field(max_length=255)
    client_id: str = Field(max_length=255)
    client_secret: str = Field(max_length=255)
    scopes: str = Field(max_length=500)
    universe_domain: str = Field(default="googleapis.com", max_length=100)
    account: str = Field(default="", max_length=255)
    expiry: Optional[datetime] = Field(default=None)
    user: Optional["User"] = Relationship(back_populates="google_token")

class Stock(SQLModel, table=True):
    isin_code: str = Field(primary_key=True, max_length=20)
    name: str = Field(max_length=255)
    nse_symbol: Optional[str] = Field(default=None, index=True, max_length=50)
    bse_symbol: Optional[str] = Field(default=None, index=True, max_length=50)
    type: str = Field(default="stock", max_length=20, index=True)
    last_updated: datetime = Field(default_factory=datetime.now, nullable=False)

class IPO(SQLModel, table=True):
    id: int = Field(primary_key=True, max_length=36)
    company_name: str = Field(max_length=255)
    nse_symbol: Optional[str] = Field(default=None, index=True, max_length=50)
    bse_symbol: Optional[str] = Field(default=None, index=True, max_length=50)
    isin_code: Optional[str] = Field(default=None, index=True, max_length=20)
    offer_price: Optional[float] = Field(default=None)
    ipo_listing_date: Optional[datetime] = Field(default=None)
    last_updated: datetime = Field(default_factory=datetime.now, nullable=False)

class EmailTasks(SQLModel, table=True):
    id: int = Field(primary_key=True, max_length=36)
    message_id: str = Field(default=None, index=True, max_length=255)
    history_id: str = Field(default=None, index=True, max_length=255)
    status: str = Field(default=None, max_length=20)
    email_address: str = Field(default=None, max_length=255, nullable=True)
    created_at: datetime = Field(default_factory=datetime.now, nullable=False)


class Demerger(SQLModel, table=True):
    """One row per child entity produced by a demerger event."""
    id: Optional[int] = Field(default=None, primary_key=True)
    original_symbol: Optional[str] = Field(default=None, max_length=50)  # raw NSE symbol of original company, e.g. "TATAMOTORS"
    effective_date: date = Field(nullable=False)
    original_isin_code: Optional[str] = Field(default=None, max_length=20)
    child_1_name: Optional[str] = Field(default=None, max_length=255)
    child_1_isin_code: Optional[str] = Field(default=None, max_length=20)
    child_1_split_ratio: float = Field(default=1.0)   # new shares per 1 old share
    child_1_price_percentage: float = Field(default=100.0)   # child 1 price as % of original
    child_2_name: Optional[str] = Field(default=None, max_length=255)
    child_2_isin_code: Optional[str] = Field(default=None, max_length=20)
    child_2_split_ratio: float = Field(default=1.0)   # new shares per 1 old share
    child_2_price_percentage: float = Field(default=100.0)   # child 2 price as % of original
    last_updated: datetime = Field(default_factory=datetime.now, nullable=False)


class StockSplit(SQLModel, table=True):
    """Stock split / sub-division records."""
    id: Optional[int] = Field(default=None, primary_key=True)
    effective_date: date = Field(nullable=False)
    symbol: str = Field(max_length=50)             # raw NSE symbol, e.g. "DRREDDY"
    ratio: int = Field(default=1)                              # new shares per 1 old share
    last_updated: datetime = Field(default_factory=datetime.now, nullable=False)


class Bonus(SQLModel, table=True):
    """Bonus issue records."""
    id: Optional[int] = Field(default=None, primary_key=True)
    effective_date: date = Field(nullable=False)
    symbol: str = Field(max_length=50)             # raw NSE symbol, e.g. "KARURVYSYA"
    bonus: int = Field(default=1)                              # bonus shares issued
    per: int = Field(default=1)                                # per N existing shares held
    last_updated: datetime = Field(default_factory=datetime.now, nullable=False)

class CASStatus(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    user_id: str = Field(foreign_key="user.user_id", max_length=36, unique=True)
    status: str = Field(default="pending", max_length=20)   # "pending" | "received" | "processed"
    cas_statement_date: Optional[date] = Field(default=None)
    last_processed_at: Optional[datetime] = Field(default=None)
    created_at: datetime = Field(default_factory=datetime.now)