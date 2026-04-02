from datetime import date, datetime
from typing import Any, Dict, List, Optional

from sqlmodel import select
from database import db_handler, Demerger, StockSplit, Bonus

def _rows_to_demerger_dict(rows) -> Optional[Dict[str, Any]]:
    """Convert a list of Demerger DB rows (same original symbol) into the
    legacy dict shape expected by callers."""
    if not rows:
        return None
    first = rows[0]
    return {
        "effective_date": first.effective_date,
        "raw_symbol": first.original_symbol,
        "bse_scrip_code": first.bse_scrip_code,
        "children": [
            {
                "symbol": r.child_symbol,
                "bse_symbol": r.child_bse_symbol,
                "company_name": r.company_name,
                "ratio": r.ratio,
                "price_ratio": r.price_ratio,
                "keep_original": r.keep_original,
            }
            for r in rows
        ],
    }


def _get_demerger_from_db(*, raw_symbol: Optional[str] = None, bse_scrip_code: Optional[str] = None,
                           transaction_date: Optional[date] = None) -> Optional[Dict[str, Any]]:
    try:
        with db_handler.get_session() as session:
            stmt = select(Demerger)
            if raw_symbol:
                stmt = stmt.where(Demerger.original_symbol == raw_symbol.upper())
            elif bse_scrip_code:
                stmt = stmt.where(Demerger.bse_scrip_code == str(bse_scrip_code).strip())
            else:
                return None
            rows = session.exec(stmt).all()
            if transaction_date:
                rows = [r for r in rows if transaction_date <= r.effective_date]
            return _rows_to_demerger_dict(rows)
    except Exception:
        return None


def _get_split_from_db(symbol: str, transaction_date: date) -> Optional[Dict[str, Any]]:
    try:
        base = symbol.upper().split(".")[0]
        with db_handler.get_session() as session:
            stmt = select(StockSplit).where(StockSplit.symbol == base)
            rows = session.exec(stmt).all()
            for r in rows:
                if transaction_date <= r.effective_date:
                    return {"effective_date": r.effective_date, "symbol": r.symbol, "ratio": r.ratio}
    except Exception:
        pass
    return None


def _get_bonus_from_db(symbol: str, transaction_date: date) -> Optional[Dict[str, Any]]:
    try:
        base = symbol.upper().split(".")[0]
        with db_handler.get_session() as session:
            stmt = select(Bonus).where(Bonus.symbol == base)
            rows = session.exec(stmt).all()
            for r in rows:
                if transaction_date <= r.effective_date:
                    return {"effective_date": r.effective_date, "symbol": r.symbol,
                            "bonus": r.bonus, "per": r.per}
    except Exception:
        pass
    return None

def get_demerger_by_raw_symbol(raw_symbol: str, transaction_date: date) -> Optional[Dict[str, Any]]:
    if isinstance(transaction_date, str):
        transaction_date = datetime.strptime(transaction_date, "%Y-%m-%d").date()
    result = _get_demerger_from_db(raw_symbol=raw_symbol, transaction_date=transaction_date)
    if result:
        return result

def get_demerger_by_bse_code(scrip_code: str, transaction_date: date) -> Optional[Dict[str, Any]]:
    if isinstance(transaction_date, str):
        transaction_date = datetime.strptime(transaction_date, "%Y-%m-%d").date()
    result = _get_demerger_from_db(bse_scrip_code=scrip_code, transaction_date=transaction_date)
    if result:
        return result


def get_split(symbol: str, transaction_date: date) -> Optional[Dict[str, Any]]:
    if isinstance(transaction_date, str):
        transaction_date = datetime.strptime(transaction_date, "%Y-%m-%d").date()
    result = _get_split_from_db(symbol, transaction_date)
    if result:
        return result


def get_bonus(symbol: str, transaction_date: date) -> Optional[Dict[str, Any]]:
    if isinstance(transaction_date, str):
        transaction_date = datetime.strptime(transaction_date, "%Y-%m-%d").date()
    result = _get_bonus_from_db(symbol, transaction_date)
    if result:
        return result
