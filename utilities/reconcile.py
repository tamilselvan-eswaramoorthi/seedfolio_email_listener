
from database import db_handler,Transaction, Stock
from sqlalchemy import select
from datetime import datetime, timedelta
import yfinance as yf

def _fill_missing_transactions(extractions, cas_holdings):
    """ For each holding in CAS, find the corresponding extractions and group by broker """
    statement_date = cas_holdings['statement_date']
    statement_date = datetime.strptime(statement_date, "%d-%m-%Y").date()
    ret_extractions = extractions[:]
    for broker, holdings in cas_holdings.items():
        if broker in ["mutual_funds", "statement_date", "transactions", "other"]:
            continue
        for holding in holdings:
            matched_extractions = []
            company_name = None
            symbol = None
            for e in extractions:
                if holding['isin'].lower() == e['isin'].lower():
                    company_name = e['company_name']
                    symbol = e['symbol']
                    if 'broker' in e:
                        broker_name = e['broker'].lower()
                        trade_date = e['trade_datetime']
                        if isinstance(trade_date, str):
                            trade_dt = datetime.strptime(trade_date, "%Y-%m-%d").date()
                        else:
                            trade_dt = trade_date.date()
                        if trade_dt <= statement_date:
                            if broker_name == broker:
                                matched_extractions.append(e)
                    else:
                        print (f"Extraction without broker info: {e}")
            
            if not symbol:
                with db_handler.get_session() as session:
                    stock = session.exec(select(Stock).where(Stock.isin_code == holding['isin'])).first()
                    if stock:
                        symbol = stock.nse_symbol or stock.bse_symbol
                        company_name = stock.name

            qty_ext = sum(e['quantity'] if e['transaction_type'] == 'BUY' else -e['quantity'] for e in matched_extractions)
            cost_ext = sum(float(e['rate']) * e['quantity'] if e['transaction_type'] == 'BUY' else -float(e['rate']) * e['quantity'] for e in matched_extractions)
            
            qty_diff = holding['free_bal'] - qty_ext
            
            if abs(qty_diff) > 0.01:
                mkt_price = holding.get('market_price')
                if not mkt_price and holding.get('value') and holding.get('free_bal'):
                    mkt_price = float(holding['value']) / float(holding['free_bal'])
                
                # Calculate profit contributed by known extractions
                known_profit = 0
                for e in matched_extractions:
                    p = (mkt_price - float(e['rate'])) * (e['quantity'] if e['transaction_type'] == 'BUY' else -e['quantity'])
                    known_profit += p

                
                value_diff = holding['value'] - cost_ext
                missing_profit = value_diff - known_profit

                approx_rate = mkt_price - (missing_profit / qty_diff)

                # Use yfinance to find the nearest date
                if symbol:
                    ticker_symbols = [f"{symbol}.NS", f"{symbol}.BO"]
                    try:
                        # Find first transaction date or go back 1 year
                        start_dt = statement_date - timedelta(days=365)
                        if matched_extractions:
                            for e in matched_extractions:
                                if isinstance(e['trade_datetime'], str):
                                    e['trade_datetime'] = datetime.strptime(e['trade_datetime'], "%Y-%m-%d")
                            first_trade = min(e['trade_datetime'].date() for e in matched_extractions)
                            start_dt = min(start_dt, first_trade)

                        for ticker_symbol in ticker_symbols:
                            try:
                                hist = yf.download(ticker_symbol, start=start_dt, end=statement_date + timedelta(days=1), progress=False)
                                break
                            except Exception as ex:
                                print(f"Error fetching data for {ticker_symbol}: {ex}")
                                hist = None

                        if hist is not None and not hist.empty:
                            # Find date where Close is closest to approx_rate
                            hist['diff'] = (hist['Close'] - approx_rate).abs()
                            best_match_date = hist['diff'].idxmin()
                            best_match_price = hist.loc[best_match_date, 'Close'].values if best_match_date in hist.index else approx_rate
                            
                            inferred_transaction = {
                                'company_name': company_name,
                                'symbol': symbol,
                                'isin': holding['isin'],
                                'rate': round(float(best_match_price), 2),
                                'quantity': abs(qty_diff),
                                'trade_datetime': best_match_date.strftime("%Y-%m-%d %H:%M:%S%z"), # type: ignore
                                'transaction_type': "BUY" if qty_diff > 0 else "SELL",
                                'exchange': 'NSE',
                                'broker': broker,
                                'is_inferred': True
                            }
                            ret_extractions.append(inferred_transaction)
                    except Exception as ex:
                        import traceback
                        traceback_str = traceback.format_exc()
                        print(traceback_str)
                        print(f"  Error fetching data for {ticker_symbol}: {ex}")
    return ret_extractions


def _reconcile_cas_transactions(extractions, cas_holdings):
    ret_extractions = extractions[:]
    cas_transactions = cas_holdings.get("transactions", [])
    for cas_tx in cas_transactions:
        cas_date = datetime.strptime(cas_tx['date'], "%d-%m-%Y").date()
        isin = cas_tx['isin']
        broker = cas_tx['broker']
        brought_qty = cas_tx['brought_quantity']
        
        matched_qty = 0
        symbol = None
        company_name = None
        for e in ret_extractions:
            if e['isin'].lower() == isin.lower() and e.get('broker', '').lower() == broker.lower():
                if not symbol:
                    symbol = e['symbol']
                    company_name = e['company_name']
                trade_date = e['trade_datetime']
                trade_dt = datetime.strptime(trade_date, "%Y-%m-%d").date() if isinstance(trade_date, str) else trade_date.date()
                if trade_dt == cas_date and e['transaction_type'] == 'BUY':
                    matched_qty += e['quantity']
        
        qty_diff = brought_qty - matched_qty
        if qty_diff > 0.01:
            if not symbol:
                with db_handler.get_session() as session:
                    stock = session.exec(select(Stock).where(Stock.isin_code == isin)).first()
                    if stock:
                        symbol = stock.nse_symbol or stock.bse_symbol
                        company_name = stock.name
            
            if symbol:
                ticker_symbols = [f"{symbol}.NS", f"{symbol}.BO"]
                approx_rate = 0.0
                try:
                    for ticker_symbol in ticker_symbols:
                        try:
                            hist = yf.download(ticker_symbol, start=cas_date - timedelta(days=5), end=cas_date + timedelta(days=1), progress=False)
                            if hist is not None and not hist.empty:
                                if cas_date in hist.index:
                                    val = hist.loc[cas_date, 'Close']
                                else:
                                    val = hist['Close'].iloc[-1]
                                approx_rate = float(val.iloc[0] if hasattr(val, 'iloc') else val) 
                                break
                        except Exception:
                            pass
                except Exception:
                    pass
                
                inferred_transaction = {
                    'company_name': company_name,
                    'symbol': symbol,
                    'isin': isin,
                    'rate': round(approx_rate, 2) if approx_rate else 0.0,
                    'quantity': qty_diff,
                    'trade_datetime': cas_date.strftime("%Y-%m-%d 12:00:00+0000"),
                    'transaction_type': "BUY",
                    'exchange': 'NSE',
                    'broker': broker,
                    'is_inferred': True
                }
                ret_extractions.append(inferred_transaction)
    return ret_extractions


def process_and_reconcile_from_cas(cas_holdings, user_id):
    if not cas_holdings:
        return []

    with db_handler.get_session() as session:
        statement = select(Transaction).where(Transaction.user_id == user_id).order_by(Transaction.transaction_datetime) # type: ignore
        transactions = session.exec(statement).scalars().all() # type: ignore

        extractions = []
        for t in transactions:
            base_symbol = t.stock_symbol.split(".")[0]
            statement = select(Stock).where((Stock.nse_symbol == base_symbol) | (Stock.bse_symbol == base_symbol))
            stock = session.exec(statement).first() # type: ignore
            isin = stock.isin_code if stock else ""

            extractions.append({
                "company_name": t.stock_name,
                "symbol": t.stock_symbol,
                "isin": isin,
                "rate": t.price,
                "quantity": t.quantity,
                "trade_datetime": t.transaction_datetime,
                "transaction_type": t.transaction_type,
                "exchange": t.exchange,
                "broker": t.broker,
                "is_inferred": t.inferred
            })

    # Step 1: Reconcile based on CAS detailed transactions
    extractions_with_tx = _reconcile_cas_transactions(extractions, cas_holdings)

    # Step 2: Reconcile overall holdings (before that month or historical discrepancies)
    inferred_txns = _fill_missing_transactions(extractions_with_tx, cas_holdings)

    # Return only the newly inferred transactions
    return inferred_txns[len(extractions):]
