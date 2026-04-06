import base64
import re
import json
import uuid
import yfinance as yf
from sqlmodel import select

from datetime import datetime, timezone, timedelta

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build

from config import Config
from utilities.pdf_processor import ExtractHoldings
from database import db_handler, GoogleOAuthToken, User, Holdings, Transaction, IPO, CASStatus, Stock, EmailTasks

class GetHoldingsFromGmail:
    def __init__(self):
        self.service = None
        self.extractor = ExtractHoldings()
        self.SCOPES = ["https://www.googleapis.com/auth/gmail.readonly"]
        self.authenticate()
        self.zerodha_query = "from:no-reply-transaction-with-holding-statement@reportsmailer.zerodha.net"
        self.cas_query = "from:eCAS@cdslstatement.com"
        self.nse_query = "from:nse-direct@nse.co.in"
        self.bse_query = "from:mgrpt@bseindia.com"

    def _get_user_details(self, user_id: str):
        with db_handler.get_session() as session:
            user = session.exec(select(User).where(User.user_id == user_id)).first()
            self.PASSWORD = user.pan_card # type: ignore

    def authenticate(self):
        creds = None
        with db_handler.get_session() as session:
            user_token = session.exec(select(GoogleOAuthToken).where(GoogleOAuthToken.user_id == Config.TRANSACTIONS_USER_ID)).first()
            if user_token:
                creds = Credentials(
                    token=user_token.token,
                    refresh_token=user_token.refresh_token,
                    token_uri=user_token.token_uri,
                    client_id=user_token.client_id,
                    client_secret=user_token.client_secret,
                    scopes=user_token.scopes.split(",") if user_token.scopes else self.SCOPES
                )
            
            if not creds or not creds.valid:
                if creds and creds.expired and creds.refresh_token:
                    creds.refresh(Request())
                    # Update DB with new tokens
                    token_dict = json.loads(creds.to_json())
                    user_token.token = token_dict.get("token", user_token.token) # type: ignore
                    if "expiry" in token_dict and token_dict["expiry"]:
                        user_token.expiry = datetime.fromisoformat(token_dict["expiry"].replace("Z", "+00:00")) # type: ignore
                    session.add(user_token)
                    session.commit()
                else:
                    raise Exception("No valid credentials found for user. User must re-authenticate.")
                    
        self.service = build("gmail", "v1", credentials=creds)

    def _extract_forwarded_from(self, gmail_msg: dict) -> str:
        payload = gmail_msg.get("payload", {})
        headers = payload.get("headers", [])
        for header in headers:
            name = header.get("name", "").lower()
            value = header.get("value", "").lower()
            if name in ["x-forwarded-to", "x-original-from"]:
                match = re.search(r'[\w\.-]+@[\w\.-]+', value)
                if match:
                    return match.group(0)

        full_text = self._get_full_body_text(payload)
        match = re.search(r"from:.*?([\w\.-]+@[\w\.-]+)", full_text, re.IGNORECASE)
        if match:
            return match.group(1).lower()

        for header in headers:
            name = header.get("name", "").lower()
            value = header.get("value", "").lower()
            if name == "from":
                match = re.search(r'[\w\.-]+@[\w\.-]+', value)
                if match:
                    return match.group(0)
        return ""

    def get_attachments_in_memory(self, user_id, msg_id, payload):
        attachments = []
        
        parts = [payload] if "parts" not in payload else payload["parts"]
        queue = parts[:]
        
        while queue:
            part = queue.pop(0)
            
            if part.get("filename"):
                filename = part["filename"]
                attachment_id = part["body"].get("attachmentId")
                data = part["body"].get("data")
                
                if attachment_id:
                    attachment = self.service.users().messages().attachments().get( # type: ignore
                        userId=user_id, messageId=msg_id, id=attachment_id).execute()
                    data = attachment.get("data")
                
                if data:
                    file_data = base64.urlsafe_b64decode(data.encode('UTF-8'))
                    # Store the bytes directly instead of writing to disk!
                    attachments.append({"filename": filename, "data": file_data})
                    
            if "parts" in part:
                queue.extend(part["parts"])
                
        return attachments

    def _get_body_text(self, payload):
        text = ""
        if 'body' in payload and 'data' in payload['body']:
            try:
                text = base64.urlsafe_b64decode(payload['body']['data']).decode('utf-8', errors='ignore')
                text = text.split('\n')[5]
            except:
                pass
        elif 'parts' in payload:
            for part in payload['parts']:
                text += self._get_body_text(part)
        return text

    def _get_full_body_text(self, payload):
        text = ""
        if 'body' in payload and 'data' in payload['body']:
            try:
                text = base64.urlsafe_b64decode(payload["body"]["data"]).decode("utf-8", errors="ignore")
            except:
                pass
        elif 'parts' in payload:
            for part in payload['parts']:
                text += self._get_full_body_text(part)
        return text

    def _detect_broker_from_email(self, msg):
        payload = msg.get("payload", {})
        body_text = self._get_full_body_text(payload).lower()
        if "zerodha" in body_text:
            return "Zerodha"
        if "groww" in body_text:
            return "Groww"
        if "angleone" in body_text or "angelone" in body_text:
            return "AngelOne"
        return None

    def _extract_nse_date(self, msg):
        payload = msg.get('payload', {})
        full_text = self._get_body_text(payload)
        
        match = re.search(r"for   (\d{1,2}-[A-Z]{3,}-\d{4})", full_text)
        if match:
            date_str = match.group(1)
            try:
                # Convert 13-MAR-2026 to 2026-03-13 for standard ISO format
                from datetime import datetime
                dt = datetime.strptime(date_str, "%d-%b-%Y")
                return dt.strftime("%Y-%m-%d")
            except:
                return date_str
        return None
        

    # ------------------------------------------------------------------
    # Core incremental processing
    # ------------------------------------------------------------------

    def _fetch_ipo_price(self, isin: str) -> float:
        with db_handler.get_session() as session:
            ipo_detail = session.exec(
                        select(IPO).where(
                            IPO.isin_code == isin
                        )
                    ).first()
            if ipo_detail:
                return ipo_detail.offer_price, ipo_detail.ipo_listing_date
        return 0, None # type: ignore

    def _save_transactions(self, extractions):
        if not extractions:
            return

        with db_handler.get_session() as session:
            for item in extractions:
                symbol = item.get("symbol")
                if not symbol:
                    continue
                try:
                    qty_val = float(str(item.get("quantity", 0)).replace(",", ""))
                    rate = float(str(item.get("rate", 0)).replace(",", ""))
                except ValueError:
                    continue

                trade_datetime = item.get("trade_datetime")
                if isinstance(trade_datetime, str):
                    try:
                        trade_datetime = datetime.fromisoformat(trade_datetime.replace("Z", "+00:00"))
                    except Exception:
                        try:
                            trade_datetime = datetime.strptime(trade_datetime, "%Y-%m-%d %H:%M:%S%z")
                        except Exception:
                            trade_datetime = datetime.now()
                elif not trade_datetime:
                    trade_datetime = datetime.now()
                
                if trade_datetime.tzinfo:
                    trade_datetime = trade_datetime.replace(tzinfo=None)

                # skip duplicates
                existing = session.exec(
                    select(Transaction).where(
                        Transaction.user_id == self.user_id,
                        Transaction.stock_symbol == symbol,
                        Transaction.transaction_type == item.get("transaction_type", "BUY").upper(),
                        Transaction.quantity == int(qty_val),
                        Transaction.price == rate
                    )
                ).all()

                found = False
                for ex in existing:
                    if abs((ex.transaction_datetime - trade_datetime).total_seconds()) < 86400:
                        found = True
                        break

                if found:
                    continue

                transaction = Transaction(
                    transaction_id=str(uuid.uuid4()),
                    user_id=self.user_id,
                    transaction_datetime=trade_datetime,
                    stock_symbol=symbol,
                    stock_name=item.get("company_name", ""),
                    transaction_type=item.get("transaction_type", "BUY").upper(),
                    quantity=int(qty_val),
                    broker=item.get("broker", ""),
                    exchange=item.get("exchange", ""),
                    price=rate,
                    inferred=item.get("is_inferred", False)
                )
                session.add(transaction)
            session.commit()

    def _get_or_create_cas_status(self):
        with db_handler.get_session() as session:
            status = session.exec(select(CASStatus).where(CASStatus.user_id == self.user_id)).first()
            if not status:
                status = CASStatus(user_id=self.user_id, status="pending")
                session.add(status)
                session.commit()
            return status

    def _calculate_and_upsert_holdings(self):
        with db_handler.get_session() as session:
            transactions = session.exec(
                select(Transaction).where(Transaction.user_id == self.user_id).order_by(Transaction.transaction_datetime)
            ).all()

            holdings_cache = {}
            transactions_to_add = []

            for t in transactions:
                base_symbol = t.stock_symbol.split(".")[0]
                if base_symbol not in holdings_cache:
                    holdings_cache[base_symbol] = Holdings(
                        holding_id=str(uuid.uuid4()),
                        user_id=self.user_id,
                        stock_symbol=base_symbol,
                        company_name=t.stock_name,
                        quantity=0,
                        avg_buy=0.0,
                        realized_pl=0.0,
                        holding_datetime=t.transaction_datetime
                    )

                holding = holdings_cache[base_symbol]
                old_qty = holding.quantity
                old_rate = holding.avg_buy

                if t.transaction_type == "BUY":
                    new_qty = old_qty + t.quantity
                    new_rate = ((old_qty * old_rate) + (t.quantity * t.price)) / new_qty if new_qty > 0 else 0
                    holding.quantity = new_qty
                    holding.avg_buy = float(f"{new_rate:.2f}")
                elif t.transaction_type == "SELL":
                    new_qty = old_qty - t.quantity
                    
                    if old_qty == 0:
                        stock = session.exec(select(Stock).where((Stock.nse_symbol == base_symbol) | (Stock.bse_symbol == base_symbol))).first()
                        isin = stock.isin_code if stock else ""
                        ipo_price, listing_date = self._fetch_ipo_price(isin)
                        
                        if ipo_price > 0:
                            old_rate = ipo_price
                            holding.avg_buy = ipo_price
                            ipo_transaction = Transaction(
                                transaction_id=str(uuid.uuid4()),
                                user_id=self.user_id,
                                transaction_datetime=listing_date if listing_date else t.transaction_datetime,
                                stock_symbol=t.stock_symbol,
                                stock_name=t.stock_name,
                                transaction_type='BUY',
                                quantity=t.quantity,
                                broker=t.broker,
                                exchange=t.exchange,
                                price=ipo_price,
                                inferred=True
                            )
                            transactions_to_add.append(ipo_transaction)
                        else:
                            old_rate = t.price
                    
                    realized_pl = (t.price - old_rate) * t.quantity
                    t.realized_pl = float(f"{realized_pl:.2f}")
                    session.add(t)
                    holding.quantity = new_qty
                    holding.realized_pl = float(f"{(holding.realized_pl + realized_pl):.2f}")

                holding.holding_datetime = t.transaction_datetime

            for ipo_txn in transactions_to_add:
                session.add(ipo_txn)

            existing_holdings = session.exec(select(Holdings).where(Holdings.user_id == self.user_id)).all()
            for h in existing_holdings:
                session.delete(h)
            
            for h in holdings_cache.values():
                session.add(h)
            session.commit()

    def process_and_reconcile_from_cas(self, cas_holdings):
        if not cas_holdings:
            return

        with db_handler.get_session() as session:
            transactions = session.exec(
                select(Transaction).where(Transaction.user_id == self.user_id).order_by(Transaction.transaction_datetime)
            ).all()

            extractions = []
            for t in transactions:
                base_symbol = t.stock_symbol.split(".")[0]
                stock = session.exec(select(Stock).where((Stock.nse_symbol == base_symbol) | (Stock.bse_symbol == base_symbol))).first()
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

        inferred_txns = self._fill_missing_transactions(extractions, cas_holdings)
        self._save_transactions(inferred_txns)
        self._calculate_and_upsert_holdings()

        with db_handler.get_session() as session:
            cas_status = session.exec(select(CASStatus).where(CASStatus.user_id == self.user_id)).first()
            if not cas_status:
                cas_status = CASStatus(user_id=self.user_id, status="processed")
            else:
                cas_status.status = "processed"
                cas_status.last_processed_at = datetime.now()
            session.add(cas_status)
            session.commit()

    def process_transactions(self, message_id: str):
        """Process email transactions.

        Args:
            message_id (str): The ID of the email message to process.
        """

        try:
            msg = self.service.users().messages().get(userId="me", id=message_id).execute() # type: ignore
        except Exception as e:
            print(f"Error fetching message {message_id}: {e}")
            return {'status': 500, 'message': f"Error fetching message: {e}"}
        
        user_email = msg.get("payload", {}).get("headers", [])
        user_email = next((h.get("value") for h in user_email if h.get("name", "").lower() == "from"), None)

        attachments = self.get_attachments_in_memory("me", msg['id'], msg.get("payload", {}))
        sender = self._extract_forwarded_from(msg)

        if not sender:
            print(f"Could not determine sender for message ID: {message_id}")
            return {'status': 500, 'message': f"Could not determine sender for message ID: {message_id}"}
        
        #extract user email from message headers
        if user_email and "<" in user_email and ">" in user_email:
            match = re.search(r'<(.*?)>', user_email)
            if match:
                user_email = match.group(1).lower()

        if user_email:
            with db_handler.get_session() as session:
                user = session.exec(select(User).where(User.email == user_email.lower())).first()
                if user:
                    self.user_id = user.user_id
                    self._get_user_details(self.user_id)
                else:
                    print(f"No user found for email: {user_email}")
                    return {'status': 202, 'message': f"No user found for email: {user_email}"}
                
        print (f"Email from: {sender}, User email: {user_email}, Attachments found: {len(attachments)}")

        email_sent_time = msg.get("internalDate")
        email_sent_date = datetime.fromtimestamp(int(email_sent_time) / 1000, tz=timezone.utc).strftime("%Y-%m-%d")
        broker_name = self._detect_broker_from_email(msg)
        if attachments:
            for att in attachments:
                if str(att["filename"]).lower().endswith(".pdf"):
                    if sender == 'ecas@cdslstatement.com':
                        holdings = self.extractor.extract_cas_pdf(att["data"], self.PASSWORD)
                        self.process_and_reconcile_from_cas(holdings)
                    elif sender in ['nse-direct@nse.co.in']:
                        email_date = self._extract_nse_date(msg)
                        if email_date is None:
                            email_date = email_sent_date
                        extractions = self.extractor.extract_nse_pdf(att["data"], self.PASSWORD, email_date)
                        if extractions is not None:
                            self._save_transactions(extractions)
                    elif sender == 'mgrpt@bseindia.com':
                        extractions = self.extractor.extract_bse_pdf(att["data"], self.PASSWORD, broker_name)
                        if extractions is not None:
                            self._save_transactions(extractions)
                    else:
                        print(f"Unknown sender: {sender}, skipping.")

            return {'status': 200, 'message': "Processing completed."}
        else:
            print(f"No attachments found in message ID: {message_id}")
            return {'status': 202, 'message': "No attachments found."}

    def _fill_missing_transactions(self, extractions, cas_holdings):
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


                    
    # def fetch_incremental_emails(self, after_timestamp_sec: int):
    # ...
