import re

def normalize_text(s):
    if not s:
        return ""
    s = " ".join(s.split())
    if "ISIN" in s.upper() or "IISSIINN" in s.upper(): return "ISIN"
    if "SECURITY" in s.upper() or "SSEECCUURRIITTYY" in s.upper(): return "SECURITY"
    if "TRANSACTION" in s.upper() or "TTRRAANNSSAACCTTIIOONN" in s.upper(): return "TRANSACTION"
    if "PARTICULARS" in s.upper() or "PPAARRTTIICCUULLAARRSS" in s.upper(): return "PARTICULARS"
    if "TTRRAANNSSAACCTTIIOONN\nPPAARRTTIICCUULLAARRSS" in s.upper().replace(" ", ""): return "TRANSACTION PARTICULARS"
    return s


def parse_number(s):
    if not s:
        return 0.0
    s = normalize_text(s).replace(",", "")
    if s in ("--", "", "-", "n.a"):
        return 0.0
    m = re.search(r"(-?\d+\.?\d*)", s)
    if m:
        try:
            return float(m.group(1))
        except ValueError:
            return 0.0
    return 0.0

class CASParser:
    def __init__(self, doc):
        self.doc = doc
        self.data = {
            "statement_date": None,
            "zerodha": [],
            "groww": [],
            "mutual_funds": [],
            "transactions": [],
            "other": []
        }
        self.seen_holdings_date_header = False
        self.BROKER_MAP = {
            "12081601": "ZERODHA",
            "12088701": "GROWW",
            "12033203": "ANGELONE",
            "12095500": "INDSTOCKS",
            "ZERODHA": "ZERODHA",
            "GROWW": "GROWW",
            "ANGEL ONE": "ANGELONE",
            "INDSTOCKS": "INDSTOCKS",
        }
        self.active_broker = None

    def get_broker_from_text(self, text):
        text_upper = text.upper()
        # Pattern: BO ID
        m_bo = re.search(r"BO ID\s*:\s*(\d{8})", text_upper)
        if m_bo:
            dp_id = m_bo.group(1)
            if dp_id in self.BROKER_MAP:
                return self.BROKER_MAP[dp_id]
        # Pattern: DP Name
        m_dp = re.search(r"DP NAME\s*:\s*(.*?)(BO ID|STATEMENT|$)", text_upper, re.DOTALL)
        if m_dp:
            dp_str = m_dp.group(1).strip()
            for key, name in self.BROKER_MAP.items():
                if not key.isdigit() and key in dp_str:
                    return name
        # Fallback Keywords
        for key, name in self.BROKER_MAP.items():
            if key in text_upper:
                return name
        return None

    def get_date_from_text(self, text):
        """Search for 'HOLDING STATEMENT AS ON [DATE]'."""
        m = re.search(r"HOLDING STATEMENT AS ON\s*(\d{2}-\d{2}-\d{4})", text, re.I)
        if m:
            self.seen_holdings_date_header = True
            return m.group(1)
        return None

    def is_transactions_header(self, row):
        if len(row) < 7: return False
        cell0 = normalize_text(row[2]).upper()
        return "TRANSACTION" in cell0 or "PARTICULARS" in cell0 or "TRANSACTION PARTICULARS" in cell0

    def is_holdings_header(self, row):
        if not self.seen_holdings_date_header: return False
        if len(row) < 7: return False
        cell0 = normalize_text(row[0]).upper()
        cell1 = normalize_text(row[1]).upper()
        return "ISIN" in cell0 and "SECURITY" in cell1

    def is_mf_header(self, row):
        if len(row) < 7: return False
        cell0 = normalize_text(row[0]).upper()
        return "SCHEME NAME" in cell0 or "MUTUAL FUND UNITS" in cell0

    def parse_holdings_row(self, row, broker):
        isin = normalize_text(row[0])
        if not re.match(r"^[A-Z]{2}[A-Z0-9]{10}$", isin):
            return None
        return {
            "isin": isin,
            "security": normalize_text(row[1]) if len(row) > 1 else "",
            "current_bal": parse_number(row[2]) if len(row) > 2 else 0.0,
            "frozen_bal": parse_number(row[3]) if len(row) > 3 else 0.0,
            "pledge_bal": parse_number(row[4]) if len(row) > 4 else 0.0,
            "free_bal": parse_number(row[6]) if len(row) > 6 else 0.0,
            "market_price": parse_number(row[7]) if len(row) > 7 else 0.0,
            "value": parse_number(row[8]) if len(row) > 8 else 0.0,
            "broker": broker or "UNKNOWN"
        }

    def parse_mf_row(self, row):
        name = normalize_text(row[0])
        isin = normalize_text(row[1]) if len(row) > 1 else ""
        if not name or name.upper() in ("GRAND TOTAL", "SCHEME NAME") or not isin:
            return None
        if not re.match(r"^[A-Z]{2}[A-Z0-9]{10}$", isin):
            return None
        return {
            "scheme_name": name,
            "isin": isin,
            "folio_no": normalize_text(row[2]) if len(row) > 2 else "",
            "closing_units": parse_number(row[3]) if len(row) > 3 else 0.0,
            "nav": parse_number(row[4]) if len(row) > 4 else 0.0,
            "valuation": parse_number(row[6]) if len(row) > 6 else 0.0,
        }

    def run(self):
        for page_num, page in enumerate(self.doc):
            markers = []
            blocks = page.get_text("blocks")
            for b in blocks:
                text = b[4]
                # Broker Detection
                broker = self.get_broker_from_text(text)
                if broker:
                    markers.append({"y": b[1], "type": "broker", "value": broker})
                # Date Detection
                h_date = self.get_date_from_text(text)
                if h_date:
                    self.data["statement_date"] = h_date
            
            markers.sort(key=lambda x: x['y'])

            tables = page.find_tables()
            if not tables:
                continue

            sorted_tables = sorted(tables, key=lambda t: t.bbox[1])

            for table in sorted_tables:
                t_y = table.bbox[1]
                
                # Update active broker if we passed a marker
                for m in markers:
                    if m['type'] == "broker" and m['y'] < t_y:
                        self.active_broker = m['value']
                
                rows = table.extract()
                if not rows: continue
                
                is_holdings, is_mf, is_transactions = False, False, False

                for row in rows:
                    if not any(row): continue
                    if self.is_transactions_header(row):
                        is_transactions, is_holdings, is_mf = True, False, False
                        continue
                    if self.is_holdings_header(row):
                        is_holdings, is_mf, is_transactions = True, False, False
                        continue
                    if self.is_mf_header(row):
                        is_mf, is_holdings, is_transactions = True, False, False
                        continue

                    

                    if is_transactions:
                        continue

                    if is_holdings:
                        if "Portfolio Value" in "".join(str(c) for c in row if c):
                            is_holdings = False
                            continue
                        parsed = self.parse_holdings_row(row, self.active_broker)
                        if parsed:
                            key = self.active_broker.lower() if self.active_broker else "other"
                            if key in self.data:
                                if not any(h['isin'] == parsed['isin'] for h in self.data[key]):
                                    self.data[key].append(parsed)
                            else:
                                if "other" not in self.data: self.data["other"] = []
                                if not any(h['isin'] == parsed['isin'] for h in self.data["other"]):
                                    self.data["other"].append(parsed)

                    elif is_mf:
                        if "Grand Total" in str(row[0]):
                            is_mf = False
                            continue
                        parsed = self.parse_mf_row(row)
                        if parsed:
                            if not any(m['isin'] == parsed['isin'] for m in self.data['mutual_funds']):
                                self.data["mutual_funds"].append(parsed)

        return self.data

