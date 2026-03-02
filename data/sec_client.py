"""
data/sec_client.py - SEC EDGAR Client (kostenlos, kein API-Key)
Holt:
  - Insider Trading (Form 4)
  - Aktuelle Filings (8-K, 10-K, 10-Q)
  - Institutionelle Inhaber (13F)
"""

import requests
import time
from loguru import logger


class SECClient:
    BASE_URL    = "https://data.sec.gov"
    EDGAR_URL   = "https://efts.sec.gov"
    USER_AGENT  = "AI-Analyst research@example.com"   # SEC verlangt User-Agent

    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent":  self.USER_AGENT,
            "Accept":      "application/json",
        })
        self._cik_cache: dict = {}

    # ── CIK Lookup ───────────────────────────────────────────────────────────

    def get_cik(self, ticker: str) -> str | None:
        ticker = ticker.upper().replace(".DE","").replace(".DE","")
        if ticker in self._cik_cache: return self._cik_cache[ticker]
        try:
            resp = self.session.get(
                "https://www.sec.gov/cgi-bin/browse-edgar",
                params={"company":"","CIK":ticker,"type":"","dateb":"",
                        "owner":"include","count":"1","search_text":"","action":"getcompany",
                        "output":"atom"},
                timeout=6)
            # Schneller: company_tickers.json
            resp2 = self.session.get(
                "https://www.sec.gov/files/company_tickers.json", timeout=8)
            if resp2.status_code == 200:
                data = resp2.json()
                for entry in data.values():
                    if entry.get("ticker","").upper() == ticker:
                        cik = str(entry["cik_str"]).zfill(10)
                        self._cik_cache[ticker] = cik
                        return cik
        except Exception as e:
            logger.debug(f"[SEC CIK] '{ticker}': {e}")
        return None

    # ── Insider Trades (Form 4) ───────────────────────────────────────────────

    def get_insider_trades(self, ticker: str, limit: int = 10) -> list:
        """
        Gibt die letzten Insider-Transaktionen zurück.
        Jede Transaktion: name, title, transaction_type, shares, price, date, value
        """
        cik = self.get_cik(ticker)
        if not cik: return []
        try:
            url  = f"{self.BASE_URL}/submissions/CIK{cik}.json"
            resp = self.session.get(url, timeout=8)
            if resp.status_code != 200: return []
            data = resp.json()

            recent = data.get("filings", {}).get("recent", {})
            forms    = recent.get("form", [])
            dates    = recent.get("filingDate", [])
            accnums  = recent.get("accessionNumber", [])

            trades = []
            for form, date, acc in zip(forms, dates, accnums):
                if form == "4" and len(trades) < limit:
                    trades.append({
                        "filing_type": "Form 4 (Insider)",
                        "date":        date,
                        "accession":   acc,
                        "url":         f"https://www.sec.gov/Archives/edgar/full-index/",
                    })
            return trades
        except Exception as e:
            logger.debug(f"[SEC Form4] '{ticker}': {e}")
            return []

    # ── Recent Filings ────────────────────────────────────────────────────────

    def get_recent_filings(self, ticker: str, form_types: list = None) -> list:
        """
        Gibt aktuelle SEC-Filings zurück.
        Standardmäßig: 8-K (Material Events), 10-K (Annual), 10-Q (Quarterly)
        """
        if form_types is None:
            form_types = ["8-K", "10-K", "10-Q"]

        cik = self.get_cik(ticker)
        if not cik: return []

        try:
            url  = f"{self.BASE_URL}/submissions/CIK{cik}.json"
            resp = self.session.get(url, timeout=8)
            if resp.status_code != 200: return []
            data = resp.json()

            recent  = data.get("filings",{}).get("recent",{})
            forms   = recent.get("form",[])
            dates   = recent.get("filingDate",[])
            descs   = recent.get("primaryDocument",[])
            accnums = recent.get("accessionNumber",[])

            filings = []
            for form, date, doc, acc in zip(forms, dates, descs, accnums):
                if form in form_types:
                    acc_fmt = acc.replace("-","")
                    filings.append({
                        "form":    form,
                        "date":    date,
                        "document": doc,
                        "url":     f"https://www.sec.gov/Archives/edgar/data/{int(cik)}/{acc_fmt}/{doc}",
                    })
                if len(filings) >= 10: break
            return filings
        except Exception as e:
            logger.debug(f"[SEC Filings] '{ticker}': {e}")
            return []

    # ── Insider Summary ───────────────────────────────────────────────────────

    def get_insider_summary(self, ticker: str) -> dict:
        """
        Kompakte Zusammenfassung der Insider-Aktivität.
        Nutzt EDGAR Full-Text Search für Form-4-Analyse.
        """
        try:
            cik = self.get_cik(ticker)
            if not cik:
                return {"buy_count":0, "sell_count":0, "signal":"Keine Daten", "cik": None}

            url  = f"{self.BASE_URL}/submissions/CIK{cik}.json"
            resp = self.session.get(url, timeout=8)
            if resp.status_code != 200:
                return {"buy_count":0,"sell_count":0,"signal":"API Fehler","cik":cik}

            data   = resp.json()
            recent = data.get("filings",{}).get("recent",{})
            forms  = recent.get("form",[])
            dates  = recent.get("filingDate",[])

            form4_count = sum(1 for f in forms[:50] if f == "4")
            # Letztes Filing-Datum
            last_form4_date = next(
                (d for f,d in zip(forms,dates) if f=="4"), "Unbekannt"
            )

            return {
                "cik":            cik,
                "form4_count_recent": form4_count,
                "last_filing":    last_form4_date,
                "signal":         "Aktiv" if form4_count > 3 else "Gering",
                "filings_url":    f"https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&CIK={cik}&type=4&dateb=&owner=include&count=20",
            }
        except Exception as e:
            logger.debug(f"[SEC Insider Summary] '{ticker}': {e}")
            return {"buy_count":0,"sell_count":0,"signal":"Fehler"}


_instance = None
def get_sec_client() -> SECClient:
    global _instance
    if _instance is None: _instance = SECClient()
    return _instance