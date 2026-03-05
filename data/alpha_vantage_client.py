import os
import requests
import pandas as pd
import io
from loguru import logger
from dotenv import load_dotenv

load_dotenv()

class AlphaVantageClient:
    """
    Der ultimative Fundamental- & Daten-Spezialist (Worker).
    Liefert Bilanzen, Dividenden, Intraday-Kurse, News, Insider-Trades 
    und technische Indikatoren exakt nach offizieller Alpha Vantage API-Dokumentation.
    """
    def __init__(self):
        self.api_key = os.getenv("ALPHAVANTAGE_API_KEY", "")
        self.base_url = "https://www.alphavantage.co/query"
        
        if not self.api_key:
            logger.warning("Kein ALPHAVANTAGE_API_KEY gefunden. Hole dir einen kostenlosen Key auf alphavantage.co!")

    def _clean_symbol(self, symbol: str) -> str:
        """Hilfsfunktion: Entfernt Suffixe (wie .DE) für US-Fokus-Abfragen."""
        if not symbol: return ""
        return symbol.split('.')[0]

    def _fetch_json(self, function: str, symbol: str = None, extra_params: dict = None) -> dict:
        """Zentrale Hilfsfunktion für JSON API-Aufrufe an Alpha Vantage."""
        if not self.api_key:
            return {}
            
        params = {"function": function, "apikey": self.api_key}
        if symbol:
            params["symbol"] = symbol
        if extra_params:
            params.update(extra_params)
            
        try:
            response = requests.get(self.base_url, params=params, timeout=15)
            response.raise_for_status()
            data = response.json()
            
            # API Limit Check (Free Tier: 25 Requests/Tag)
            if "Information" in data and "rate limit" in data.get("Information", "").lower():
                logger.error(f"Alpha Vantage Rate Limit erreicht bei {function}!")
                return {}
            if "Note" in data and "API call frequency" in data.get("Note", ""):
                logger.warning(f"Alpha Vantage Frequenz-Limit erreicht bei {function}. Kurz warten...")
                
            return data
        except Exception as e:
            logger.error(f"Alpha Vantage Fehler bei {function} für {symbol}: {e}")
            return {}

    def _fetch_csv(self, function: str, symbol: str = None, extra_params: dict = None) -> pd.DataFrame:
        """Spezial-Funktion für Endpunkte, die standardmäßig CSV liefern (z.B. Earnings Calendar)."""
        if not self.api_key:
            return pd.DataFrame()
            
        params = {"function": function, "apikey": self.api_key, "datatype": "csv"}
        if symbol: params["symbol"] = symbol
        if extra_params: params.update(extra_params)
            
        try:
            response = requests.get(self.base_url, params=params, timeout=15)
            response.raise_for_status()
            # Prüfe, ob versehentlich ein JSON mit Limit-Fehler zurückkam
            if "{" in response.text[:10]:
                return pd.DataFrame()
            return pd.read_csv(io.StringIO(response.text))
        except Exception as e:
            logger.error(f"Alpha Vantage CSV Fehler bei {function}: {e}")
            return pd.DataFrame()

    # -------------------------------------------------------------------
    # 1. CORE & MARKET DATA (Search, Quote, Intraday/Daily)
    # -------------------------------------------------------------------

    def get_market_status(self) -> dict:
        """Market Opening Status: Prüft, ob globale Börsen offen oder geschlossen sind."""
        data = self._fetch_json("MARKET_STATUS")
        return {"markets": data.get("markets", [])}

    def search_symbol(self, keywords: str) -> pd.DataFrame:
        """SearchBox: Sucht nach Tickersymbolen und Firmennamen."""
        data = self._fetch_json("SYMBOL_SEARCH", extra_params={"keywords": keywords})
        if "bestMatches" in data:
            return pd.DataFrame(data["bestMatches"])
        return pd.DataFrame()

    def get_global_quote(self, symbol: str) -> dict:
        """Quote Endpoint: Holt den aktuellsten Preis, Volumen und Tages-High/Low."""
        data = self._fetch_json("GLOBAL_QUOTE", symbol=self._clean_symbol(symbol))
        return data.get("Global Quote", {})

    def get_intraday_data(self, symbol: str, interval: str = "5min") -> pd.DataFrame:
        """Intraday Stocks: Liefert hochauflösende untertägige Kerzen (1min, 5min, 15min, 30min, 60min)."""
        data = self._fetch_json("TIME_SERIES_INTRADAY", symbol=self._clean_symbol(symbol), extra_params={"interval": interval})
        key = f"Time Series ({interval})"
        if key in data:
            df = pd.DataFrame.from_dict(data[key], orient="index")
            df.index = pd.to_datetime(df.index)
            # Spaltennamen bereinigen (z.B. "1. open" -> "open")
            df.columns = [col.split(" ")[1] for col in df.columns]
            return df.sort_index(ascending=True)
        return pd.DataFrame()

    def get_daily_data(self, symbol: str) -> pd.DataFrame:
        """Daily Stocks: Liefert die historische Tages-Historie der Aktie."""
        data = self._fetch_json("TIME_SERIES_DAILY", symbol=self._clean_symbol(symbol))
        if "Time Series (Daily)" in data:
            df = pd.DataFrame.from_dict(data["Time Series (Daily)"], orient="index")
            df.index = pd.to_datetime(df.index)
            df.columns = [col.split(" ")[1] for col in df.columns]
            return df.sort_index(ascending=True)
        return pd.DataFrame()

    # -------------------------------------------------------------------
    # 2. FUNDAMENTALS (Overview, Bilanzen, Cashflow, Income, Dividends)
    # -------------------------------------------------------------------

    def get_company_overview(self, symbol: str) -> dict:
        """Company Overview: Massives Datenpaket inkl. EBITDA, KGV, Margen, Branche."""
        return self._fetch_json("OVERVIEW", symbol=self._clean_symbol(symbol))

    def get_income_statement(self, symbol: str) -> pd.DataFrame:
        """Income Statements: Gewinn- und Verlustrechnung (GuV)."""
        data = self._fetch_json("INCOME_STATEMENT", symbol=self._clean_symbol(symbol))
        if "annualReports" in data:
            return pd.DataFrame(data["annualReports"])
        return pd.DataFrame()

    def get_balance_sheet(self, symbol: str) -> pd.DataFrame:
        """Balance Sheets: Komplette Bilanz (Assets, Liabilities)."""
        data = self._fetch_json("BALANCE_SHEET", symbol=self._clean_symbol(symbol))
        if "annualReports" in data:
            return pd.DataFrame(data["annualReports"])
        return pd.DataFrame()

    def get_cash_flow(self, symbol: str) -> pd.DataFrame:
        """Cash Flow: Cashflow-Statements (Operating, Investing, Financing)."""
        data = self._fetch_json("CASH_FLOW", symbol=self._clean_symbol(symbol))
        if "annualReports" in data:
            return pd.DataFrame(data["annualReports"])
        return pd.DataFrame()

    def get_dividends(self, symbol: str) -> pd.DataFrame:
        """Dividends: Historische Dividendenzahlungen des Unternehmens."""
        data = self._fetch_json("DIVIDENDS", symbol=self._clean_symbol(symbol))
        if "data" in data:
            return pd.DataFrame(data["data"])
        return pd.DataFrame()

    # -------------------------------------------------------------------
    # 3. EARNINGS & ESTIMATIONS (Vergangenheit & Zukunft)
    # -------------------------------------------------------------------

    def get_earnings(self, symbol: str) -> dict:
        """Earning Estimations & History: Vierteljährliche und jährliche EPS (Schätzung vs. Realität)."""
        return self._fetch_json("EARNINGS", symbol=self._clean_symbol(symbol))

    def get_earnings_calendar(self, symbol: str = None, horizon: str = "3month") -> pd.DataFrame:
        """Earning Calls Calendar: Wann veröffentlichen Unternehmen ihre nächsten Zahlen? (Liefert CSV)"""
        params = {"horizon": horizon}
        return self._fetch_csv("EARNINGS_CALENDAR", symbol=self._clean_symbol(symbol), extra_params=params)

    # -------------------------------------------------------------------
    # 4. ALTERNATIVE & ETF DATA (News, Sentiment, Insider)
    # -------------------------------------------------------------------

    def get_news_and_sentiment(self, tickers: str = None) -> list:
        """Market News & Sentiment: Holt Markt-News & KI-basiertes Sentiment (Bullish/Bearish)."""
        params = {}
        if tickers:
            params["tickers"] = tickers
        data = self._fetch_json("NEWS_SENTIMENT", extra_params=params)
        return data.get("feed", [])

    def get_insider_transactions(self, symbol: str) -> pd.DataFrame:
        """Insider Trading: Käufe/Verkäufe vom Management (SEC Form 4)."""
        data = self._fetch_json("INSIDER_TRANSACTIONS", symbol=self._clean_symbol(symbol))
        if "data" in data:
            return pd.DataFrame(data["data"])
        return pd.DataFrame()

    def get_etf_profile(self, symbol: str) -> dict:
        """ETF Metrics: Spezifische ETF-Daten (Holdings, Sektor-Gewichtung, Kostenquote/TER)."""
        return self._fetch_json("ETF_PROFILE", symbol=self._clean_symbol(symbol))

    # -------------------------------------------------------------------
    # 5. TECHNICAL INDICATORS (SMA, RSI, MACD etc.)
    # -------------------------------------------------------------------

    def get_technical_indicator(self, symbol: str, indicator: str = "SMA", interval: str = "daily", time_period: int = 20) -> pd.DataFrame:
        """
        Technische Indikatoren: Universeller Endpunkt für SMA, EMA, RSI, MACD etc.
        """
        params = {
            "interval": interval,
            "time_period": time_period,
            "series_type": "close"
        }
        data = self._fetch_json(indicator.upper(), symbol=self._clean_symbol(symbol), extra_params=params)
        
        # Daten-Keys heißen z.B. "Technical Analysis: SMA" - Extraktion dynamisch
        data_key = next((key for key in data.keys() if "Technical Analysis" in key), None)
        if data_key:
            df = pd.DataFrame.from_dict(data[data_key], orient="index")
            df.index = pd.to_datetime(df.index)
            # Konvertiere Strings in Floats für bessere Weiterverarbeitung
            df = df.apply(pd.to_numeric, errors='coerce')
            return df.sort_index(ascending=True)
        return pd.DataFrame()


# Singleton-Pattern für einfachen Import in den Data Orchestrator
_client = None
def get_alpha_vantage_client():
    global _client
    if not _client: _client = AlphaVantageClient()
    return _client