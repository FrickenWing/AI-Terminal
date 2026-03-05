import os
import time
import requests
import pandas as pd
import sqlite3
from loguru import logger
from dotenv import load_dotenv
from openbb import obb # Das Herzstück

load_dotenv()

class OpenBBClient:
    """
    Der God-Mode OpenBB Hub. 
    Kombiniert OpenBB (Charts, Makro, Fundamentals) mit 
    Finnhub (Analysten, Sentiment, Social Media).
    """
    def __init__(self):
        self.finnhub_key = os.getenv("FINNHUB_API_KEY", "")
        
        # Pfad zur lokalen Asset-DB für die Ticker-Übersetzung
        base_dir = os.path.dirname(os.path.abspath(__file__))
        self.db_path = os.path.join(base_dir, "master_assets.db")

    def _resolve_ticker(self, ticker: str) -> str:
        """Übersetzt den Ticker für OpenBB (z.B. hängt '.DE' an für europäische Werte)."""
        ticker = ticker.upper()
        if not os.path.exists(self.db_path):
            return ticker
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT full_ticker FROM assets WHERE symbol = ? OR full_ticker = ? LIMIT 1", (ticker, ticker))
                row = cursor.fetchone()
                return row[0] if row else ticker
        except:
            return ticker

    # ------------------------------------------------------------------------
    # 1. KERN-DATEN (Preise, Charts, Makro via OpenBB)
    # ------------------------------------------------------------------------

    def get_price_history(self, ticker: str, period: str = "1y"):
        full_ticker = self._resolve_ticker(ticker)
        try:
            df = obb.equity.price.historical(symbol=full_ticker, provider="yfinance").to_df()
            if not df.empty:
                df.columns = [str(c).lower() for c in df.columns]
                return df, "OpenBB"
        except Exception as e:
            logger.debug(f"OpenBB History fehlgeschlagen für {ticker}: {e}")

        if self.finnhub_key:
            try:
                end_ts = int(time.time())
                start_ts = end_ts - (365 * 24 * 3600) 
                url = f"https://finnhub.io/api/v1/stock/candle?symbol={ticker}&resolution=D&from={start_ts}&to={end_ts}&token={self.finnhub_key}"
                resp = requests.get(url, timeout=5).json()
                if resp.get("s") == "ok":
                    df = pd.DataFrame({
                        "open": resp["o"], "high": resp["h"], "low": resp["l"],
                        "close": resp["c"], "volume": resp["v"]
                    }, index=pd.to_datetime(resp["t"], unit="s"))
                    df.index.name = "date"
                    return df.sort_index(ascending=True), "Finnhub"
            except: pass
        return pd.DataFrame(), "Error"

    def get_quote(self, ticker: str) -> dict:
        full_ticker = self._resolve_ticker(ticker)
        try:
            res_df = obb.equity.price.quote(symbol=full_ticker, provider="yfinance").to_df()
            if not res_df.empty:
                row = res_df.iloc[0]
                return {
                    "price": row.get("last_price", 0),
                    "change": row.get("change", 0),
                    "change_pct": (row.get("percent_change", 0) / 100) if row.get("percent_change") else 0,
                    "name": full_ticker,
                    "source": "OpenBB"
                }
        except: pass
            
        if self.finnhub_key:
            try:
                url = f"https://finnhub.io/api/v1/quote?symbol={ticker.split('.')[0]}&token={self.finnhub_key}"
                resp = requests.get(url, timeout=3).json()
                if "c" in resp and resp["c"] != 0:
                    return {
                        "price": resp.get("c", 0),
                        "change": resp.get("d", 0),
                        "change_pct": (resp.get("dp", 0) / 100) if resp.get("dp") else 0,
                        "name": ticker,
                        "source": "Finnhub"
                    }
            except: pass
        return {"price": 0, "change": 0, "change_pct": 0, "name": ticker, "source": "None"}

    def get_macro_data(self, series_id: str = "FEDFUNDS"):
        try:
            return obb.economy.fred_series(symbol=series_id).to_df()
        except:
            return pd.DataFrame()

    def get_company_metrics(self, ticker: str):
        try:
            return obb.equity.fundamental.metrics(symbol=self._resolve_ticker(ticker), provider="yfinance").to_df()
        except:
            return pd.DataFrame()

    def search_ticker(self, query: str) -> list:
        """
        Premium-Suche: Sucht lokal nach ISIN/Name/Ticker. 
        Wenn nicht genug gefunden wird, greift die Live-Suche von Finnhub für 100% globale Abdeckung (Asien, Kanada, ETFs etc.).
        """
        results = []
        q = f"%{query.upper()}%"
        
        # 1. Lokale DB Suche (Rasend schnell, enthält ISIN & Börse)
        if os.path.exists(self.db_path):
            try:
                with sqlite3.connect(self.db_path) as conn:
                    conn.row_factory = sqlite3.Row
                    cursor = conn.cursor()
                    # Suche in Ticker, Name oder ISIN
                    cursor.execute("""
                        SELECT full_ticker, name, type, isin, exchange_code 
                        FROM assets 
                        WHERE full_ticker LIKE ? OR name LIKE ? OR isin LIKE ? 
                        LIMIT 8
                    """, (q, q, q))
                    
                    for r in cursor.fetchall():
                        results.append({
                            "ticker": r["full_ticker"], 
                            "name": r["name"], 
                            "type": r["type"],
                            "isin": r["isin"] if r["isin"] else "",
                            "exchange": r["exchange_code"] if r["exchange_code"] else "US"
                        })
            except Exception as e:
                logger.error(f"DB Search Error: {e}")

        # 2. FINNHUB LIVE-SUCHE (Der God-Mode Fallback für exotische Märkte & ETFs)
        if len(results) < 5 and self.finnhub_key:
            try:
                url = f"https://finnhub.io/api/v1/search?q={query}&token={self.finnhub_key}"
                resp = requests.get(url, timeout=3).json()
                if "result" in resp:
                    for item in resp["result"]:
                        # Vermeide Duplikate, falls wir den Ticker schon lokal gefunden haben
                        if not any(r["ticker"] == item["symbol"] for r in results):
                            results.append({
                                "ticker": item["symbol"],
                                "name": item["description"],
                                "type": item["type"],
                                "isin": "", # Finnhub Live liefert keine ISIN in der Kurzübersicht
                                "exchange": "Global / " + (item["type"] or "Asset")
                            })
            except Exception as e:
                logger.debug(f"Finnhub Search Error: {e}")

        # Gib maximal die besten 10 Ergebnisse zurück
        return results[:10]

    # ------------------------------------------------------------------------
    # 2. ALTERNATIVE DATEN (Analysten & Sentiment via FINNHUB) -> NEU!
    # ------------------------------------------------------------------------

    def get_analyst_ratings(self, ticker: str) -> dict:
        """Holt die aktuellen Wallstreet Analysten-Empfehlungen (Buy/Hold/Sell)."""
        if not self.finnhub_key: return {}
        try:
            # Finnhub braucht den reinen US Ticker, ohne Suffixe wie .DE
            clean_ticker = ticker.split('.')[0]
            url = f"https://finnhub.io/api/v1/stock/recommendation?symbol={clean_ticker}&token={self.finnhub_key}"
            resp = requests.get(url, timeout=3).json()
            if resp and isinstance(resp, list) and len(resp) > 0:
                # Wir nehmen den aktuellsten Monat (Index 0)
                latest = resp[0]
                return {
                    "strongBuy": latest.get("strongBuy", 0),
                    "buy": latest.get("buy", 0),
                    "hold": latest.get("hold", 0),
                    "sell": latest.get("sell", 0),
                    "strongSell": latest.get("strongSell", 0)
                }
        except Exception as e:
            logger.warning(f"Konnte Analysten-Ratings für {ticker} nicht laden: {e}")
        return {}

    def get_news_sentiment(self, ticker: str) -> dict:
        """Analysiert die Stimmung (bullish/bearish) der Nachrichten der letzten Woche."""
        if not self.finnhub_key: return {}
        try:
            clean_ticker = ticker.split('.')[0]
            url = f"https://finnhub.io/api/v1/news-sentiment?symbol={clean_ticker}&token={self.finnhub_key}"
            resp = requests.get(url, timeout=3).json()
            if "sentiment" in resp:
                return {
                    "bullishPercent": resp["sentiment"].get("bullishPercent", 0.5),
                    "bearishPercent": resp["sentiment"].get("bearishPercent", 0.5),
                    "score": resp.get("companyNewsScore", 0.5) # 0 bis 1 (1 ist super positiv)
                }
        except: pass
        return {}

    def get_insider_sentiment(self, ticker: str) -> dict:
        """Prüft, ob Vorstände (Insider) aktuell eher kaufen oder verkaufen."""
        if not self.finnhub_key: return {}
        try:
            clean_ticker = ticker.split('.')[0]
            url = f"https://finnhub.io/api/v1/stock/insider-sentiment?symbol={clean_ticker}&from=2024-01-01&to=2026-12-31&token={self.finnhub_key}"
            resp = requests.get(url, timeout=3).json()
            if "data" in resp and len(resp["data"]) > 0:
                # Summiere die "MSPR" (Management Sentiment Score) der letzten Monate
                total_score = sum([item.get("mspr", 0) for item in resp["data"]])
                avg_score = total_score / len(resp["data"])
                return {
                    "score": avg_score,
                    "trend": "Kaufen" if avg_score > 0 else ("Verkaufen" if avg_score < 0 else "Neutral")
                }
        except: pass
        return {}

_client = None
def get_client():
    global _client
    if not _client: _client = OpenBBClient()
    return _client