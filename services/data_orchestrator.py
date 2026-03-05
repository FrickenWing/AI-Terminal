import os
import json
import time
import sqlite3
import concurrent.futures
from loguru import logger
from data.openbb_client import get_client as get_obb
from data.alpha_vantage_client import get_alpha_vantage_client as get_av

class DataOrchestrator:
    """
    Der 'General' des Terminals.
    Weist alle APIs (Finnhub, Alpha Vantage, OpenBB) an, sammelt die Daten
    und speichert sie dauerhaft für das KI-RAG in der lokalen Datenbank.
    """
    def __init__(self):
        self.obb = get_obb()
        self.av = get_av()
        
        base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        self.db_path = os.path.join(base_dir, "data", "master_assets.db")
        self._init_cache_db()

    def _init_cache_db(self):
        """Erstellt die RAG-Cache-Tabelle, falls sie nicht existiert."""
        try:
            with sqlite3.connect(self.db_path) as conn:
                conn.execute("""
                    CREATE TABLE IF NOT EXISTS orchestrator_cache (
                        symbol TEXT PRIMARY KEY,
                        data_json TEXT,
                        timestamp REAL
                    )
                """)
        except Exception as e:
            logger.error(f"Konnte Cache-DB nicht initialisieren: {e}")

    def get_full_profile(self, symbol: str) -> dict:
        """
        Der Master-Befehl: Holt ALLE verfügbaren Daten eines Assets.
        Prüft zuerst den Cache, um die Alpha Vantage API-Limits zu schonen!
        """
        # 1. RAG-Cache Prüfung (Sind die Daten jünger als 24 Stunden?)
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT data_json, timestamp FROM orchestrator_cache WHERE symbol=?", (symbol,))
                row = cursor.fetchone()
                # 86400 Sekunden = 24 Stunden
                if row and (time.time() - row[1]) < 86400:
                    logger.info(f"⚡ Lade komplettes Profil für {symbol} blitzschnell aus dem lokalen RAG-Cache!")
                    return json.loads(row[0])
        except Exception as e:
            logger.error(f"Cache Lese-Fehler: {e}")

        logger.info(f"🌍 Orchestrator startet globalen Live-Daten-Abruf für {symbol}...")
        
        # 2. Paralleler Abruf von den Workern (Finnhub & Alpha Vantage)
        # Wir limitieren auf 4 parallele Threads, damit die APIs nicht blockieren
        profile = {"symbol": symbol}
        
        try:
            with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
                # Alpha Vantage Tasks (Fundamentals)
                f_overview = executor.submit(self.av.get_company_overview, symbol)
                f_market = executor.submit(self.av.get_market_status)
                f_income = executor.submit(self.av.get_income_statement, symbol)
                f_cashflow = executor.submit(self.av.get_cash_flow, symbol)
                
                # Finnhub / OpenBB Tasks (Live & Sentiment)
                f_quote = executor.submit(self.obb.get_quote, symbol)
                f_analyst = executor.submit(self.obb.get_analyst_ratings, symbol)
                f_news = executor.submit(self.obb.get_news_sentiment, symbol)

                # Ergebnisse einsammeln
                profile["overview"] = f_overview.result()
                profile["market_status"] = f_market.result()
                profile["quote"] = f_quote.result()
                
                # Pandas DataFrames in Listen umwandeln
                income_df = f_income.result()
                profile["income_statement"] = income_df.to_dict(orient="records") if not income_df.empty else []
                
                cashflow_df = f_cashflow.result()
                profile["cash_flow"] = cashflow_df.to_dict(orient="records") if not cashflow_df.empty else []

                profile["sentiment"] = {
                    "analyst": f_analyst.result(),
                    "news": f_news.result()
                }
                
        except Exception as e:
            logger.error(f"Orchestrator Abbruch bei {symbol}: {e}")
            profile["error"] = str(e)

        # 3. Für RAG in die Datenbank speichern (Persistenz)
        try:
            with sqlite3.connect(self.db_path) as conn:
                conn.execute(
                    "INSERT OR REPLACE INTO orchestrator_cache (symbol, data_json, timestamp) VALUES (?, ?, ?)",
                    (symbol, json.dumps(profile), time.time())
                )
            logger.success(f"💾 Neues Profil für {symbol} in der Master-DB für die KI gespeichert!")
        except Exception as e:
            logger.error(f"Cache Schreib-Fehler: {e}")

        return profile

_orchestrator = None
def get_orchestrator():
    global _orchestrator
    if not _orchestrator:
        _orchestrator = DataOrchestrator()
    return _orchestrator