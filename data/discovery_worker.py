import os
import sqlite3
import requests
from loguru import logger
from dotenv import load_dotenv

# --- NEU: Finde die .env Datei garantiert ---
# Das sucht nach dem Hauptordner deines Projekts
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
ENV_PATH = os.path.join(BASE_DIR, ".env")
DB_PATH = os.path.join(BASE_DIR, "data", "master_assets.db")

# Lade die .env Datei explizit von diesem Pfad
if os.path.exists(ENV_PATH):
    # override=True zwingt Python dazu, den Wert aus der Datei zu nehmen!
    load_dotenv(ENV_PATH, override=True)
    logger.info(f".env Datei gefunden unter: {ENV_PATH}")
else:
    logger.warning(f"Keine .env Datei gefunden unter: {ENV_PATH}! Erstelle eine im Hauptverzeichnis.")

class DiscoveryWorker:
    """
    Sammelt global handelbare Symbole (Aktien & ETFs) von Finnhub und TwelveData
    und speichert sie in einer lokalen SQLite Datenbank.
    """
    
    # --- UPDATE: Erweitertes Mapping für Finnhub (US) & TwelveData (Global) ---
    EXCHANGE_MAPPING = {
        "US": {"suffix": "", "td_country": "United States"},      
        "DE": {"suffix": ".DE", "td_country": "Germany"},   
        "L": {"suffix": ".L", "td_country": "United Kingdom"},     
        "PA": {"suffix": ".PA", "td_country": "France"},   
        "HK": {"suffix": ".HK", "td_country": "Hong Kong"},   
        "TO": {"suffix": ".TO", "td_country": "Canada"},   
        "AS": {"suffix": ".AS", "td_country": "Netherlands"},   
        "MI": {"suffix": ".MI", "td_country": "Italy"},   
        "MC": {"suffix": ".MC", "td_country": "Spain"},   
        "SW": {"suffix": ".SW", "td_country": "Switzerland"},   
    }

    def __init__(self, db_path=DB_PATH):
        # Versuche den Key zu holen, mit Fallback
        self.api_key = os.getenv("FINNHUB_API_KEY") or os.getenv("finnhub_api_key")
        
        # --- NEU: Debug Info ---
        if self.api_key:
            logger.info("✅ API Key erfolgreich aus .env geladen!")
        else:
            logger.error("❌ API Key ist immer noch LEER. Bitte prüfe den Text in der .env Datei.")
            
        self.db_path = db_path
        
        os.makedirs(os.path.dirname(self.db_path), exist_ok=True)
        self._init_db()

    def _init_db(self):
        logger.info(f"Datenbank wird vorbereitet unter: {self.db_path}")
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS assets (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    symbol TEXT NOT NULL,
                    name TEXT,
                    type TEXT,
                    currency TEXT,
                    exchange_code TEXT,
                    yahoo_suffix TEXT,
                    full_ticker TEXT UNIQUE,
                    isin TEXT,
                    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            conn.execute("CREATE INDEX IF NOT EXISTS idx_symbol ON assets(symbol)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_full_ticker ON assets(full_ticker)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_type ON assets(type)")

    def fetch_finnhub_us(self):
        """Holt US Symbole exklusiv von Finnhub."""
        exchange_code = "US"
        suffix = self.EXCHANGE_MAPPING[exchange_code]["suffix"]
        
        if not self.api_key:
            logger.error("🚨 Abbruch: Kein FINNHUB_API_KEY gefunden.")
            return

        logger.info("Starte Finnhub Discovery für US-Markt...")
        url = f"https://finnhub.io/api/v1/stock/symbol?exchange={exchange_code}&token={self.api_key}"
        
        try:
            response = requests.get(url, timeout=20)
            response.raise_for_status()
            symbols = response.json()
            
            data_to_insert = []
            for s in symbols:
                ticker = s.get("symbol", "")
                if not ticker: continue
                full_ticker = ticker # US hat kein Suffix
                
                data_to_insert.append((
                    ticker, s.get("description", ""), s.get("type", "Common Stock"),
                    s.get("currency", "USD"), exchange_code, suffix, full_ticker, s.get("isin", "")
                ))

            self._save_to_db(data_to_insert, exchange_code)
        except Exception as e:
            logger.error(f"❌ Fehler bei Finnhub US: {e}")

    def fetch_twelvedata_global(self, exchange_code: str):
        """Holt internationale Symbole über die kostenlose TwelveData API (ohne Key)."""
        country = self.EXCHANGE_MAPPING[exchange_code]["td_country"]
        suffix = self.EXCHANGE_MAPPING[exchange_code]["suffix"]
        
        logger.info(f"Starte TwelveData Discovery für {exchange_code} ({country})...")
        url = f"https://api.twelvedata.com/stocks?country={country}"
        
        try:
            response = requests.get(url, timeout=20)
            response.raise_for_status()
            data = response.json().get("data", [])
            
            data_to_insert = []
            for s in data:
                ticker = s.get("symbol", "")
                if not ticker: continue
                
                # Wir hängen das Suffix für Yahoo Finance an (z.B. SAP -> SAP.DE)
                full_ticker = f"{ticker}{suffix}" if suffix and not ticker.endswith(suffix) else ticker
                
                data_to_insert.append((
                    ticker, s.get("name", ""), s.get("type", "Common Stock"),
                    s.get("currency", ""), exchange_code, suffix, full_ticker, "" 
                ))

            self._save_to_db(data_to_insert, exchange_code)
        except Exception as e:
            logger.error(f"❌ Fehler bei TwelveData {exchange_code}: {e}")

    def _save_to_db(self, data_to_insert, exchange_code):
        """Hilfsfunktion zum Speichern in die SQLite (DRY-Prinzip)."""
        with sqlite3.connect(self.db_path) as conn:
            conn.executemany("""
                INSERT OR REPLACE INTO assets 
                (symbol, name, type, currency, exchange_code, yahoo_suffix, full_ticker, isin)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, data_to_insert)
        logger.success(f"✅ {len(data_to_insert)} Symbole für {exchange_code} gespeichert.")

    def run_global_sync(self):
        logger.info("Starte globalen Asset-Sync (Hybrid-Modus)...")
        
        for ex in self.EXCHANGE_MAPPING.keys():
            if ex == "US":
                self.fetch_finnhub_us()
            else:
                self.fetch_twelvedata_global(ex)
                
        logger.info("🏁 Globaler Hybrid Asset-Sync abgeschlossen. Die Master-Datenbank ist bereit!")

if __name__ == "__main__":
    worker = DiscoveryWorker()
    worker.run_global_sync()