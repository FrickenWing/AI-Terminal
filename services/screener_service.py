import sqlite3
import os
from loguru import logger
from data.openbb_client import get_client

class ScreenerService:
    """
    Das Herzstück für Empfehlungen. 
    Dieser Service filtert die globale Datenbank nach deinen Kriterien.
    """
    
    def __init__(self, db_path="data/master_assets.db"):
        self.db_path = db_path
        self.market_client = get_client()

    def get_recommendations(self, sector: str = None, asset_type: str = "Common Stock", limit: int = 10):
        """
        Sucht nach Top-Aktien basierend auf Kriterien.
        Später wird hier die KI (FinGPT) drüberlaufen.
        """
        logger.info(f"Suche Empfehlungen für Typ: {asset_type}...")
        
        recommendations = []
        try:
            with sqlite3.connect(self.db_path) as conn:
                conn.row_factory = sqlite3.Row
                cursor = conn.cursor()
                
                # Wir filtern erst einmal nach dem Typ (z.B. nur echte Aktien, keine ETFs)
                cursor.execute("""
                    SELECT full_ticker, name, exchange_code, currency 
                    FROM assets 
                    WHERE type = ? 
                    ORDER BY RANDOM() LIMIT ?
                """, (asset_type, limit))
                
                candidates = cursor.fetchall()
                
                for cand in candidates:
                    ticker = cand["full_ticker"]
                    # Jetzt holen wir Live-Daten über deinen Finnhub-Account
                    quote = self.market_client.get_quote(ticker)
                    
                    # Ein einfaches Ranking-System (Beispiel: KGV unter 25)
                    pe = quote.get("pe_ratio", "N/A")
                    score = 0
                    if isinstance(pe, (int, float)) and pe > 0:
                        if pe < 15: score += 30  # Sehr günstig
                        elif pe < 25: score += 15 # Fair bewertet
                    
                    recommendations.append({
                        "ticker": ticker,
                        "name": cand["name"],
                        "price": quote.get("price", 0),
                        "pe": pe,
                        "score": score,
                        "reason": "Günstige Bewertung" if score > 0 else "Neutral"
                    })
                    
            # Sortiere nach dem Score (Die besten zuerst)
            return sorted(recommendations, key=lambda x: x['score'], reverse=True)
            
        except Exception as e:
            logger.error(f"Screener Fehler: {e}")
            return []

    def get_global_stats(self):
        """Liefert eine Übersicht, wie viele Assets wir weltweit haben."""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT type, COUNT(*) FROM assets GROUP BY type")
                return dict(cursor.fetchall())
        except:
            return {}