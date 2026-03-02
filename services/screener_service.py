"""
services/screener_service.py - Logik für den Aktien-Screener
"""
import pandas as pd
from data.openbb_client import get_client

# Vordefinierte Listen für den Screener (wie in deiner Dokumentation gefordert)
UNIVERSES = {
    "mega_cap_us": ["AAPL", "MSFT", "NVDA", "GOOGL", "AMZN", "META", "TSLA"],
    "dax_40": ["SAP.DE", "SIE.DE", "ALV.DE", "DPW.DE", "MBG.DE", "DTE.DE", "VOW3.DE"],
    "tech_growth": ["PLTR", "CRWD", "SNOW", "DDOG", "NET", "SHOP"]
}

class ScreenerService:
    def __init__(self):
        self.client = get_client()

    def run_screen(self, universe: list, filters: dict = None) -> pd.DataFrame:
        """Führt den Screener für eine Liste von Tickern durch und berechnet den Score."""
        results = []
        for ticker in universe:
            quote = self.client.get_quote(ticker)
            
            # Basis Composite Score (0-100) berechnen
            score = 50 
            change = quote.get("change_pct", 0)
            if change > 0:
                score += 15  # Pluspunkte für Momentum
            elif change < -2:
                score -= 10  # Minuspunkte für starken Abverkauf
                
            results.append({
                "Ticker": ticker,
                "Price": quote.get("price", 0),
                "Change %": change,
                "P/E Ratio": quote.get("pe_ratio", 0),
                "Score": min(max(score, 0), 100) # Score zwischen 0 und 100 halten
            })
            
        df = pd.DataFrame(results)
        
        # Filter anwenden (falls in der UI ausgewählt)
        if filters and not df.empty:
            if "pe_max" in filters:
                df = df[(df["P/E Ratio"] <= filters["pe_max"]) | (df["P/E Ratio"].isna())]
        
        # Nach bestem Score sortieren
        if not df.empty:
            df = df.sort_values(by="Score", ascending=False)
            
        return df

    def get_display_df(self, df: pd.DataFrame) -> pd.DataFrame:
        """Formatiert das DataFrame schön für die Streamlit-Tabelle."""
        if df.empty:
            return df
            
        df_display = df.copy()
        df_display["Price"] = df_display["Price"].apply(lambda x: f"${x:,.2f}" if pd.notnull(x) else "N/A")
        df_display["Change %"] = df_display["Change %"].apply(lambda x: f"{x:+.2f}%" if pd.notnull(x) else "N/A")
        
        # Score als Balken formatieren (optional, sieht in Streamlit toll aus)
        return df_display

# --- SINGLETON PATTERN ---
_screener_service_instance = None

def get_screener_service() -> ScreenerService:
    global _screener_service_instance
    if _screener_service_instance is None:
        _screener_service_instance = ScreenerService()
    return _screener_service_instance