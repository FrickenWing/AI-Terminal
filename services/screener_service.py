"""
services/screener_service.py - Logik für den Aktien-Screener
"""
import pandas as pd
from data.openbb_client import get_client
from concurrent.futures import ThreadPoolExecutor
from loguru import logger
from services.technical_analysis_service import get_technical_analysis_service

# Vordefinierte Listen für den Screener
UNIVERSES = {
    "mega_cap_us": ["AAPL", "MSFT", "NVDA", "GOOGL", "AMZN", "META", "TSLA", "BRK-B", "LLY", "AVGO"],
    "dax_40": ["SAP.DE", "SIE.DE", "ALV.DE", "DPW.DE", "MBG.DE", "DTE.DE", "VOW3.DE", "BMW.DE", "BAS.DE", "MUV2.DE"],
    "tech_growth": ["PLTR", "CRWD", "SNOW", "DDOG", "NET", "SHOP", "MNDY", "ZS", "U", "RBLX"]
}

class ScreenerService:
    def __init__(self):
        self.client = get_client()
        self.tech = get_technical_analysis_service()

    def _analyze_ticker(self, ticker: str) -> dict:
        """Hilfsfunktion für ThreadPoolExecutor - Holt Quote + Tech Score für einen Ticker."""
        try:
            quote = self.client.get_quote(ticker)
            price = quote.get("price", 0)
            if not price: return None
            
            # Technik-Score holen für "God-Mode" Screening
            df = self.tech.get_price_data(ticker, period="3mo")
            tech_analysis = self.tech.analyze_indicators(df)
            score = tech_analysis.get("score", 50)
            rsi = tech_analysis.get("latest_data", {}).get("rsi", 50)

            return {
                "Ticker": ticker,
                "Price": price,
                "Change %": quote.get("change_pct", 0) * 100,
                "P/E Ratio": quote.get("pe_ratio"),
                "Score": score,
                "RSI": rsi
            }
        except Exception as e:
            logger.warning(f"Screener Fehler für {ticker}: {e}")
            return None

    def run_screen(self, universe_key: str, filters: dict = None) -> pd.DataFrame:
        """
        Führt den Screener für eine Liste von Tickern durch und berechnet den Score.
        Neu v4.0: Nutzt ThreadPoolExecutor für massive Beschleunigung.
        """
        tickers = UNIVERSES.get(universe_key, UNIVERSES["mega_cap_us"])
        
        results = []
        with ThreadPoolExecutor(max_workers=10) as executor:
            for res in executor.map(self._analyze_ticker, tickers):
                if res: results.append(res)
                
        df = pd.DataFrame(results)
        if df.empty: return df

        # Filter anwenden (falls in der UI ausgewählt)
        if filters:
            pe_max = filters.get("pe_max")
            if pe_max and pe_max != "":
                df = df[(df["P/E Ratio"].isna()) | (df["P/E Ratio"] == "N/A") | (pd.to_numeric(df["P/E Ratio"], errors='coerce') <= float(pe_max))]
                
            rsi_min = filters.get("rsi_min")
            if rsi_min and rsi_min != "":
                df = df[df["RSI"] >= float(rsi_min)]

        # Nach bestem Score sortieren
        if not df.empty:
            df = df.sort_values(by="Score", ascending=False)
            
        return df

    def get_display_df(self, df: pd.DataFrame) -> pd.DataFrame:
        """Formatiert das DataFrame schön für die Streamlit-Tabelle (Original v3.1 Funktion)."""
        if df.empty:
            return df
            
        df_display = df.copy()
        df_display["Price"] = df_display["Price"].apply(lambda x: f"${x:,.2f}" if pd.notnull(x) else "N/A")
        df_display["Change %"] = df_display["Change %"].apply(lambda x: f"{x:+.2f}%" if pd.notnull(x) else "N/A")
        
        return df_display

# --- SINGLETON PATTERN ---
_screener_service_instance = None

def get_screener_service() -> ScreenerService:
    global _screener_service_instance
    if _screener_service_instance is None:
        _screener_service_instance = ScreenerService()
    return _screener_service_instance