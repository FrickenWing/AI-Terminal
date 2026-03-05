import os
import pandas as pd
from loguru import logger
from dotenv import load_dotenv
from openai import OpenAI

# Wir nutzen unseren neuen OpenBB Hub!
from data.openbb_client import get_client

load_dotenv()

class OmniDataService:
    """
    Das Gehirn des Terminals. Sammelt alle verfügbaren OpenBB-Daten 
    und füttert sie in das LLM für eine professionelle Analyse.
    """
    def __init__(self):
        self.market_client = get_client()
        
        # KI-Setup (OpenAI als Standard-Reasoning-Engine)
        self.api_key = os.getenv("OPENAI_API_KEY", "")
        if self.api_key:
            self.llm_client = OpenAI(api_key=self.api_key)
        else:
            self.llm_client = None
            logger.warning("Kein OPENAI_API_KEY gefunden. Fallback-Modus aktiv.")

    def _format_dataframe(self, df: pd.DataFrame, limit: int = 5) -> str:
        """Hilfsfunktion: Macht Pandas Dataframes für die KI lesbar."""
        if df.empty:
            return "Keine Daten verfügbar."
        return df.head(limit).to_markdown()

    def generate_report(self, ticker: str) -> str:
        """
        Der ultimative God-Mode Report. 
        Zieht alle Daten und zwingt die KI zu einer klaren Kauf/Verkauf-Entscheidung.
        """
        logger.info(f"Starte Omni-Datenbeschaffung für {ticker}...")

        # 1. Datenbeschaffung über OpenBB
        quote = self.market_client.get_quote(ticker)
        metrics_df = self.market_client.get_company_metrics(ticker)
        macro_df = self.market_client.get_macro_data("FEDFUNDS") # Aktuelle US-Leitzinsen

        # 2. Daten für den Prompt aufbereiten
        price_str = f"${quote.get('price', 'N/A')} (Änderung: {quote.get('change_pct', 0) * 100:.2f}%)"
        metrics_str = self._format_dataframe(metrics_df)
        macro_str = self._format_dataframe(macro_df, limit=1)

        # 3. Der Master-Prompt für die KI
        system_prompt = (
            "Du bist ein gnadenloser, hochbezahlter Hedgefonds-Analyst. "
            "Deine Aufgabe ist es, Aktien auf Basis von echten Daten zu bewerten. "
            "Du sprichst Deutsch. Keine Floskeln, nur harte Fakten."
        )

        user_prompt = f"""
        Erstelle eine Blitz-Analyse für die Aktie: {ticker}.

        HIER SIND DIE LIVE-DATEN (via OpenBB Terminal):
        - Aktueller Preis: {price_str}
        - Fundamentale Kennzahlen:
        {metrics_str}
        - Makro-Umfeld (US-Leitzins FEDFUNDS):
        {macro_str}

        DEINE AUFGABE:
        Schreibe einen gut formatierten Report in Markdown mit folgender Struktur:
        1. 📊 **Executive Summary:** 2 Sätze zur aktuellen Lage.
        2. 🏢 **Fundamental-Check:** Sind die Kennzahlen (KGV etc.) gut oder schlecht? Was bedeutet der Leitzins für diese Aktie?
        3. 🎯 **HANDELSSIGNAL & KURSZIEL:** Gib eine EINDEUTIGE Empfehlung ab (KAUFEN, HALTEN oder VERKAUFEN). Nenne einen exakten Zielpreis für die nächsten 6 Monate und einen Stop-Loss Preis.
        
        WICHTIG: Vermeide Haftungsausschlüsse. Tu so, als wärst du mein interner Analyst.
        """

        # 4. KI-Generierung (Wenn API Key vorhanden)
        if self.llm_client:
            try:
                logger.info(f"Sende Daten an LLM für {ticker}...")
                response = self.llm_client.chat.completions.create(
                    model="gpt-4o-mini", # Schnell und kostengünstig
                    messages=[
                        {"role": "system", "content": system_prompt},
                        {"role": "user", "content": user_prompt}
                    ],
                    temperature=0.2 # Sehr analytisch, wenig Halluzination
                )
                return response.choices[0].message.content
            except Exception as e:
                logger.error(f"LLM Fehler: {e}")
                return f"Fehler bei der KI-Generierung: {e}"
        else:
            # Fallback, falls (noch) kein OpenAI Key da ist
            return (
                f"### 🛑 KI Offline\n"
                f"Bitte füge einen `OPENAI_API_KEY` in deine `.env` Datei ein.\n\n"
                f"**Geladene Rohdaten für {ticker}:**\n"
                f"- Preis: {price_str}\n"
                f"- Kennzahlen geladen: {'Ja' if not metrics_df.empty else 'Nein'}\n"
                f"- Makro geladen: {'Ja' if not macro_df.empty else 'Nein'}\n"
            )

    def chat(self, ticker: str, message: str) -> dict:
        """Für den kleinen Chatbot in der rechten Sidebar."""
        return {"reply": f"Du fragtest nach {ticker}: '{message}'. (Die echte Chat-KI wird im nächsten Schritt an die SEC-Filings gekoppelt!)"}

_omni_service = None
def get_omni_service():
    global _omni_service
    if not _omni_service:
        _omni_service = OmniDataService()
    return _omni_service