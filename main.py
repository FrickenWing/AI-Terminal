from fastapi import FastAPI, HTTPException, Depends
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from pydantic import BaseModel, Field
from typing import List, Optional, Dict, Any
import os
import time
import pandas as pd
from dotenv import load_dotenv
from loguru import logger

# Lade Umgebungsvariablen (API-Keys etc.)
load_dotenv()

# Importiere unsere spezialisierten Module (Die "Gehirne" hinter den Endpunkten)
from data.openbb_client import get_client
from services.omni_data_service import get_omni_service
from services.technical_analysis_service import get_technical_analysis_service
from services.screener_service import ScreenerService
from services.portfolio_service import get_portfolio_service

# --- API Konfiguration ---
app = FastAPI(
    title="AI-Terminal God-Mode API",
    description="Zentrale Schnittstelle für globale Marktdaten, KI-Analysen und Screening.",
    version="0.3.5"
)

# CORS-Einstellungen für das Frontend (Dashboard)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# --- Service Initialisierung ---
# Wir laden die Instanzen einmalig beim Start
market_client = get_client()
omni_service = get_omni_service()
tech_service = get_technical_analysis_service()
screener_service = ScreenerService()
portfolio_service = get_portfolio_service()

# --- Pydantic Modelle (Sicherstellung valider Daten-Inputs) ---
class ChatMessage(BaseModel):
    symbol: str = Field(..., example="AAPL")
    message: str = Field(..., example="Wie ist die aktuelle Stimmung?")
    model: str = "mistralai/Mistral-7B-Instruct-v0.3"

class OmniAnalyzeRequest(BaseModel):
    symbol: str
    model: str = "mistralai/Mistral-7B-Instruct-v0.3"

class ScreenerRequest(BaseModel):
    sector: Optional[str] = None
    asset_type: str = "Common Stock"
    limit: int = 10

class TechnicalRequest(BaseModel):
    symbol: str
    period: str = "1y"

class PortfolioPosition(BaseModel):
    symbol: str
    shares: float
    avg_price: float

class PortfolioRequest(BaseModel):
    positions: List[PortfolioPosition]

# --- API Endpunkte ---

@app.get("/")
async def serve_index():
    """Serviert das Haupt-Interface des Terminals."""
    # Macht den Pfad absolut, egal von wo das Skript gestartet wird
    base_dir = os.path.dirname(os.path.abspath(__file__))
    index_path = os.path.join(base_dir, "index.html")
    
    if os.path.exists(index_path):
        return FileResponse(index_path)
    raise HTTPException(status_code=404, detail=f"index.html nicht gefunden unter: {index_path}")

@app.get("/api/search")
async def search_assets(q: str):
    """
    Sucht nach Tickern. 
    Nutzt zuerst die lokale master_assets.db (Meilenstein 1) für maximale Geschwindigkeit.
    """
    logger.info(f"Suche nach: {q}")
    try:
        return market_client.search_ticker(q)
    except Exception as e:
        logger.error(f"Fehler bei der Suche: {e}")
        return []

@app.get("/api/quote")
def quote(symbol: str):
    """Holt die aktuellen Preisdaten."""
    from data.openbb_client import get_client
    return get_client().get_quote(symbol)

# --- NEU: Endpunkt für Alternative Daten (Finnhub) ---
@app.get("/api/sentiment")
def get_asset_sentiment(symbol: str):
    """Holt Analysten-Ratings, News-Stimmung und Insider-Trades via Finnhub."""
    from data.openbb_client import get_client
    client = get_client()
    return {
        "analyst": client.get_analyst_ratings(symbol),
        "news": client.get_news_sentiment(symbol),
        "insider": client.get_insider_sentiment(symbol)
    }

# --- NEU: Der Data Orchestrator Endpunkt ---
@app.get("/api/orchestrator/profile")
def get_orchestrator_profile(symbol: str):
    """
    Der Master-Endpunkt: Holt Bilanzen, Cashflows, Sentiment und Quotes 
    als ein massives, RAG-optimiertes JSON-Paket.
    """
    from services.data_orchestrator import get_orchestrator
    return get_orchestrator().get_full_profile(symbol)
# -------------------------------------------

@app.post("/api/analyze/omni")
def analyze_omni(req: OmniAnalyzeRequest):
        return {"error": "KI-Analyse fehlgeschlagen."}

@app.post("/api/chat")
async def ai_chat_interaction(req: ChatMessage):
    """Verarbeitet Chat-Anfragen an den KI-Analysten."""
    try:
        response = omni_service.chat(req.symbol, req.message)
        return response
    except Exception as e:
        logger.error(f"Chat-Fehler: {e}")
        return {"reply": "Entschuldigung, ich habe gerade Verbindungsprobleme."}

@app.post("/api/technical")
async def run_ta_scan(req: TechnicalRequest):
    """Berechnet Indikatoren (SMA, RSI, MACD) auf Basis historischer Daten."""
    logger.debug(f"Technischer Scan: {req.symbol} ({req.period})")
    df, source = market_client.get_price_history(req.symbol, req.period)
    
    if df.empty:
        return {"error": "Keine historischen Daten verfügbar."}
    
    # Aufbereitung der Chart-Daten für Lightweight Charts
    chart_data = []
    for idx, row in df.iterrows():
        chart_data.append({
            "time": idx.strftime("%Y-%m-%d"),
            "open": round(row["open"], 2),
            "high": round(row["high"], 2),
            "low": round(row["low"], 2),
            "close": round(row["close"], 2)
        })
    
    # Berechnung der technischen Indikatoren über den Service
    analysis = tech_service.analyze_indicators(df)
    
    return {
        "symbol": req.symbol,
        "source": source,
        "chart_data": chart_data,
        "analysis": analysis
    }

@app.post("/api/screener")
async def run_global_screener(req: ScreenerRequest):
    """Filtert die globale Datenbank nach den besten Investment-Chancen."""
    logger.info(f"Screener Scan gestartet: {req.asset_type}")
    try:
        results = screener_service.get_recommendations(
            sector=req.sector,
            asset_type=req.asset_type,
            limit=req.limit
        )
        return results
    except Exception as e:
        logger.error(f"Screener-Fehler: {e}")
        return {"error": "Screener konnte nicht ausgeführt werden."}

@app.get("/api/screener/stats")
async def get_database_metrics():
    """Gibt Einblick in die Größe der lokalen master_assets.db."""
    return screener_service.get_global_stats()

@app.post("/api/portfolio/analyze")
async def run_portfolio_risk_check(req: PortfolioRequest):
    """Analysiert Klumpenrisiken und Performance im Portfolio."""
    logger.info(f"Portfolio-Analyse für {len(req.positions)} Positionen")
    try:
        # Konvertiere Pydantic Modelle in einfache Liste für den Service
        pos_list = [p.dict() for p in req.positions]
        analytics = portfolio_service.get_full_analytics(pos_list)
        return analytics
    except Exception as e:
        logger.error(f"Portfolio-Fehler: {e}")
        return {"error": "Analyse fehlgeschlagen."}

if __name__ == "__main__":
    import uvicorn
    logger.info("Starte Terminal-Server auf http://localhost:8000")
    uvicorn.run(app, host="0.0.0.0", port=8000)