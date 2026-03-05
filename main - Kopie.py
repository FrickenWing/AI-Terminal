from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from pydantic import BaseModel
from contextlib import asynccontextmanager
import os
import json
import requests
import pandas as pd
from dotenv import load_dotenv
from loguru import logger

load_dotenv()

from data.openbb_client import get_client
from data.fingpt_client import get_fingpt_client, AVAILABLE_MODELS
from services.omni_data_service import get_omni_service
from services.technical_analysis_service import get_technical_analysis_service
from services.screener_service import get_screener_service
from services.portfolio_service import get_portfolio_service
from data.cache_manager import get_cache, TTL

omni_service = get_omni_service()
tech_service = get_technical_analysis_service()
screener_service = get_screener_service()
portfolio_service = get_portfolio_service()
cache        = get_cache()

# ── Pydantic Models ───────────────────────────────────────────────────────────
class WatchlistItem(BaseModel):
    symbol: str
    name: str

class ChatMessage(BaseModel):
    symbol: str
    message: str
    model: str = "mistralai/Mistral-7B-Instruct-v0.3"

class OmniAnalyzeRequest(BaseModel):
    symbol: str
    model:  str = "mistralai/Mistral-7B-Instruct-v0.3"
    focus:  str = "full"

class ReportRequest(BaseModel):
    symbol: str
    model:  str = "mistralai/Mistral-7B-Instruct-v0.3"

# NEU v4.0:
class OmniMultiAgentRequest(BaseModel):
    symbol: str
    model:  str = "mistralai/Mistral-7B-Instruct-v0.3"

class ScreenerRequest(BaseModel):
    universe: str
    filters: dict = {}

class PortfolioRequest(BaseModel):
    positions: list[dict]

# ── Watchlist Helpers ─────────────────────────────────────────────────────────
WATCHLIST_FILE = "watchlist.json"
DEFAULT_WATCHLIST = [
    {"symbol": "AAPL", "name": "Apple Inc."},
    {"symbol": "MSFT", "name": "Microsoft Corp."},
    {"symbol": "NVDA", "name": "NVIDIA Corp."},
    {"symbol": "RHM.DE", "name": "Rheinmetall AG"}
]

def load_watchlist_data():
    if not os.path.exists(WATCHLIST_FILE):
        with open(WATCHLIST_FILE, "w", encoding="utf-8") as f:
            json.dump(DEFAULT_WATCHLIST, f, indent=4)
        return DEFAULT_WATCHLIST
    try:
        with open(WATCHLIST_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except:
        return DEFAULT_WATCHLIST

def save_watchlist_data(data):
    with open(WATCHLIST_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=4)

# ── Lifespan ──────────────────────────────────────────────────────────────────
@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("[STARTUP] Lade Watchlist-Cache vor...")
    try:
        for item in load_watchlist_data():
            client.get_quote(item["symbol"])
    except Exception as e:
        logger.warning(f"[STARTUP] {e}")
    yield

# ── App Setup ─────────────────────────────────────────────────────────────────
app = FastAPI(title="AI-Analyst Backend v4.0 – God-Mode", lifespan=lifespan)
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_credentials=True,
                   allow_methods=["*"], allow_headers=["*"])

client    = get_client()
ai_client = get_fingpt_client()

# ═════════════════════════════════════════════════════════════════════════════
# BESTEHENDE ENDPUNKTE (v3.1) - Unverändert für das Dashboard!
# ═════════════════════════════════════════════════════════════════════════════

@app.get("/")
def serve_frontend():
    return FileResponse(os.path.join(os.path.dirname(os.path.abspath(__file__)), "index.html"))

@app.get("/api/models")
def get_models():
    return AVAILABLE_MODELS

@app.get("/api/watchlist")
def get_watchlist():
    
    data = []
           
    for item in load_watchlist_data():
        t = item["symbol"]
        if t == "UNDEFINED" or not t: continue # Ignoriere fehlerhafte Einträge
        
        quote = client.get_quote(t)
        if quote and quote.get("price", 0) > 0:
            change_pct = quote.get("change_pct", 0) * 100
            data.append({
                "symbol": t,
                "name": item.get("name", quote.get("name", t)),
                "price": f"{quote.get('price', 0):.2f}",
                "change": f"{'+' if change_pct > 0 else ''}{change_pct:.2f}%",
                "isPositive": change_pct > 0
            })
    return data

@app.post("/api/watchlist")
def add_to_watchlist(item: WatchlistItem):
    
    if not item.symbol or item.symbol.upper() == "UNDEFINED":
        return {"status": "error", "message": "Ungültiges Symbol"}
        
    items = load_watchlist_data()
    if not any(i["symbol"] == item.symbol.upper() for i in items):
        items.append({"symbol": item.symbol.upper(), "name": item.name})
        save_watchlist_data(items)
    return {"status": "success"}

@app.delete("/api/watchlist/{symbol}")
def remove_from_watchlist(symbol: str):
    save_watchlist_data([i for i in load_watchlist_data() if i["symbol"] != symbol.upper()])
    return {"status": "success"}

@app.get("/api/search")
def search_ticker(q: str = ""):
    if not q: return []
    return [{"symbol": r["ticker"], "name": r["name"], "type": r.get("type", "Aktie")}
            for r in client.search_ticker(q)]

@app.get("/api/chart/{symbol}")
def get_chart_data(symbol: str, period: str = "1y"):
    result = client.get_price_history(symbol, period=period)
    if result is None: return []
    df, source = result
    if df is None or df.empty: return []
    df = df.dropna(subset=["open","high","low","close"])
    df = df[~df.index.duplicated(keep="last")].sort_index()
    is_intraday = period in ["1d","5d","1mo"]
    out = []
    for date, row in df.iterrows():
        try:
            out.append({"time": int(date.timestamp()) if is_intraday else date.strftime("%Y-%m-%d"),
                        "open": round(float(row["open"]),2), "high": round(float(row["high"]),2),
                        "low": round(float(row["low"]),2),   "close": round(float(row["close"]),2),
                        "value": round(float(row["close"]),2)})
        except: continue
    return out

@app.get("/api/stock/{symbol}")
def get_stock(symbol: str):
    quote      = client.get_quote(symbol)
    price      = quote.get("price", 0)
    change_pct = quote.get("change_pct", 0) * 100
    try:
        ai_summary = ai_client.ask(
            f"Aktie: {symbol} | Preis: ${price:.2f} | Tagesänderung: {change_pct:+.2f}% | "
            f"KGV: {quote.get('pe_ratio','N/A')}\n"
            f"Schreibe eine prägnante 2-Satz Einschätzung zu dieser Aktie auf Deutsch.")
    except Exception as e:
        ai_summary = f"{symbol} notiert bei ${price:.2f} ({change_pct:+.2f}%). KI nicht verfügbar: {str(e)[:80]}"
    return {"symbol": symbol.upper(), "ai_summary": ai_summary,
            "recommendation": "KAUFEN" if change_pct > 0 else "BEOBACHTEN",
            "source": quote.get("source","Unbekannt")}

@app.get("/api/news/{symbol}")
def get_news(symbol: str):
    return client.get_news(symbol)

@app.post("/api/chat")
def chat_with_ai(chat: ChatMessage):
    try:
        quote = client.get_quote(chat.symbol)
        reply = ai_client.ask(
            f"Aktie: {chat.symbol} | Preis: ${quote.get('price','?')} | "
            f"KGV: {quote.get('pe_ratio','N/A')} | "
            f"Tagesänderung: {quote.get('change_pct',0)*100:+.2f}%\n\nNutzerfrage: {chat.message}",
            model_id=chat.model)
        return {"reply": reply, "model_used": chat.model}
    except Exception as e:
        return {"reply": f"FinGPT-Fehler: {str(e)}"}

@app.get("/api/fmp/{symbol}")
def get_fmp_data(symbol: str):
    fmp_key     = os.getenv("FMP_API_KEY","")
    finnhub_key = os.getenv("FINNHUB_API_KEY","")
    d = {"revenue":"N/A","netIncome":"N/A","eps":"N/A","peRatio":"N/A",
         "marketCap":"N/A","debtToEquity":"N/A","source":"Unbekannt"}
    ok = False
    if fmp_key:
        try:
            inc = requests.get(f"https://financialmodelingprep.com/stable/income-statement?symbol={symbol}&limit=1&apikey={fmp_key}",timeout=5).json()
            met = requests.get(f"https://financialmodelingprep.com/stable/key-metrics?symbol={symbol}&limit=1&apikey={fmp_key}",timeout=5).json()
            if isinstance(inc,list) and inc:
                m = met[0] if isinstance(met,list) and met else {}
                d.update({"revenue":inc[0].get("revenue","N/A"),"netIncome":inc[0].get("netIncome","N/A"),
                          "eps":inc[0].get("eps","N/A"),"peRatio":m.get("peRatio","N/A"),
                          "marketCap":m.get("marketCap","N/A"),"debtToEquity":m.get("debtToEquity","N/A"),"source":"FMP"})
                ok = True
        except: pass
    if finnhub_key and (not ok or d["peRatio"] in [None,"N/A",""]):
        try:
            fh = requests.get(f"https://finnhub.io/api/v1/stock/metric?symbol={symbol}&metric=all&token={finnhub_key}",timeout=3).json().get("metric",{})
            if fh:
                if d["peRatio"] in [None,"N/A",""]:     d["peRatio"]      = fh.get("peNormalizedAnnual","N/A")
                if d["eps"] in [None,"N/A",""]:          d["eps"]          = fh.get("epsTrailingTwelveMonths","N/A")
                if d["marketCap"] in [None,"N/A",""]:
                    mc = fh.get("marketCapitalization")
                    d["marketCap"] = mc*1_000_000 if mc else "N/A"
                if d["debtToEquity"] in [None,"N/A",""]: d["debtToEquity"] = fh.get("totalDebt/totalEquityAnnual","N/A")
                d["source"] = "FMP & Finnhub" if ok else "Finnhub"
        except: pass
    if d["source"] == "Unbekannt":
        try:
            import yfinance as yf
            i = yf.Ticker(symbol).info
            d.update({"revenue":i.get("totalRevenue","N/A"),"netIncome":i.get("netIncomeToCommon","N/A"),
                      "eps":i.get("trailingEps","N/A"),"peRatio":i.get("trailingPE","N/A"),
                      "marketCap":i.get("marketCap","N/A"),
                      "debtToEquity":(i.get("debtToEquity")/100 if i.get("debtToEquity") else "N/A"),
                      "source":"Yahoo Finance"})
        except: pass
    for k,v in d.items():
        if v is None: d[k] = "N/A"
    return d

@app.post("/api/report")
def generate_report(req: ReportRequest):
    return omni_analyze(OmniAnalyzeRequest(symbol=req.symbol, model=req.model, focus="full"))

@app.get("/api/omni/{symbol}")
def get_omni_bundle(symbol: str):
    key    = f"omni:{symbol.upper()}"
    cached = cache.get(key)
    if cached: return cached
    bundle = omni_service.get_bundle(symbol)
    cache.set(key, bundle, ttl=TTL["screener"])
    return bundle

@app.post("/api/omni/analyze")
def omni_analyze(req: OmniAnalyzeRequest):
    key    = f"omni_report:{req.symbol.upper()}:{req.model}:{req.focus}"
    cached = cache.get(key)
    if cached: return cached

    bundle = omni_service.get_bundle(req.symbol)
    prompt = omni_service.build_llm_prompt(bundle, analysis_focus=req.focus)

    try:
        report = ai_client.ask(prompt, model_id=req.model)
    except Exception as e:
        logger.error(f"[OmniAnalyze] {e}")
        report = f"**KI temporär nicht verfügbar.**\n\nFehler: {str(e)}"

    result = {
        "symbol":     req.symbol.upper(),
        "report":     report,
        "bundle":     bundle,
        "prompt_len": len(prompt),
        "timestamp":  bundle.get("timestamp"),
        "errors":     bundle.get("errors",[]),
    }
    cache.set(key, result, ttl=900)
    return result

@app.get("/api/reddit/{symbol}")
def get_reddit_sentiment(symbol: str):
    key = f"reddit:{symbol.upper()}"
    c   = cache.get(key)
    if c: return c
    from data.reddit_client import get_reddit_client
    result = get_reddit_client().get_ticker_sentiment(symbol)
    cache.set(key, result, ttl=600)
    return result

@app.get("/api/trends/{symbol}")
def get_google_trends(symbol: str):
    key = f"trends:{symbol.upper()}"
    c   = cache.get(key)
    if c: return c
    from data.trends_client import get_trends_client
    result = get_trends_client().get_interest(symbol)
    cache.set(key, result, ttl=3600)
    return result

@app.get("/api/sec/{symbol}")
def get_sec_data(symbol: str):
    key = f"sec:{symbol.upper()}"
    c   = cache.get(key)
    if c: return c
    from data.sec_client import get_sec_client
    s      = get_sec_client()
    result = {"insider_summary": s.get_insider_summary(symbol),
              "recent_filings":  s.get_recent_filings(symbol)}
    cache.set(key, result, ttl=3600)
    return result

@app.get("/api/signals")
def get_market_signals():
    key = "global_signals"
    c   = cache.get(key)
    if c: return c
    from data.signals_client import get_signals_client
    s      = get_signals_client()
    result = {"fear_greed": s.get_fear_greed(), "macro": s.get_macro_signals()}
    cache.set(key, result, ttl=300)
    return result

@app.get("/api/signals/{symbol}")
def get_stock_signals(symbol: str):
    key = f"signals:{symbol.upper()}"
    c   = cache.get(key)
    if c: return c
    from data.signals_client import get_signals_client
    s      = get_signals_client()
    result = {"earnings":       s.get_earnings_calendar(symbol),
              "analyst":        s.get_analyst_ratings(symbol),
              "news_sentiment": s.get_news_sentiment(symbol)}
    cache.set(key, result, ttl=TTL["screener"])
    return result


# ═════════════════════════════════════════════════════════════════════════════
# NEU v4.0: GOD-MODE ENDPUNKTE (Multi-Agent, Chart Overlays, Screener, Portfolio)
# ═════════════════════════════════════════════════════════════════════════════

@app.post("/api/omni/multi-agent")
def omni_multi_agent_analyze(req: OmniMultiAgentRequest):
    """Führt das asynchrone Multi-Agenten System aus (Parallel KI)."""
    bundle = omni_service.get_bundle(req.symbol)
    report = omni_service.run_multi_agent_analysis(bundle, ai_client, req.model)
    return {"symbol": req.symbol.upper(), "report": report}

@app.get("/api/technical/{symbol}")
def get_technical_data(symbol: str):
    """Liefert Zeitreihen für Chart-Overlays (SMA, BB) und aktuelle Signale."""
    df = tech_service.get_price_data(symbol, period="1y")
    if df.empty: return {"chart_data": [], "analysis": {}}
    
    # Chart formatieren für Lightweight Charts UI
    chart_data = []
    for date, row in df.iterrows():
        chart_data.append({
            "time": date.strftime("%Y-%m-%d"),
            "sma_20": round(row.get('sma_20', 0), 2) if pd.notna(row.get('sma_20')) else None,
            "sma_50": round(row.get('sma_50', 0), 2) if pd.notna(row.get('sma_50')) else None,
            "bb_upper": round(row.get('bb_upper', 0), 2) if pd.notna(row.get('bb_upper')) else None,
            "bb_lower": round(row.get('bb_lower', 0), 2) if pd.notna(row.get('bb_lower')) else None,
        })
    
    analysis = tech_service.analyze_indicators(df)
    return {"chart_data": chart_data, "analysis": analysis}

@app.post("/api/screener")
def run_screener(req: ScreenerRequest):
    """Nutzt den Screener Service mit Finnhub."""
    df = screener_service.run_screen(req.universe, req.filters)
    # NaN zu None für JSON Parsing
    df = df.where(pd.notnull(df), None)
    return df.to_dict(orient="records")

@app.post("/api/portfolio/analyze")
def analyze_portfolio(req: PortfolioRequest):
    """Berechnet Sharpe, VaR, Max Drawdown für das Portfolio."""
    res = portfolio_service.get_full_analytics(req.positions)
    # Wir brauchen keine täglichen Returns im JSON an die UI (zu groß)
    if "daily_returns" in res: del res["daily_returns"]
    if "cum_returns" in res: del res["cum_returns"]
    if "cum_benchmark" in res: del res["cum_benchmark"]
    if "correlation" in res and res["correlation"] is not None:
        res["correlation"] = res["correlation"].to_dict()
    return res


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)