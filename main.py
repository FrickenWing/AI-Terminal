from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from pydantic import BaseModel
from contextlib import asynccontextmanager
import os
import json
import requests
from dotenv import load_dotenv
from loguru import logger

load_dotenv()

from data.openbb_client import get_client
from data.fingpt_client import get_fingpt_client, AVAILABLE_MODELS

@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("[STARTUP] Lade Watchlist-Cache vor...")
    try:
        items = load_watchlist_data()
        for item in items:
            client.get_quote(item["symbol"])
    except Exception as e:
        pass
    yield


app = FastAPI(title="AI-Analyst Backend", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

client = get_client()

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
    except: return DEFAULT_WATCHLIST

def save_watchlist_data(data):
    with open(WATCHLIST_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=4)

class WatchlistItem(BaseModel):
    symbol: str
    name: str

class ChatMessage(BaseModel):
    symbol: str
    message: str
    model: str = "mistralai/Mistral-7B-Instruct-v0.3"

ai_client = get_fingpt_client()

@app.get("/api/models")
def get_models():
    """Liefert verfügbare HuggingFace-Modelle ans Frontend."""
    return AVAILABLE_MODELS

@app.get("/")
def serve_frontend():
    base_dir = os.path.dirname(os.path.abspath(__file__))
    return FileResponse(os.path.join(base_dir, "index.html"))

@app.get("/api/watchlist")
def get_watchlist():
    items = load_watchlist_data()
    data = []
    for item in items:
        t = item["symbol"]
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
    items = load_watchlist_data()
    if not any(i["symbol"] == item.symbol.upper() for i in items):
        items.append({"symbol": item.symbol.upper(), "name": item.name})
        save_watchlist_data(items)
    return {"status": "success"}

@app.delete("/api/watchlist/{symbol}")
def remove_from_watchlist(symbol: str):
    items = load_watchlist_data()
    items = [i for i in items if i["symbol"] != symbol.upper()]
    save_watchlist_data(items)
    return {"status": "success"}

@app.get("/api/search")
def search_ticker(q: str = ""):
    if not q: return []
    results = client.search_ticker(q)
    return [{"symbol": r["ticker"], "name": r["name"], "type": r.get("type", "Aktie")} for r in results]

@app.get("/api/chart/{symbol}")
def get_chart_data(symbol: str, period: str = "1y"):
    result = client.get_price_history(symbol, period=period)
    if result is None: return []
    df, source = result
    if df is None or df.empty: return []

    df = df.dropna(subset=['open', 'high', 'low', 'close'])
    df = df[~df.index.duplicated(keep='last')]
    df = df.sort_index(ascending=True)

    chart_data = []
    is_intraday = period in ["1d", "5d", "1mo"]
    
    for date, row in df.iterrows():
        try:
            time_val = int(date.timestamp()) if is_intraday else date.strftime('%Y-%m-%d')
            chart_data.append({
                "time": time_val,
                "open": round(float(row['open']), 2),
                "high": round(float(row['high']), 2),
                "low": round(float(row['low']), 2),
                "close": round(float(row['close']), 2),
                "value": round(float(row['close']), 2)
            })
        except: continue
    return chart_data

@app.get("/api/stock/{symbol}")
def get_stock(symbol: str):
    quote = client.get_quote(symbol)
    price = quote.get('price', 0)
    change_pct = quote.get("change_pct", 0) * 100
    source = quote.get('source', 'Unbekannt')

    try:
        prompt = (
            f"Aktie: {symbol} | Preis: ${price:.2f} | "
            f"Tagesänderung: {change_pct:+.2f}% | KGV: {quote.get('pe_ratio', 'N/A')}\n"
            f"Schreibe eine prägnante 2-Satz Einschätzung zu dieser Aktie auf Deutsch."
        )
        ai_summary = ai_client.ask(prompt)
    except Exception as e:
        ai_summary = f"{symbol} notiert bei ${price:.2f} ({change_pct:+.2f}%). KI nicht verfügbar: {str(e)[:80]}"
    
    return {
        "symbol": symbol.upper(),
        "ai_summary": ai_summary,
        "recommendation": "KAUFEN" if change_pct > 0 else "BEOBACHTEN",
        "source": source
    }

@app.get("/api/news/{symbol}")
def get_news(symbol: str):
    return client.get_news(symbol)

@app.post("/api/chat")
def chat_with_ai(chat: ChatMessage):
    try:
        quote = client.get_quote(chat.symbol)
        prompt = (
            f"Aktie: {chat.symbol} | Preis: ${quote.get('price', '?')} | "
            f"KGV: {quote.get('pe_ratio', 'N/A')} | "
            f"Tagesänderung: {quote.get('change_pct', 0)*100:+.2f}%\n\n"
            f"Nutzerfrage: {chat.message}"
        )
        reply = ai_client.ask(prompt, model_id=chat.model)
        return {"reply": reply, "model_used": chat.model}
    except Exception as e:
        logger.error(f"[FinGPT ERROR] {e}")
        return {"reply": f"FinGPT-Fehler: {str(e)}"}

@app.get("/api/fmp/{symbol}")
def get_fmp_data(symbol: str):
    fmp_key = os.getenv("FMP_API_KEY", "")
    finnhub_key = os.getenv("FINNHUB_API_KEY", "")
    final_data = { "revenue": "N/A", "netIncome": "N/A", "eps": "N/A", "peRatio": "N/A", "marketCap": "N/A", "debtToEquity": "N/A", "source": "Unbekannt" }
    fmp_success = False

    if fmp_key:
        try:
            url_income = f"https://financialmodelingprep.com/stable/income-statement?symbol={symbol}&limit=1&apikey={fmp_key}"
            url_metrics = f"https://financialmodelingprep.com/stable/key-metrics?symbol={symbol}&limit=1&apikey={fmp_key}"
            resp_inc = requests.get(url_income, timeout=5).json()
            resp_met = requests.get(url_metrics, timeout=5).json()
            if isinstance(resp_inc, list) and len(resp_inc) > 0:
                inc = resp_inc[0]
                met = resp_met[0] if isinstance(resp_met, list) and len(resp_met) > 0 else {}
                final_data.update({
                    "revenue": inc.get("revenue", "N/A"), "netIncome": inc.get("netIncome", "N/A"),
                    "eps": inc.get("eps", "N/A"), "peRatio": met.get("peRatio", "N/A"),
                    "marketCap": met.get("marketCap", "N/A"), "debtToEquity": met.get("debtToEquity", "N/A"),
                    "source": "FMP"
                })
                fmp_success = True
        except: pass

    if finnhub_key and (not fmp_success or final_data["peRatio"] in [None, "N/A", ""]):
        try:
            url_metrics = f"https://finnhub.io/api/v1/stock/metric?symbol={symbol}&metric=all&token={finnhub_key}"
            fh = requests.get(url_metrics, timeout=3).json().get("metric", {})
            if fh:
                if final_data["peRatio"] in [None, "N/A", ""]: final_data["peRatio"] = fh.get("peNormalizedAnnual", "N/A")
                if final_data["eps"] in [None, "N/A", ""]: final_data["eps"] = fh.get("epsTrailingTwelveMonths", "N/A")
                if final_data["marketCap"] in [None, "N/A", ""]: 
                    mcap = fh.get("marketCapitalization")
                    final_data["marketCap"] = mcap * 1000000 if mcap else "N/A"
                if final_data["debtToEquity"] in [None, "N/A", ""]: final_data["debtToEquity"] = fh.get("totalDebt/totalEquityAnnual", "N/A")
                final_data["source"] = "FMP & Finnhub" if fmp_success else "Finnhub"
        except: pass

    if final_data["source"] == "Unbekannt":
        try:
            import yfinance as yf
            i = yf.Ticker(symbol).info
            final_data.update({
                "revenue": i.get("totalRevenue", "N/A"), "netIncome": i.get("netIncomeToCommon", "N/A"),
                "eps": i.get("trailingEps", "N/A"), "peRatio": i.get("trailingPE", "N/A"),
                "marketCap": i.get("marketCap", "N/A"), "debtToEquity": (i.get("debtToEquity") / 100) if i.get("debtToEquity") else "N/A",
                "source": "Yahoo Finance"
            })
        except: pass

    for k, v in final_data.items():
        if v is None: final_data[k] = "N/A"
    return final_data

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)