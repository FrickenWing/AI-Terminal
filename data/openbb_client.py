import time
import hashlib
import json
import requests
import pandas as pd
import os
import yfinance as yf
from functools import wraps
from loguru import logger
from dotenv import load_dotenv
from datetime import datetime, timedelta

load_dotenv()
_cache_store: dict = {}

def cached(ttl_seconds: int = 300):
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            key_parts = [func.__name__, str(args), str(kwargs)]
            key = hashlib.md5(json.dumps(key_parts, sort_keys=True, default=str).encode()).hexdigest()
            if key in _cache_store:
                val, exp = _cache_store[key]
                if time.time() < exp:
                    return val
            try:
                res = func(*args, **kwargs)
                if res is not None:
                    _cache_store[key] = (res, time.time() + ttl_seconds)
                return res
            except Exception as e:
                logger.error(f"Error in {func.__name__}: {e}")
                return None
        return wrapper
    return decorator


def _flatten_yf_columns(df: pd.DataFrame) -> pd.DataFrame:
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = df.columns.get_level_values(0)
    df.columns = [str(c).strip().lower() for c in df.columns]
    return df


class OpenBBClient:
    def __init__(self):
        self.fmp_key = os.getenv("FMP_API_KEY", "")
        self.finnhub_key = os.getenv("FINNHUB_API_KEY", "")
        self.headers = {'User-Agent': 'Mozilla/5.0'}

    def search_ticker(self, query: str) -> list:
        # Sucht Aktien, ETFs und akzeptiert ISINs (Yahoo Finance mappt ISINs automatisch)
        if not query: return []
        results = []
        try:
            url = f"https://query2.finance.yahoo.com/v1/finance/search?q={query}&quotesCount=10"
            data = requests.get(url, headers=self.headers, timeout=4).json()
            for item in data.get('quotes', []):
                # Akzeptiere EQUITY und ETFs
                if item.get('quoteType') in ['EQUITY', 'ETF', 'MUTUALFUND']:
                    type_str = "ETF" if item.get('quoteType') != 'EQUITY' else "Aktie"
                    results.append({
                        "ticker": item['symbol'], 
                        "name": item.get('shortname') or item.get('longname'),
                        "type": type_str
                    })
        except: pass
        return results

    @cached(ttl_seconds=300)
    def get_price_history(self, ticker: str, period: str = "1y", interval: str = "1d") -> tuple:
        """Chart Daten: Yahoo Finance -> Finnhub -> FMP"""
        df = pd.DataFrame()
        source = ""
        req_cols = ["open", "high", "low", "close", "volume"]

        # Intervall-Logik für Yahoo
        yf_interval = "1d"
        if period in ["1d", "5d"]: yf_interval = "5m"
        elif period == "1mo": yf_interval = "1h"

        # 1. YAHOO FINANCE
        try:
            t = yf.Ticker(ticker)
            df_yf = t.history(period=period, interval=yf_interval, auto_adjust=True)

            if df_yf is not None and not df_yf.empty:
                df_yf = _flatten_yf_columns(df_yf)
                if all(col in df_yf.columns for col in req_cols):
                    df = df_yf[req_cols].copy().dropna()
                    df.index = pd.to_datetime(df.index).tz_localize(None)
                    source = "Yahoo Finance"
        except Exception as e:
            logger.error(f"[CHART YAHOO ERROR] {ticker}: {e}")

        # 2. FINNHUB Fallback (nur Daily/1y)
        if df.empty and self.finnhub_key and period == "1y":
            try:
                end_ts = int(time.time())
                start_ts = end_ts - (365 * 24 * 3600)
                url = f"https://finnhub.io/api/v1/stock/candle?symbol={ticker}&resolution=D&from={start_ts}&to={end_ts}&token={self.finnhub_key}"
                resp = requests.get(url, timeout=5).json()

                if resp.get("s") == "ok" and resp.get("t"):
                    df = pd.DataFrame({
                        "open": resp["o"], "high": resp["h"], "low": resp["l"],
                        "close": resp["c"], "volume": resp["v"]
                    }, index=pd.to_datetime(resp["t"], unit="s"))
                    df.index.name = "date"
                    df = df.sort_index(ascending=True)
                    source = "Finnhub"
            except: pass

        # 3. FMP Fallback (nur Daily/1y)
        if df.empty and self.fmp_key and period == "1y":
            try:
                url = f"https://financialmodelingprep.com/stable/historical-price-full?symbol={ticker}&apikey={self.fmp_key}"
                resp = requests.get(url, timeout=5).json()
                if "historical" in resp and len(resp["historical"]) > 0:
                    df = pd.DataFrame(resp["historical"])
                    df['date'] = pd.to_datetime(df['date'])
                    df.set_index('date', inplace=True)
                    df = df.sort_index(ascending=True)
                    df = df.tail(252)
                    df = df[req_cols]
                    source = "FMP"
            except: pass

        return df, source

    @cached(ttl_seconds=60)
    def get_quote(self, ticker: str) -> dict:
        result = {"price": 0, "change": 0, "change_pct": 0, "name": ticker, "pe_ratio": "N/A", "source": "Unbekannt"}
        try:
            t = yf.Ticker(ticker)
            fi = t.fast_info
            price = fi.last_price
            prev = fi.previous_close
            if price and prev:
                result["price"] = price
                result["change"] = price - prev
                result["change_pct"] = (price - prev) / prev
                result["source"] = "Yahoo Finance"
            i = t.info
            if i:
                result["name"] = i.get("shortName", ticker)
                result["pe_ratio"] = i.get("trailingPE", "N/A")
        except: pass

        if result["price"] == 0 and self.finnhub_key:
            try:
                url_q = f"https://finnhub.io/api/v1/quote?symbol={ticker}&token={self.finnhub_key}"
                resp_q = requests.get(url_q, timeout=3).json()
                if "c" in resp_q and resp_q["c"] != 0:
                    result["price"] = resp_q.get("c", 0)
                    result["change"] = resp_q.get("d", 0)
                    result["change_pct"] = (resp_q.get("dp", 0) / 100) if resp_q.get("dp") else 0
                    result["source"] = "Finnhub"
            except: pass

        if result["pe_ratio"] in [None, "N/A", ""] and self.finnhub_key:
            try:
                url_m = f"https://finnhub.io/api/v1/stock/metric?symbol={ticker}&metric=all&token={self.finnhub_key}"
                resp_m = requests.get(url_m, timeout=3).json()
                if "metric" in resp_m:
                    pe = resp_m["metric"].get("peNormalizedAnnual", "N/A")
                    if pe not in [None, "N/A", ""]:
                        result["pe_ratio"] = pe
            except: pass

        if result["pe_ratio"] is None: result["pe_ratio"] = "N/A"
        if isinstance(result["pe_ratio"], (int, float)): result["pe_ratio"] = round(result["pe_ratio"], 2)
        return result

    @cached(ttl_seconds=600)
    def get_news(self, ticker: str) -> list:
        """Holt News von Finnhub."""
        if not self.finnhub_key: return []
        try:
            to_date = datetime.now().strftime("%Y-%m-%d")
            from_date = (datetime.now() - timedelta(days=7)).strftime("%Y-%m-%d")
            url = f"https://finnhub.io/api/v1/company-news?symbol={ticker}&from={from_date}&to={to_date}&token={self.finnhub_key}"
            resp = requests.get(url, timeout=5).json()
            
            news_list = []
            if isinstance(resp, list):
                for n in resp[:10]: # Top 10 News
                    # Filter für englische/deutsche Sprache (Finnhub ist primär EN)
                    news_list.append({
                        "headline": n.get("headline", ""),
                        "source": n.get("source", "News"),
                        "url": n.get("url", "#"),
                        "summary": n.get("summary", ""),
                        "datetime": datetime.fromtimestamp(n.get("datetime", time.time())).strftime("%d.%m.%Y %H:%M")
                    })
            return news_list
        except Exception as e:
            logger.error(f"[NEWS FEHLER] {e}")
            return []

    def get_financials(self, ticker: str): return {}
    def get_analyst_info(self, ticker: str): return {}

_client = None
def get_client():
    global _client
    if not _client: _client = OpenBBClient()
    return _client