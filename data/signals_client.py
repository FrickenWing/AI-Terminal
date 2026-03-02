"""
data/signals_client.py - Market Signals Engine
Aggregiert:
  - CNN Fear & Greed Index (alternative.me API)
  - Finviz Short Interest
  - Earnings Calendar (Finnhub)
  - Analyst Ratings (Yahoo Finance + Finnhub)
  - Macro Signals (VIX, Dollar Index via yfinance)
"""

import os, requests, time
import yfinance as yf
from loguru import logger
from datetime import datetime, timedelta
from dotenv import load_dotenv

load_dotenv()


class SignalsClient:
    def __init__(self):
        self.finnhub_key = os.getenv("FINNHUB_API_KEY","")
        self.session     = requests.Session()
        self.session.headers.update({"User-Agent": "AI-Analyst/3.1"})

    # ── Fear & Greed ─────────────────────────────────────────────────────────

    def get_fear_greed(self) -> dict:
        """
        CNN Fear & Greed Index via alternative.me (free).
        Returns: value (0-100), label, previous_close, previous_1_week
        """
        try:
            resp = self.session.get(
                "https://api.alternative.me/fng/?limit=8&format=json",
                timeout=6)
            if resp.status_code == 200:
                data = resp.json().get("data", [])
                if data:
                    current = data[0]
                    prev_week = data[-1] if len(data) >= 7 else data[-1]
                    val = int(current.get("value", 0))
                    return {
                        "value":         val,
                        "label":         current.get("value_classification","N/A"),
                        "previous_close": int(data[1]["value"]) if len(data) > 1 else val,
                        "previous_week":  int(prev_week["value"]),
                        "trend":          ("Rising" if val > int(data[1]["value"] if len(data)>1 else val)
                                           else "Falling"),
                        "emoji":          ("😱" if val<25 else "😰" if val<45
                                           else "😐" if val<55 else "🤑" if val<75 else "🚀"),
                    }
        except Exception as e:
            logger.debug(f"[FearGreed] {e}")
        return {"value": 0, "label": "Unbekannt", "emoji": "❓"}

    # ── Macro Signals (VIX, DXY, TNX) ───────────────────────────────────────

    def get_macro_signals(self) -> dict:
        """
        Holt VIX (Volatilitätsindex), 10Y Treasury Yield, Dollar Index.
        """
        signals = {}
        macro_tickers = {
            "VIX":  "^VIX",
            "DXY":  "DX-Y.NYB",
            "TNX":  "^TNX",     # 10Y Treasury
            "SP500": "^GSPC",
        }
        for label, sym in macro_tickers.items():
            try:
                t    = yf.Ticker(sym)
                fi   = t.fast_info
                price = fi.last_price
                prev  = fi.previous_close
                if price and prev:
                    signals[label] = {
                        "value":      round(price, 2),
                        "change_pct": round((price - prev) / prev * 100, 2),
                    }
            except: pass
        return signals

    # ── Earnings Calendar ────────────────────────────────────────────────────

    def get_earnings_calendar(self, ticker: str) -> dict:
        """Nächster Earnings-Termin + letztes EPS Ergebnis."""
        result = {"next_date": "N/A", "estimate": "N/A", "actual": "N/A",
                  "surprise_pct": "N/A", "quarter": "N/A"}
        try:
            t = yf.Ticker(ticker)
            cal = t.calendar
            if cal is not None and not cal.empty:
                dates = cal.get("Earnings Date")
                if dates is not None and len(dates) > 0:
                    result["next_date"] = str(dates[0].date()) if hasattr(dates[0],"date") else str(dates[0])
        except: pass

        if self.finnhub_key:
            try:
                to_date   = datetime.now().strftime("%Y-%m-%d")
                from_date = (datetime.now() - timedelta(days=90)).strftime("%Y-%m-%d")
                url  = (f"https://finnhub.io/api/v1/stock/earnings?"
                        f"symbol={ticker}&token={self.finnhub_key}")
                data = requests.get(url, timeout=5).json()
                if isinstance(data, list) and data:
                    last = data[0]
                    result.update({
                        "quarter":      last.get("period","N/A"),
                        "estimate":     last.get("estimate","N/A"),
                        "actual":       last.get("actual","N/A"),
                        "surprise_pct": round(last.get("surprisePercent",0),2),
                    })
            except: pass
        return result

    # ── Analyst Ratings ──────────────────────────────────────────────────────

    def get_analyst_ratings(self, ticker: str) -> dict:
        """Aggregiert Analyst-Ratings aus Yahoo + Finnhub."""
        result = {"buy":0,"hold":0,"sell":0,"avg_target":0,"recommendation":"N/A"}
        try:
            info = yf.Ticker(ticker).info
            result.update({
                "buy":            info.get("numberOfAnalystOpinions", 0),
                "avg_target":     round(info.get("targetMeanPrice", 0), 2),
                "recommendation": info.get("recommendationKey","N/A").upper(),
            })
        except: pass

        if self.finnhub_key:
            try:
                url  = f"https://finnhub.io/api/v1/stock/recommendation?symbol={ticker}&token={self.finnhub_key}"
                data = requests.get(url, timeout=5).json()
                if isinstance(data, list) and data:
                    latest = data[0]
                    result.update({
                        "buy":    latest.get("buy",0) + latest.get("strongBuy",0),
                        "hold":   latest.get("hold",0),
                        "sell":   latest.get("sell",0) + latest.get("strongSell",0),
                        "period": latest.get("period","N/A"),
                    })
            except: pass
        return result

    # ── News Sentiment (Finnhub) ─────────────────────────────────────────────

    def get_news_sentiment(self, ticker: str) -> dict:
        """Aggregierter Nachrichten-Sentiment via Finnhub."""
        if not self.finnhub_key:
            return {"score": 0.0, "buzz": 0, "label": "N/A"}
        try:
            url  = f"https://finnhub.io/api/v1/news-sentiment?symbol={ticker}&token={self.finnhub_key}"
            data = requests.get(url, timeout=5).json()
            if "sentiment" in data:
                bull = data["sentiment"].get("bullishPercent", 0)
                bear = data["sentiment"].get("bearishPercent", 0)
                score = bull - bear
                return {
                    "score":         round(score, 3),
                    "bullish_pct":   round(bull*100, 1),
                    "bearish_pct":   round(bear*100, 1),
                    "label":         "Positiv" if score>0.1 else "Negativ" if score<-0.1 else "Neutral",
                    "buzz":          data.get("buzz",{}).get("articlesInLastWeek", 0),
                    "weekly_average": data.get("buzz",{}).get("weeklyAverage",0),
                }
        except Exception as e:
            logger.debug(f"[NewsSentiment] {e}")
        return {"score":0.0,"buzz":0,"label":"N/A"}


_instance = None
def get_signals_client() -> SignalsClient:
    global _instance
    if _instance is None: _instance = SignalsClient()
    return _instance