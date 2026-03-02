"""
data/trends_client.py - Google Trends Integration
Nutzt pytrends für Interest-over-Time und Related Queries.
pip install pytrends
"""

import time
from loguru import logger

try:
    from pytrends.request import TrendReq
    PYTRENDS_AVAILABLE = True
except ImportError:
    PYTRENDS_AVAILABLE = False
    logger.warning("[Trends] pytrends nicht installiert: pip install pytrends")


class TrendsClient:
    def __init__(self):
        self._pytrends = None
        self._last_request = 0
        self.MIN_INTERVAL = 2.0   # Sekunden zwischen Requests (Rate Limit)

    def _get_pytrends(self):
        if not PYTRENDS_AVAILABLE: return None
        if self._pytrends is None:
            self._pytrends = TrendReq(hl="en-US", tz=360, timeout=(10, 25))
        return self._pytrends

    def _throttle(self):
        elapsed = time.time() - self._last_request
        if elapsed < self.MIN_INTERVAL:
            time.sleep(self.MIN_INTERVAL - elapsed)
        self._last_request = time.time()

    def get_interest(self, ticker: str, company_name: str = "") -> dict:
        """
        Gibt Google Trends Interest-over-Time zurück (letzte 90 Tage).

        Returns:
            current_interest  int    Aktueller Wert (0-100)
            peak_interest     int    Max in letzten 90 Tagen
            avg_interest      float  Durchschnitt
            trend_direction   str    "Rising" / "Falling" / "Stable"
            interest_over_time list  [{date, value}, ...]
            related_queries   list   Top verwandte Suchanfragen
        """
        pt = self._get_pytrends()
        if not pt:
            return {"error": "pytrends nicht installiert", "current_interest": 0}

        keywords = [ticker]
        if company_name:
            keywords = [ticker, company_name[:40]]  # Beide probieren

        try:
            self._throttle()
            pt.build_payload(keywords[:1], cat=7,   # Cat 7 = Finance
                             timeframe="today 3-m", geo="")
            df = pt.interest_over_time()

            if df.empty:
                return {"error": "Keine Daten", "current_interest": 0}

            col  = keywords[0]
            vals = df[col].tolist()
            dates = [str(d.date()) for d in df.index.tolist()]

            current = vals[-1]  if vals else 0
            peak    = max(vals) if vals else 0
            avg     = round(sum(vals)/len(vals), 1) if vals else 0

            # Trend: Vergleiche letzte 2 Wochen mit vorletzten 2 Wochen
            mid = len(vals) // 2
            first_half  = sum(vals[:mid]) / max(mid, 1)
            second_half = sum(vals[mid:]) / max(len(vals)-mid, 1)
            direction   = ("Rising 📈" if second_half > first_half*1.1
                           else "Falling 📉" if second_half < first_half*0.9
                           else "Stable ➡️")

            # Related Queries
            related = []
            try:
                self._throttle()
                rq = pt.related_queries()
                top_df = rq.get(col, {}).get("top")
                if top_df is not None and not top_df.empty:
                    related = top_df["query"].tolist()[:8]
            except: pass

            return {
                "current_interest":    current,
                "peak_interest":       peak,
                "avg_interest":        avg,
                "trend_direction":     direction,
                "interest_over_time":  [{"date": d, "value": v}
                                        for d, v in zip(dates, vals)],
                "related_queries":     related,
            }

        except Exception as e:
            logger.warning(f"[Trends] '{ticker}': {e}")
            return {"error": str(e), "current_interest": 0}

    def compare_tickers(self, tickers: list) -> dict:
        """Vergleicht bis zu 5 Ticker im Trends-Interesse."""
        pt = self._get_pytrends()
        if not pt or not tickers: return {}
        try:
            self._throttle()
            kw = tickers[:5]
            pt.build_payload(kw, cat=7, timeframe="today 3-m", geo="")
            df = pt.interest_over_time()
            if df.empty: return {}
            return {t: int(df[t].mean()) for t in kw if t in df.columns}
        except Exception as e:
            logger.debug(f"[Trends compare] {e}")
            return {}


_instance = None
def get_trends_client() -> TrendsClient:
    global _instance
    if _instance is None: _instance = TrendsClient()
    return _instance