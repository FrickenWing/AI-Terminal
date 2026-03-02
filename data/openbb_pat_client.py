"""
data/openbb_pat_client.py - OpenBB Platform Client via PAT Key
Voraussetzung: pip install openbb
               OPENBB_PAT_KEY in .env setzen

Holt:
  - Institutionelle Nachrichten
  - Earnings Überraschungen
  - Kursziel-Konsensus
"""

import os
from loguru import logger
from dotenv import load_dotenv

load_dotenv()

OPENBB_AVAILABLE = False
obb = None

try:
    from openbb import obb as _obb
    pat = os.getenv("OPENBB_PAT_KEY","")
    if pat:
        _obb.account.login(pat_token=pat)
        obb = _obb
        OPENBB_AVAILABLE = True
        logger.info("[OpenBB] PAT Login erfolgreich")
    else:
        logger.warning("[OpenBB] OPENBB_PAT_KEY nicht gesetzt – OpenBB deaktiviert")
except ImportError:
    logger.warning("[OpenBB] openbb nicht installiert: pip install openbb")
except Exception as e:
    logger.warning(f"[OpenBB] Init Fehler: {e}")


class OpenBBPATClient:
    def __init__(self):
        self.available = OPENBB_AVAILABLE

    def get_news(self, ticker: str, limit: int = 10) -> list:
        """Institutionelle News via OpenBB (benötigt PAT)."""
        if not self.available: return []
        try:
            data = obb.news.company(symbol=ticker, limit=limit)
            news = []
            for item in data.results:
                news.append({
                    "headline": item.title,
                    "source":   item.source or "OpenBB",
                    "url":      item.url or "#",
                    "date":     str(item.date)[:10] if item.date else "N/A",
                    "summary":  item.text[:200] if item.text else "",
                })
            return news
        except Exception as e:
            logger.debug(f"[OpenBB News] {ticker}: {e}")
            return []

    def get_price_target(self, ticker: str) -> dict:
        """Analyst Kursziele via OpenBB."""
        if not self.available:
            return {"avg_target": "N/A", "high_target": "N/A", "low_target": "N/A"}
        try:
            data = obb.equity.estimates.price_target(symbol=ticker).results
            if data:
                targets = [float(r.price_target) for r in data if r.price_target]
                return {
                    "avg_target":  round(sum(targets)/len(targets), 2),
                    "high_target": max(targets),
                    "low_target":  min(targets),
                    "count":       len(targets),
                }
        except Exception as e:
            logger.debug(f"[OpenBB Target] {ticker}: {e}")
        return {"avg_target":"N/A","high_target":"N/A","low_target":"N/A"}

    def get_earnings_estimates(self, ticker: str) -> list:
        """EPS-Schätzungen der Analysten."""
        if not self.available: return []
        try:
            data = obb.equity.estimates.consensus(symbol=ticker).results
            estimates = []
            for r in data[:4]:
                estimates.append({
                    "period":   str(r.period) if hasattr(r,"period") else "N/A",
                    "eps_est":  r.eps_consensus if hasattr(r,"eps_consensus") else "N/A",
                    "rev_est":  r.revenue_consensus if hasattr(r,"revenue_consensus") else "N/A",
                })
            return estimates
        except Exception as e:
            logger.debug(f"[OpenBB Estimates] {ticker}: {e}")
            return []


_instance = None
def get_openbb_pat_client() -> OpenBBPATClient:
    global _instance
    if _instance is None: _instance = OpenBBPATClient()
    return _instance