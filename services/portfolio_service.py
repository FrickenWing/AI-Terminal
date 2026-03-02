"""
services/portfolio_service.py - Erweiterte Portfolio Analytics

Berechnet:
- Performance vs. S&P 500
- Sharpe Ratio
- Max Drawdown
- Value at Risk (VaR 95%)
- Korrelations-Matrix
- Sektor-Allokation
- Tägliche Returns

Verwendung:
    svc = PortfolioService()
    analytics = svc.get_full_analytics(positions)
"""

import pandas as pd
import numpy as np
from typing import Optional
from loguru import logger

from data.openbb_client import get_client
from config import RISK_FREE_RATE, TRADING_DAYS_PER_YEAR


class PortfolioService:
    """
    Berechnet erweiterte Portfolio-Metriken.

    Alle Methoden akzeptieren eine positions-Liste:
    [{"ticker": "AAPL", "qty": 10, "buy_price": 150.0, "date": "2024-01-15"}, ...]
    """

    def __init__(self):
        self.client = get_client()

    # ─────────────────────────────────────────
    # HAUPT-METHODE
    # ─────────────────────────────────────────

    def get_full_analytics(self, positions: list[dict]) -> dict:
        """
        Berechnet alle Portfolio-Metriken auf einmal.

        Returns:
            dict mit: performance, metrics, correlation, sector_alloc, benchmark
        """
        if not positions:
            return {}

        tickers = [p["ticker"] for p in positions]

        # 1. Historische Preise laden
        price_data = self._load_price_history(tickers)
        if price_data.empty:
            return {}

        # 2. Portfolio-Returns berechnen
        weights       = self._calculate_weights(positions, price_data)
        port_returns  = self._portfolio_returns(price_data, weights)
        bench_returns = self._benchmark_returns()

        # 3. Metriken berechnen
        metrics = self._calculate_metrics(port_returns)

        # 4. Benchmark-Vergleich
        benchmark = self._compare_benchmark(port_returns, bench_returns)

        # 5. Korrelation
        correlation = self._correlation_matrix(price_data)

        # 6. Sektor-Allokation
        sector_alloc = self._sector_allocation(positions, price_data)

        # 7. Kumulierte Performance-Kurve
        cum_returns   = self._cumulative_returns(port_returns)
        cum_benchmark = self._cumulative_returns(bench_returns)

        return {
            "metrics":       metrics,
            "benchmark":     benchmark,
            "correlation":   correlation,
            "sector_alloc":  sector_alloc,
            "cum_returns":   cum_returns,
            "cum_benchmark": cum_benchmark,
            "daily_returns": port_returns,
            "tickers":       tickers,
        }

    # ─────────────────────────────────────────
    # DATEN LADEN
    # ─────────────────────────────────────────

    def _load_price_history(self, tickers: list[str], period: str = "1y") -> pd.DataFrame:
        """Lädt Close-Preise für alle Ticker als DataFrame (Spalten = Ticker)."""
        dfs = {}
        for ticker in tickers:
            try:
                df = self.client.get_price_history(ticker, period, "1d")
                if not df.empty:
                    dfs[ticker] = df["close"]
            except Exception as e:
                logger.warning(f"Preisverlauf für {ticker} fehlgeschlagen: {e}")

        if not dfs:
            return pd.DataFrame()

        combined = pd.DataFrame(dfs)
        combined.index = pd.to_datetime(combined.index)
        return combined.dropna(how="all")

    def _benchmark_returns(self, symbol: str = "^GSPC") -> pd.Series:
        """S&P 500 als Benchmark laden."""
        try:
            df = self.client.get_price_history(symbol, "1y", "1d")
            if df.empty:
                return pd.Series(dtype=float)
            returns = df["close"].pct_change().dropna()
            returns.name = "SP500"
            return returns
        except Exception:
            return pd.Series(dtype=float)

    # ─────────────────────────────────────────
    # BERECHNUNG
    # ─────────────────────────────────────────

    def _calculate_weights(self, positions: list[dict], price_data: pd.DataFrame) -> dict:
        """Berechnet Portfolio-Gewichtung nach aktuellem Marktwert."""
        weights = {}
        total = 0.0

        for pos in positions:
            ticker = pos["ticker"]
            if ticker not in price_data.columns:
                continue
            last_price = price_data[ticker].iloc[-1]
            mv = pos["qty"] * last_price
            weights[ticker] = mv
            total += mv

        if total == 0:
            n = len(weights)
            return {t: 1/n for t in weights} if n > 0 else {}

        return {t: v/total for t, v in weights.items()}

    def _portfolio_returns(self, price_data: pd.DataFrame, weights: dict) -> pd.Series:
        """Berechnet gewichtete tägliche Portfolio-Returns."""
        daily_returns = price_data.pct_change().dropna()

        # Nur Ticker mit Gewicht
        valid = {t: w for t, w in weights.items() if t in daily_returns.columns}
        if not valid:
            return pd.Series(dtype=float)

        weighted = sum(daily_returns[t] * w for t, w in valid.items())
        weighted.name = "Portfolio"
        return weighted

    def _cumulative_returns(self, returns: pd.Series) -> pd.Series:
        """Kumulierte Returns: (1+r).cumprod() - 1"""
        if returns.empty:
            return pd.Series(dtype=float)
        return (1 + returns).cumprod() - 1

    def _calculate_metrics(self, returns: pd.Series) -> dict:
        """
        Berechnet alle wichtigen Risiko-/Rendite-Kennzahlen.

        Returns:
            dict mit: total_return, annualized_return, volatility,
                      sharpe_ratio, max_drawdown, var_95, best_day, worst_day
        """
        if returns.empty or len(returns) < 5:
            return {}

        r = returns.dropna()
        n = len(r)

        # Gesamtrendite
        total_return = (1 + r).prod() - 1

        # Annualisierte Rendite
        years = n / TRADING_DAYS_PER_YEAR
        ann_return = (1 + total_return) ** (1 / years) - 1 if years > 0 else 0

        # Volatilität (annualisiert)
        volatility = r.std() * np.sqrt(TRADING_DAYS_PER_YEAR)

        # Sharpe Ratio
        daily_rf   = RISK_FREE_RATE / TRADING_DAYS_PER_YEAR
        excess     = r - daily_rf
        sharpe     = (excess.mean() / r.std() * np.sqrt(TRADING_DAYS_PER_YEAR)
                      if r.std() > 0 else 0)

        # Max Drawdown
        cumulative = (1 + r).cumprod()
        rolling_max = cumulative.expanding().max()
        drawdowns   = (cumulative - rolling_max) / rolling_max
        max_drawdown = drawdowns.min()

        # Value at Risk (95% – historische Simulation)
        var_95 = np.percentile(r, 5)

        # Calmar Ratio (Ann. Return / |Max Drawdown|)
        calmar = (ann_return / abs(max_drawdown)
                  if max_drawdown != 0 else 0)

        # Win-Rate
        win_rate = (r > 0).sum() / len(r)

        return {
            "total_return":      float(total_return),
            "ann_return":        float(ann_return),
            "volatility":        float(volatility),
            "sharpe_ratio":      float(sharpe),
            "max_drawdown":      float(max_drawdown),
            "var_95":            float(var_95),
            "calmar_ratio":      float(calmar),
            "win_rate":          float(win_rate),
            "best_day":          float(r.max()),
            "worst_day":         float(r.min()),
            "avg_daily_return":  float(r.mean()),
            "trading_days":      n,
        }

    def _compare_benchmark(
        self, port_returns: pd.Series, bench_returns: pd.Series
    ) -> dict:
        """Vergleicht Portfolio mit S&P 500 Benchmark."""
        if port_returns.empty or bench_returns.empty:
            return {}

        # Auf gemeinsamen Zeitraum einschränken
        common = port_returns.index.intersection(bench_returns.index)
        if len(common) < 5:
            return {}

        p = port_returns.loc[common]
        b = bench_returns.loc[common]

        port_total  = (1 + p).prod() - 1
        bench_total = (1 + b).prod() - 1
        alpha       = port_total - bench_total

        # Beta (Kovarianz / Varianz des Benchmark)
        if b.var() > 0:
            beta = p.cov(b) / b.var()
        else:
            beta = 1.0

        # Korrelation mit Benchmark
        correlation = p.corr(b)

        return {
            "port_return":   float(port_total),
            "bench_return":  float(bench_total),
            "alpha":         float(alpha),
            "beta":          float(beta),
            "correlation":   float(correlation),
            "outperformed":  port_total > bench_total,
        }

    def _correlation_matrix(self, price_data: pd.DataFrame) -> Optional[pd.DataFrame]:
        """Korrelations-Matrix der täglichen Returns."""
        if price_data.empty or len(price_data.columns) < 2:
            return None
        try:
            returns = price_data.pct_change().dropna()
            return returns.corr().round(3)
        except Exception:
            return None

    def _sector_allocation(
        self, positions: list[dict], price_data: pd.DataFrame
    ) -> list[dict]:
        """Berechnet Sektor-Allokation basierend auf aktuellem Marktwert."""
        import yfinance as yf

        sector_data = {}
        total_value = 0.0

        for pos in positions:
            ticker = pos["ticker"]
            try:
                info   = yf.Ticker(ticker).info
                sector = info.get("sector", "Unbekannt")
                if ticker in price_data.columns:
                    last_price = price_data[ticker].iloc[-1]
                else:
                    last_price = pos["buy_price"]
                mv = pos["qty"] * last_price
                sector_data[sector] = sector_data.get(sector, 0) + mv
                total_value += mv
            except Exception:
                sector_data["Unbekannt"] = sector_data.get("Unbekannt", 0) + (
                    pos["qty"] * pos["buy_price"]
                )
                total_value += pos["qty"] * pos["buy_price"]

        if total_value == 0:
            return []

        return [
            {
                "sector": sector,
                "value":  value,
                "weight": value / total_value,
            }
            for sector, value in sorted(sector_data.items(), key=lambda x: -x[1])
        ]

    # ─────────────────────────────────────────
    # EINZELNE METRIKEN (für schnellen Zugriff)
    # ─────────────────────────────────────────

    def get_sharpe_ratio(self, positions: list[dict]) -> Optional[float]:
        """Nur Sharpe Ratio berechnen."""
        tickers      = [p["ticker"] for p in positions]
        price_data   = self._load_price_history(tickers)
        if price_data.empty:
            return None
        weights      = self._calculate_weights(positions, price_data)
        port_returns = self._portfolio_returns(price_data, weights)
        metrics      = self._calculate_metrics(port_returns)
        return metrics.get("sharpe_ratio")

    def get_var(self, positions: list[dict], confidence: float = 0.95) -> Optional[float]:
        """Value at Risk (historische Simulation)."""
        tickers      = [p["ticker"] for p in positions]
        price_data   = self._load_price_history(tickers)
        if price_data.empty:
            return None
        weights      = self._calculate_weights(positions, price_data)
        port_returns = self._portfolio_returns(price_data, weights)
        if port_returns.empty:
            return None
        return float(np.percentile(port_returns.dropna(), (1 - confidence) * 100))


# Singleton
_portfolio_service_instance = None

def get_portfolio_service() -> PortfolioService:
    global _portfolio_service_instance
    if _portfolio_service_instance is None:
        _portfolio_service_instance = PortfolioService()
    return _portfolio_service_instance
