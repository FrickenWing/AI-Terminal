"""
services/technical_analysis_service.py - Technische Analyse mit Scoring
"""
import pandas as pd
import numpy as np
from typing import Dict, Any, Optional
from data.openbb_client import get_client
from indicators.technical import TechnicalIndicators


class TechnicalAnalysisService:
    def __init__(self):
        self.client = get_client()

    def get_price_data(self, ticker: str, period: str = "3mo", interval: str = "1d") -> pd.DataFrame:
        """Holt Kursdaten und berechnet Indikatoren."""
        df = self.client.get_price_history(ticker, period=period, interval=interval)
        if df.empty:
            return pd.DataFrame()

        # Technische Indikatoren hinzufügen
        ti = TechnicalIndicators(df)
        ti.add_sma([20, 50, 200])
        ti.add_ema([9, 21])
        ti.add_rsi(14)
        ti.add_macd(12, 26, 9)
        ti.add_bollinger_bands(20, 2)
        ti.add_atr(14)
        ti.add_obv()
        ti.add_volume_ma(20)

        return ti.df.dropna()

    def analyze_indicators(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Analysiert alle Indikatoren und erstellt ein Signal."""
        if df.empty:
            return {}

        latest = df.iloc[-1]
        prev = df.iloc[-2] if len(df) > 1 else latest

        signals = {}
        score = 0
        total_indicators = 0

        # RSI (0-100)
        rsi = latest.get('rsi', 50)
        if rsi < 30:
            signals['rsi'] = {'value': rsi, 'signal': 'BUY', 'reason': 'RSI oversold'}
            score += 1
        elif rsi > 70:
            signals['rsi'] = {'value': rsi, 'signal': 'SELL', 'reason': 'RSI overbought'}
            score -= 1
        else:
            signals['rsi'] = {'value': rsi, 'signal': 'NEUTRAL', 'reason': 'RSI neutral'}
        total_indicators += 1

        # MACD
        macd = latest.get('macd', 0)
        macd_signal = latest.get('macd_signal', 0)
        macd_hist = latest.get('macd_hist', 0)
        prev_hist = prev.get('macd_hist', 0)

        if macd > macd_signal and macd_hist > prev_hist:
            signals['macd'] = {'value': macd, 'signal': 'BUY', 'reason': 'MACD crosses up'}
            score += 1
        elif macd < macd_signal and macd_hist < prev_hist:
            signals['macd'] = {'value': macd, 'signal': 'SELL', 'reason': 'MACD crosses down'}
            score -= 1
        else:
            signals['macd'] = {'value': macd, 'signal': 'NEUTRAL', 'reason': 'MACD neutral'}
        total_indicators += 1

        # Bollinger Bands
        price = latest.get('close', 0)
        bb_upper = latest.get('bb_upper', price)
        bb_lower = latest.get('bb_lower', price)
        bb_middle = latest.get('bb_middle', price)

        if price < bb_lower:
            signals['bb'] = {'value': price, 'signal': 'BUY', 'reason': 'Price below lower BB'}
            score += 1
        elif price > bb_upper:
            signals['bb'] = {'value': price, 'signal': 'SELL', 'reason': 'Price above upper BB'}
            score -= 1
        else:
            # Check if near bands
            if price > bb_middle:
                signals['bb'] = {'value': price, 'signal': 'NEUTRAL', 'reason': 'Price in upper half of BB'}
            else:
                signals['bb'] = {'value': price, 'signal': 'NEUTRAL', 'reason': 'Price in lower half of BB'}
        total_indicators += 1

        # SMA Crossovers
        sma_20 = latest.get('sma_20', 0)
        sma_50 = latest.get('sma_50', 0)
        sma_200 = latest.get('sma_200', 0)
        prev_sma_20 = prev.get('sma_20', 0)
        prev_sma_50 = prev.get('sma_50', 0)

        if sma_20 > sma_50 and prev_sma_20 <= prev_sma_50:
            signals['sma'] = {'value': f"20>{50}", 'signal': 'BUY', 'reason': 'SMA 20 crossed above SMA 50'}
            score += 1
        elif sma_20 < sma_50 and prev_sma_20 >= prev_sma_50:
            signals['sma'] = {'value': f"20<{50}", 'signal': 'SELL', 'reason': 'SMA 20 crossed below SMA 50'}
            score -= 1
        else:
            if sma_20 > sma_50:
                signals['sma'] = {'value': f"20>{50}", 'signal': 'NEUTRAL', 'reason': 'SMA 20 above SMA 50'}
            else:
                signals['sma'] = {'value': f"20<{50}", 'signal': 'NEUTRAL', 'reason': 'SMA 20 below SMA 50'}
        total_indicators += 1

        # Trend (SMA 200)
        if sma_50 > sma_200 and sma_20 > sma_50:
            signals['trend'] = {'value': 'UPTREND', 'signal': 'BUY', 'reason': 'Above SMA 200'}
            score += 1
        elif sma_50 < sma_200 and sma_20 < sma_50:
            signals['trend'] = {'value': 'DOWNTREND', 'signal': 'SELL', 'reason': 'Below SMA 200'}
            signals['trend'] = {'value': 'DOWNTREND', 'signal': 'SELL', 'reason': 'Below SMA 200'}
            score -= 1
        else:
            signals['trend'] = {'value': 'SIDEWAYS', 'signal': 'NEUTRAL', 'reason': 'No clear trend'}
        total_indicators += 1

        # Volume
        vol = latest.get('volume', 0)
        vol_ma = latest.get('volume_ma', 1)
        if vol > vol_ma * 1.5:
            signals['volume'] = {'value': vol, 'signal': 'BUY', 'reason': 'High volume'}
            score += 0.5  # Volume is secondary
        elif vol < vol_ma * 0.5:
            signals['volume'] = {'value': vol, 'signal': 'NEUTRAL', 'reason': 'Low volume'}
        else:
            signals['volume'] = {'value': vol, 'signal': 'NEUTRAL', 'reason': 'Normal volume'}
        total_indicators += 1

        # Calculate final score (-100 to +100)
        normalized_score = (score / total_indicators) * 100

        return {
            'signals': signals,
            'score': round(normalized_score, 1),
            'buy_signals': sum(1 for s in signals.values() if s['signal'] == 'BUY'),
            'sell_signals': sum(1 for s in signals.values() if s['signal'] == 'SELL'),
            'neutral_signals': sum(1 for s in signals.values() if s['signal'] == 'NEUTRAL'),
            'latest_data': {
                'price': latest.get('close'),
                'rsi': rsi,
                'macd': macd,
                'macd_signal': macd_signal,
                'sma_20': sma_20,
                'sma_50': sma_50,
                'sma_200': sma_200,
                'volume': vol,
                'volume_ma': vol_ma,
                'atr': latest.get('atr'),
            }
        }

    def prepare_gemini_prompt(self, ticker: str, analysis: Dict[str, Any]) -> str:
        """Erstellt den Prompt für Gemini."""
        if not analysis:
            return ""

        data = analysis['latest_data']
        signals = analysis['signals']

        prompt = f"""Analysiere die technische Analyse für {ticker}.

AKTUELLE DATEN:
- Preis: ${data.get('price', 0):.2f}
- RSI (14): {data.get('rsi', 0):.1f}
- MACD: {data.get('macd', 0):.4f}
- MACD Signal: {data.get('macd_signal', 0):.4f}
- SMA 20: ${data.get('sma_20', 0):.2f}
- SMA 50: ${data.get('sma_50', 0):.2f}
- SMA 200: ${data.get('sma_200', 0):.2f}
- Volumen: {data.get('volume', 0):,.0f}
- ATR: {data.get('atr', 0):.2f}

INDIKATOR SIGNALE:
"""

        for name, signal in signals.items():
            prompt += f"- {name}: {signal['signal']} - {signal['reason']}\n"

        prompt += f"""
BERECHNETER SCORE: {analysis['score']} (von -100 bis +100)
Buy-Signale: {analysis['buy_signals']}
Sell-Signale: {analysis['sell_signals']}
Neutral: {analysis['neutral_signals']}

AUFGABE:
1. Gib eine kurze Zusammenfassung der technischen Situation (max 2 Sätze)
2. Bewerte den Score und erkläre warum
3. Gib eine klare Handlungsempfehlung: BUY, SELL oder HOLD
4. Nenne die wichtigsten Unterstützungs- und Widerstandsniveaus

Antworte auf Deutsch.
"""

        return prompt


# --- SINGLETON PATTERN ---
_analysis_service_instance = None


def get_technical_analysis_service() -> TechnicalAnalysisService:
    global _analysis_service_instance
    if _analysis_service_instance is None:
        _analysis_service_instance = TechnicalAnalysisService()
    return _analysis_service_instance
