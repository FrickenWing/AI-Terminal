"""
services/omni_data_service.py - Omni-Data Engine
══════════════════════════════════════════════════
Aggregiert alle Datenquellen parallel (concurrent.futures) und
gibt ein vollständiges OmniBundle zurück. Beinhaltet jetzt
auch die Multi-Agenten Logik.
"""

import time
from concurrent.futures import ThreadPoolExecutor, as_completed, TimeoutError
from loguru import logger

from data.openbb_client    import get_client
from data.reddit_client    import get_reddit_client
from data.trends_client    import get_trends_client
from data.sec_client       import get_sec_client
from data.signals_client   import get_signals_client
from data.openbb_pat_client import get_openbb_pat_client
# NEU v4.0: Technical Service Import für Agenten
from services.technical_analysis_service import get_technical_analysis_service

class OmniDataService:
    FETCH_TIMEOUT = 12  # Sekunden pro Quelle

    def __init__(self):
        self.yf_client     = get_client()
        self.reddit        = get_reddit_client()
        self.trends        = get_trends_client()
        self.sec           = get_sec_client()
        self.signals       = get_signals_client()
        self.openbb        = get_openbb_pat_client()
        self.tech          = get_technical_analysis_service()

    # ── Haupt-Methode (v3.1 Original) ─────────────────────────────────────────
    def get_bundle(self, ticker: str) -> dict:
        ticker = ticker.upper()
        bundle = {
            "ticker":    ticker,
            "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
            "errors":    [],
        }

        # Tasks inklusive technischer Analyse für den neuen Technical-Agenten
        tasks = {
            "quote":          lambda: self.yf_client.get_quote(ticker),
            "news":           lambda: self.yf_client.get_news(ticker),
            "reddit":         lambda: self.reddit.get_ticker_sentiment(ticker),
            "trends":         lambda: self.trends.get_interest(ticker),
            "insider":        lambda: self.sec.get_insider_summary(ticker),
            "filings":        lambda: self.sec.get_recent_filings(ticker),
            "fear_greed":     lambda: self.signals.get_fear_greed(),
            "macro":          lambda: self.signals.get_macro_signals(),
            "earnings":       lambda: self.signals.get_earnings_calendar(ticker),
            "analyst":        lambda: self.signals.get_analyst_ratings(ticker),
            "news_sentiment": lambda: self.signals.get_news_sentiment(ticker),
            "openbb_news":    lambda: self.openbb.get_news(ticker),
            "price_target":   lambda: self.openbb.get_price_target(ticker),
            "tech":           lambda: self.tech.analyze_indicators(self.tech.get_price_data(ticker)) # v4.0 addition
        }

        # Parallel ausführen
        with ThreadPoolExecutor(max_workers=10) as executor:
            future_to_key = {
                executor.submit(fn): key
                for key, fn in tasks.items()
            }
            for future in as_completed(future_to_key, timeout=self.FETCH_TIMEOUT + 5):
                key = future_to_key[future]
                try:
                    bundle[key] = future.result(timeout=self.FETCH_TIMEOUT)
                except TimeoutError:
                    bundle[key] = {}
                    bundle["errors"].append(f"{key}: Timeout")
                    logger.warning(f"[Omni] Timeout: {key}")
                except Exception as e:
                    bundle[key] = {}
                    bundle["errors"].append(f"{key}: {str(e)[:80]}")
                    logger.warning(f"[Omni] Fehler {key}: {e}")

        for key in tasks:
            if key not in bundle:
                bundle[key] = {}

        return bundle

    # ── Prompt Builder (v3.1 Original) ────────────────────────────────────────
    def build_llm_prompt(self, bundle: dict, analysis_focus: str = "full") -> str:
        """Originaler strukturierter Mega-Prompt für Legacy /api/omni/analyze"""
        t       = bundle.get("ticker","N/A")
        q       = bundle.get("quote",{})
        reddit  = bundle.get("reddit",{})
        trends  = bundle.get("trends",{})
        insider = bundle.get("insider",{})
        fg      = bundle.get("fear_greed",{})
        macro   = bundle.get("macro",{})
        earn    = bundle.get("earnings",{})
        analyst = bundle.get("analyst",{})
        news_s  = bundle.get("news_sentiment",{})
        filings = bundle.get("filings",[])
        news    = bundle.get("news",[])
        target  = bundle.get("price_target",{})

        prompt = f"""Du bist ein institutioneller Finanzanalyst. Analysiere {t} auf Basis der folgenden Omni-Data-Streams.

════════════════════════════════════════════════════════
📊 MARKTDATEN (Echtzeit)
════════════════════════════════════════════════════════
Ticker:         {t}
Kurs:           ${q.get('price', 'N/A')}
Tagesänderung:  {round(q.get('change_pct',0)*100,2):+.2f}%
KGV (P/E):      {q.get('pe_ratio','N/A')}
Datenquelle:    {q.get('source','N/A')}

════════════════════════════════════════════════════════
🧠 SOCIAL MEDIA & RETAIL SENTIMENT
════════════════════════════════════════════════════════
Reddit Mentions (7 Tage):  {reddit.get('mentions', 0)} Posts
Sentiment-Score:           {reddit.get('sentiment', 0):+.3f} ({reddit.get('sentiment_label','N/A')})
Bullish Posts:             {reddit.get('bullish_pct', 0):.1f}%
Bearish Posts:             {reddit.get('bearish_pct', 0):.1f}%
Subreddits: {', '.join(f"{k}:{v}" for k,v in reddit.get('subreddit_breakdown',{}).items() if v>0)}
"""

        if reddit.get("top_posts"):
            prompt += "\nTop Reddit Posts:\n"
            for p in reddit["top_posts"][:3]:
                prompt += f"  • [{p.get('subreddit','')}] \"{p.get('title','')[:80]}\" (Score: {p.get('score',0)}, Sentiment: {p.get('post_sentiment',0):+.2f})\n"

        prompt += f"""
════════════════════════════════════════════════════════
🔍 GOOGLE TRENDS (Suchinteresse, 90 Tage)
════════════════════════════════════════════════════════
Aktuelles Interesse:  {trends.get('current_interest', 'N/A')}/100
Peak (90d):           {trends.get('peak_interest', 'N/A')}/100
Durchschnitt:         {trends.get('avg_interest', 'N/A')}/100
Trend-Richtung:       {trends.get('trend_direction', 'N/A')}
"""
        if trends.get("related_queries"):
            prompt += f"Verwandte Suchanfragen: {', '.join(trends['related_queries'][:5])}\n"

        prompt += f"""
════════════════════════════════════════════════════════
📰 NEWS SENTIMENT (Aggregiert)
════════════════════════════════════════════════════════
News Sentiment:       {news_s.get('label','N/A')} (Score: {news_s.get('score',0):+.3f})
Bullish:              {news_s.get('bullish_pct',0):.1f}%
Bearish:              {news_s.get('bearish_pct',0):.1f}%
Artikel (7 Tage):     {news_s.get('buzz', 0)}
"""

        if news:
            prompt += "\nAktuelle Schlagzeilen:\n"
            for n in news[:4]:
                prompt += f"  • [{n.get('source','')}] {n.get('headline','')[:100]}\n"

        prompt += f"""
════════════════════════════════════════════════════════
🏛️ SEC INSIDER TRADING
════════════════════════════════════════════════════════
CIK:                {insider.get('cik','N/A')}
Form-4 Filings (recent): {insider.get('form4_count_recent', 0)}
Letztes Filing:     {insider.get('last_filing','N/A')}
Aktivitätssignal:   {insider.get('signal','N/A')}
"""

        if filings:
            prompt += "\nAktuelle SEC Filings:\n"
            for f in filings[:4]:
                prompt += f"  • {f.get('form','')} ({f.get('date','')}) – {f.get('document','')[:60]}\n"

        prompt += f"""
════════════════════════════════════════════════════════
😱 MARKTSENTIMENT & MAKRO
════════════════════════════════════════════════════════
Fear & Greed Index:   {fg.get('value','N/A')}/100 – {fg.get('label','N/A')} {fg.get('emoji','')}
Trend F&G:            {fg.get('trend','N/A')}
Vorwoche F&G:         {fg.get('previous_week','N/A')}/100

Makro-Daten:
"""
        for key, vals in macro.items():
            prompt += f"  {key}: {vals.get('value','N/A')} ({vals.get('change_pct',0):+.2f}%)\n"

        prompt += f"""
════════════════════════════════════════════════════════
📅 EARNINGS & ANALYST KONSENSUS
════════════════════════════════════════════════════════
Nächstes Earnings:    {earn.get('next_date','N/A')}
Letztes Quartal:      {earn.get('quarter','N/A')}
EPS Schätzung:        {earn.get('estimate','N/A')}
EPS Actual:           {earn.get('actual','N/A')}
Surprise:             {earn.get('surprise_pct','N/A')}%

Analyst Ratings:
  Buy:                {analyst.get('buy', 0)}
  Hold:               {analyst.get('hold', 0)}
  Sell:               {analyst.get('sell', 0)}
  Empfehlung:         {analyst.get('recommendation','N/A')}
  Ø Kursziel:         ${analyst.get('avg_target',0)}
"""
        if target.get("avg_target") not in ["N/A", 0]:
            prompt += f"  OpenBB Kursziel:      Ø ${target.get('avg_target')} (Low: ${target.get('low_target')}, High: ${target.get('high_target')})\n"

        prompt += f"""
════════════════════════════════════════════════════════
📋 ANALYSEANWEISUNG
════════════════════════════════════════════════════════
Timestamp: {bundle.get('timestamp','N/A')}
Fehlende Quellen: {', '.join(bundle.get('errors',[])) or 'Keine'}

AUFGABE:
1. **Executive Summary** (3-4 Sätze): Gesamtlage auf Basis aller Datenquellen
2. **Sentiment-Analyse**: Bewerte Social Media, News und Insider-Signal zusammen
3. **Risiko-Faktoren**: Identifiziere die 3 größten Risiken anhand der Daten
4. **Katalysatoren**: Was könnte den Kurs positiv beeinflussen?
5. **Handlungsempfehlung**: KAUFEN / HALTEN / VERKAUFEN mit klarer Begründung
6. **Kursziel-Range**: Kurz- (4W) und mittelfristig (3M) auf Basis der Analyst-Daten

Antworte strukturiert auf Deutsch mit Markdown-Formatierung.
Keine allgemeinen Floskeln – nur datenbasierte, spezifische Aussagen.
"""
        return prompt


    # ── NEU v4.0: MULTI-AGENT ORCHESTRATOR ────────────────────────────────────
    def run_multi_agent_analysis(self, bundle: dict, ai_client, model: str) -> str:
        """
        Orchestriert das Multi-Agenten System.
        Startet 3 spezialisierte Agenten parallel und übergibt die Ergebnisse an den Chief Analyst.
        """
        t = bundle.get("ticker", "UNKNOWN")
        
        # --- PROMPTS FÜR DIE AGENTEN ---
        fund_prompt = f"""Du bist der Fundamental-Agent. Bewerte die Aktie {t}.
Daten: KGV: {bundle.get('quote',{}).get('pe_ratio')}, Analysten-Rating: {bundle.get('analyst',{}).get('recommendation')}. 
SEC Insider Aktivität: {bundle.get('insider',{}).get('signal')} ({bundle.get('insider',{}).get('form4_count_recent')} Filings).
Gib eine kurze, kritische fundamentale Einschätzung (2-3 Sätze)."""

        tech_data = bundle.get('tech', {})
        tech_prompt = f"""Du bist der Technical-Agent. Bewerte die Charttechnik von {t}.
Aktueller Preis: {bundle.get('quote',{}).get('price')}.
Tech Score: {tech_data.get('score', 0)}/100. RSI: {tech_data.get('latest_data',{}).get('rsi')}.
Signale: {tech_data.get('buy_signals')} Buy, {tech_data.get('sell_signals')} Sell.
Bewerte kurz den Trend und Momentum (2-3 Sätze)."""

        sent_prompt = f"""Du bist der Sentiment-Agent. Bewerte die Marktstimmung zu {t}.
Reddit Sentiment Score: {bundle.get('reddit',{}).get('sentiment')} ({bundle.get('reddit',{}).get('sentiment_label')}).
Fear & Greed Index: {bundle.get('fear_greed',{}).get('value')} ({bundle.get('fear_greed',{}).get('label')}).
Google Trends Richtung: {bundle.get('trends',{}).get('trend_direction')}.
Fasse die aktuelle Hype/Fear Dynamik zusammen (2-3 Sätze)."""

        # --- PARALLELE AUSFÜHRUNG DER AGENTEN ---
        reports = {}
        with ThreadPoolExecutor(max_workers=3) as executor:
            future_fund = executor.submit(ai_client.ask, fund_prompt, model)
            future_tech = executor.submit(ai_client.ask, tech_prompt, model)
            future_sent = executor.submit(ai_client.ask, sent_prompt, model)
            
            try: reports["fund"] = future_fund.result(timeout=15)
            except Exception: reports["fund"] = "Fundamental-Analyse nicht verfügbar."
            try: reports["tech"] = future_tech.result(timeout=15)
            except Exception: reports["tech"] = "Technische Analyse nicht verfügbar."
            try: reports["sent"] = future_sent.result(timeout=15)
            except Exception: reports["sent"] = "Sentiment-Analyse nicht verfügbar."

        # --- CHIEF ANALYST ---
        meta_prompt = f"""Du bist der Chief Investment Officer (Meta-Agent). 
Du hast soeben die Berichte deiner 3 spezialisierten Analysten für {t} erhalten:

[FUNDAMENTAL REPORT]: {reports['fund']}
[TECHNICAL REPORT]: {reports['tech']}
[SENTIMENT REPORT]: {reports['sent']}

Deine Aufgabe:
Schreibe einen professionellen Markdown-Report für einen institutionellen Kunden.
Struktur:
1. **Executive Summary:** Bringe die 3 Perspektiven in einem Fazit zusammen. Sind sie einig oder widersprüchlich?
2. **Agenten-Übersicht:** Fasse kurz die Hauptpunkte der 3 Reports in Bulletpoints zusammen.
3. **Actionable Advice:** Klare KAUFEN, HALTEN oder VERKAUFEN Empfehlung mit strategischer Begründung.

Der Tonfall ist hochprofessionell, direkt und auf Deutsch."""

        try:
            final_report = ai_client.ask(meta_prompt, model)
            return final_report
        except Exception as e:
            return f"Fehler beim Chief Analyst: {e}"


# ── Singleton ────────────────────────────────────────────────────────────────
_instance = None
def get_omni_service() -> OmniDataService:
    global _instance
    if _instance is None: _instance = OmniDataService()
    return _instance