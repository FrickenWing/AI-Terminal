"""
utils/formatters.py - Formatierungs-Hilfsfunktionen

Einheitliche Formatierung von Zahlen, Preisen, Prozenten, etc.
"""

from datetime import datetime
from typing import Union


def fmt_large(value: Union[float, int], decimals: int = 2) -> str:
    """
    Formatiert große Zahlen (1.2M, 3.5B, etc.)
    
    Args:
        value: Zahl die formatiert werden soll
        decimals: Anzahl Dezimalstellen
    
    Returns:
        Formatierter String (z.B. "$1.2M")
    """
    if value is None or (isinstance(value, float) and value != value):  # NaN check
        return "N/A"
    
    try:
        value = float(value)
        if abs(value) >= 1e12:
            return f"${value/1e12:.{decimals}f}T"
        elif abs(value) >= 1e9:
            return f"${value/1e9:.{decimals}f}B"
        elif abs(value) >= 1e6:
            return f"${value/1e6:.{decimals}f}M"
        elif abs(value) >= 1e3:
            return f"${value/1e3:.{decimals}f}K"
        else:
            return f"${value:.{decimals}f}"
    except (ValueError, TypeError):
        return "N/A"


def fmt_price(value: Union[float, int], decimals: int = 2) -> str:
    """Formatiert Preis mit $ und Tausender-Trennzeichen"""
    if value is None or (isinstance(value, float) and value != value):
        return "N/A"
    try:
        return f"${float(value):,.{decimals}f}"
    except (ValueError, TypeError):
        return "N/A"


def fmt_pct(value: Union[float, int], decimals: int = 2, show_plus: bool = True) -> str:
    """
    Formatiert Prozent-Werte
    
    Args:
        value: Wert (z.B. 0.0532 für 5.32%)
        decimals: Dezimalstellen
        show_plus: Ob + vor positiven Werten angezeigt wird
    """
    if value is None or (isinstance(value, float) and value != value):
        return "N/A"
    
    try:
        value = float(value) * 100  # Umrechnung zu Prozent
        sign = "+" if value > 0 and show_plus else ""
        return f"{sign}{value:.{decimals}f}%"
    except (ValueError, TypeError):
        return "N/A"


def fmt_ratio(value: Union[float, int], decimals: int = 2) -> str:
    """Formatiert Ratios (P/E, P/B, etc.)"""
    if value is None or (isinstance(value, float) and value != value):
        return "N/A"
    try:
        return f"{float(value):.{decimals}f}x"
    except (ValueError, TypeError):
        return "N/A"


def fmt_volume(value: Union[float, int]) -> str:
    """Formatiert Handelsvolumen"""
    return fmt_large(value, decimals=1).replace("$", "")


def fmt_date(date: Union[datetime, str], format_str: str = "%d.%m.%Y") -> str:
    """Formatiert Datum"""
    if date is None:
        return "N/A"
    
    try:
        if isinstance(date, str):
            date = datetime.fromisoformat(date.replace("Z", "+00:00"))
        return date.strftime(format_str)
    except (ValueError, AttributeError):
        return str(date)


def color_pct(value: Union[float, int]) -> str:
    """
    Gibt Farbe für Prozent-Werte zurück
    
    Returns:
        Hex-Farbe (#26a69a für positiv, #ef5350 für negativ)
    """
    if value is None or (isinstance(value, float) and value != value):
        return "#8b95a1"  # Grau für N/A
    
    try:
        return "#26a69a" if float(value) >= 0 else "#ef5350"
    except (ValueError, TypeError):
        return "#8b95a1"


def trend_arrow(value: Union[float, int]) -> str:
    """Gibt Pfeil für Trend zurück (▲ oder ▼)"""
    if value is None or (isinstance(value, float) and value != value):
        return ""
    
    try:
        return "▲" if float(value) >= 0 else "▼"
    except (ValueError, TypeError):
        return ""


def format_large_number(value: Union[float, int]) -> str:
    """Alias für fmt_large (für Kompatibilität)"""
    return fmt_large(value)
