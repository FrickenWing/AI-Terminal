"""
data/cache_manager.py - Persistentes Disk-Caching

Speichert API-Antworten auf der Festplatte.
Überlebt Streamlit-Neustarts → deutlich schneller nach erstem Aufruf.

Verwendung:
    from data.cache_manager import get_cache, cached
    cache = get_cache()
    cache.set("aapl_quote", data, ttl=60)
    data = cache.get("aapl_quote")

    # Oder als Decorator:
    @cached(ttl=300)
    def my_expensive_function(ticker):
        ...
"""

import json
import time
import hashlib
import os
from pathlib import Path
from typing import Any, Optional, Callable
from functools import wraps
from loguru import logger

try:
    import diskcache as dc
    DISKCACHE_AVAILABLE = True
except ImportError:
    DISKCACHE_AVAILABLE = False
    logger.warning("diskcache nicht verfügbar – nutze In-Memory-Cache als Fallback")


# ─────────────────────────────────────────────
# IN-MEMORY FALLBACK (wenn diskcache fehlt)
# ─────────────────────────────────────────────

class InMemoryCache:
    """Einfacher In-Memory-Cache als Fallback für diskcache."""

    def __init__(self):
        self._store: dict = {}
        self._expiry: dict = {}

    def get(self, key: str) -> Optional[Any]:
        if key not in self._store:
            return None
        if self._expiry.get(key, float("inf")) < time.time():
            del self._store[key]
            del self._expiry[key]
            return None
        return self._store[key]

    def set(self, key: str, value: Any, ttl: int = 300) -> bool:
        try:
            self._store[key] = value
            self._expiry[key] = time.time() + ttl
            return True
        except Exception:
            return False

    def delete(self, key: str) -> bool:
        self._store.pop(key, None)
        self._expiry.pop(key, None)
        return True

    def clear(self) -> int:
        count = len(self._store)
        self._store.clear()
        self._expiry.clear()
        return count

    def stats(self) -> dict:
        return {
            "type":    "in_memory",
            "entries": len(self._store),
            "size_mb": 0,
        }


# ─────────────────────────────────────────────
# DISK CACHE WRAPPER
# ─────────────────────────────────────────────

class CacheManager:
    """
    Einheitliche Cache-Schnittstelle.
    Nutzt diskcache wenn verfügbar, sonst InMemoryCache.

    Alle Keys werden automatisch normalisiert (lowercase, kein Sonderzeichen).
    """

    def __init__(self, cache_dir: Optional[str] = None):
        if cache_dir is None:
            root = Path(__file__).parent.parent
            cache_dir = str(root / ".cache")

        Path(cache_dir).mkdir(parents=True, exist_ok=True)
        self.cache_dir = cache_dir

        if DISKCACHE_AVAILABLE:
            self._cache = dc.Cache(cache_dir, size_limit=500 * 1024 * 1024)  # 500 MB
            logger.info(f"DiskCache initialisiert: {cache_dir}")
        else:
            self._cache = InMemoryCache()
            logger.info("InMemoryCache initialisiert (diskcache nicht verfügbar)")

    def _normalize_key(self, key: str) -> str:
        """Schlüssel bereinigen: lowercase, nur alphanumerisch + Unterstrich."""
        clean = key.lower().replace(" ", "_")
        return "".join(c for c in clean if c.isalnum() or c in "_:-.")

    def get(self, key: str) -> Optional[Any]:
        """Wert aus Cache holen. None wenn nicht vorhanden oder abgelaufen."""
        try:
            normalized = self._normalize_key(key)
            if DISKCACHE_AVAILABLE:
                return self._cache.get(normalized)
            else:
                return self._cache.get(normalized)
        except Exception as e:
            logger.debug(f"Cache get Fehler für '{key}': {e}")
            return None

    def set(self, key: str, value: Any, ttl: int = 300) -> bool:
        """Wert in Cache speichern."""
        try:
            normalized = self._normalize_key(key)
            if DISKCACHE_AVAILABLE:
                self._cache.set(normalized, value, expire=ttl)
            else:
                self._cache.set(normalized, value, ttl=ttl)
            return True
        except Exception as e:
            logger.debug(f"Cache set Fehler für '{key}': {e}")
            return False

    def delete(self, key: str) -> bool:
        """Einzelnen Eintrag löschen."""
        try:
            normalized = self._normalize_key(key)
            if DISKCACHE_AVAILABLE:
                self._cache.delete(normalized)
            else:
                self._cache.delete(normalized)
            return True
        except Exception:
            return False

    def clear(self) -> int:
        """Kompletten Cache leeren. Gibt Anzahl gelöschter Einträge zurück."""
        try:
            if DISKCACHE_AVAILABLE:
                count = len(self._cache)
                self._cache.clear()
                return count
            else:
                return self._cache.clear()
        except Exception:
            return 0

    def clear_prefix(self, prefix: str) -> int:
        """Alle Einträge mit bestimmtem Prefix löschen."""
        if not DISKCACHE_AVAILABLE:
            return 0
        deleted = 0
        try:
            norm_prefix = self._normalize_key(prefix)
            for key in list(self._cache):
                if str(key).startswith(norm_prefix):
                    self._cache.delete(key)
                    deleted += 1
        except Exception:
            pass
        return deleted

    def stats(self) -> dict:
        """Cache-Statistiken für UI-Anzeige."""
        try:
            if DISKCACHE_AVAILABLE:
                size_bytes = self._cache.volume()
                return {
                    "type":    "disk",
                    "entries": len(self._cache),
                    "size_mb": round(size_bytes / 1024 / 1024, 1),
                    "dir":     self.cache_dir,
                }
            else:
                return self._cache.stats()
        except Exception:
            return {"type": "unknown", "entries": 0, "size_mb": 0}

    def make_key(self, *args, **kwargs) -> str:
        """Erzeugt einen stabilen Cache-Key aus beliebigen Argumenten."""
        raw = str(args) + str(sorted(kwargs.items()))
        return hashlib.md5(raw.encode()).hexdigest()[:16]


# ─────────────────────────────────────────────
# DECORATOR
# ─────────────────────────────────────────────

def cached(ttl: int = 300, prefix: str = ""):
    """
    Decorator für gecachte Funktionen.

    Verwendung:
        @cached(ttl=300, prefix="quote")
        def get_quote(ticker: str) -> dict:
            ...
    """
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs):
            cache = get_cache()
            raw_key = f"{prefix}:{func.__name__}:{args}:{sorted(kwargs.items())}"
            key = cache.make_key(raw_key)
            result = cache.get(key)
            if result is not None:
                return result
            result = func(*args, **kwargs)
            if result is not None:
                cache.set(key, result, ttl=ttl)
            return result
        return wrapper
    return decorator


# ─────────────────────────────────────────────
# SINGLETON
# ─────────────────────────────────────────────

_cache_instance: Optional[CacheManager] = None

def get_cache() -> CacheManager:
    """Gibt globale CacheManager-Instanz zurück."""
    global _cache_instance
    if _cache_instance is None:
        _cache_instance = CacheManager()
    return _cache_instance


# TTL-Konstanten (in Sekunden)
TTL = {
    "quote":        60,     # 1 Min  – Kursdaten
    "price_history": 300,   # 5 Min  – historische Kurse
    "fundamentals": 3600,   # 1 Std  – Fundamentaldaten
    "news":         900,    # 15 Min – News
    "screener":     600,    # 10 Min – Screener-Ergebnisse
    "macro":        3600,   # 1 Std  – Makrodaten
    "company_info": 86400,  # 1 Tag  – Unternehmensinfos
}
