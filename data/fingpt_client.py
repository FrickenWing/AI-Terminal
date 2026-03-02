"""
fingpt_client.py
────────────────────────────────────────────────────────────────────────────
Finanz-KI Client via HuggingFace Inference API.

WARUM NICHT DIREKT "FinGPT"?
  Die offiziellen FinGPT-Modelle (AI4Finance-Foundation/FinGPT) sind
  LoRA-Adapter auf LLaMA2 – sie lassen sich NICHT direkt per REST-API
  aufrufen. Du bräuchtest ~16 GB VRAM lokal ODER einen GPU-Cloud-Service.

WAS WIR STATTDESSEN TUN:
  Wir nutzen die HuggingFace Serverless Inference API mit aktuellen
  Instruct-Modellen + einem FinGPT-inspirierten System-Prompt.
  Das Ergebnis ist für diesen Use-Case praktisch gleichwertig.

LOKALES FINGPT (optional, für später):
  Wenn du Ollama installiert hast, kannst du OLLAMA_URL in der .env
  setzen (z.B. http://localhost:11434) und ein LLaMA-Modell laden.
  Der Client wechselt dann automatisch auf die lokale Instanz.

.env Variablen:
  HF_API_KEY    = dein HuggingFace Access Token (https://huggingface.co/settings/tokens)
  OLLAMA_URL    = (optional) http://localhost:11434  →  lokales FinGPT via Ollama
  OLLAMA_MODEL  = (optional) z.B. llama3 oder mistral
"""

import os
import requests
from loguru import logger
from dotenv import load_dotenv

load_dotenv()

# ── Verfügbare HuggingFace-Modelle ──────────────────────────────────────────
# Alle über die kostenlose Serverless-Inference verfügbar (mit HF-Token)
AVAILABLE_MODELS = [
    {
        "id":          "mistralai/Mistral-7B-Instruct-v0.3",
        "label":       "Mistral 7B Instruct",
        "description": "Schnell & präzise – empfohlen für Finanzanalyse",
    },
    {
        "id":          "HuggingFaceH4/zephyr-7b-beta",
        "label":       "Zephyr 7B Beta",
        "description": "Gut für strukturierte Antworten",
    },
    {
        "id":          "microsoft/Phi-3-mini-4k-instruct",
        "label":       "Phi-3 Mini (Microsoft)",
        "description": "Sehr schnell, kompaktes Modell",
    },
    {
        "id":          "google/gemma-2-2b-it",
        "label":       "Gemma 2 2B (Google)",
        "description": "Leichtes Google-Modell",
    },
    {
        "id":          "meta-llama/Llama-3.2-3B-Instruct",
        "label":       "LLaMA 3.2 3B (Meta)",
        "description": "Meta-Modell – benötigt HF-Zugang",
    },
]

# FinGPT-inspirierter System-Prompt
SYSTEM_PROMPT = """Du bist FinGPT, ein spezialisierter KI-Finanzanalyst.
Du analysierst Aktien, Märkte und Finanzdaten auf Basis der dir gegebenen Kennzahlen.
Antworte immer auf Deutsch, prägnant (max. 3-4 Sätze) und mit konkreten Einschätzungen.
Verwende keine allgemeinen Floskeln. Keine Anlageberatung – nur Analyse.
Formatiere deine Antwort klar und verständlich."""


class FinGPTClient:
    def __init__(self):
        self.hf_key      = os.getenv("HF_API_KEY", "")
        self.ollama_url  = os.getenv("OLLAMA_URL", "").rstrip("/")
        self.ollama_model = os.getenv("OLLAMA_MODEL", "mistral")

        if not self.hf_key and not self.ollama_url:
            logger.warning(
                "[FinGPT] Kein HF_API_KEY und keine OLLAMA_URL in .env gefunden! "
                "KI-Antworten werden nicht funktionieren."
            )

    # ── Öffentliche Methode ──────────────────────────────────────────────────

    def ask(self, prompt: str, model_id: str = "mistralai/Mistral-7B-Instruct-v0.3") -> str:
        """
        Sendet einen Prompt an das Modell und gibt die Antwort zurück.
        Reihenfolge: Ollama (lokal) → HuggingFace Inference API
        """
        # 1. Lokales Ollama hat Vorrang (kein API-Key nötig, schneller)
        if self.ollama_url:
            try:
                return self._ask_ollama(prompt)
            except Exception as e:
                logger.warning(f"[Ollama] fehlgeschlagen, wechsle zu HF: {e}")

        # 2. HuggingFace Serverless Inference
        if self.hf_key:
            return self._ask_hf(prompt, model_id)

        raise ValueError(
            "Kein HF_API_KEY und kein OLLAMA_URL konfiguriert. "
            "Bitte in der .env eintragen."
        )

    # ── Private Methoden ─────────────────────────────────────────────────────

    def _ask_hf(self, prompt: str, model_id: str) -> str:
        """Ruft die HuggingFace Serverless Inference API auf."""
        # Fallback-Kette falls das gewünschte Modell nicht verfügbar
        fallbacks = [m["id"] for m in AVAILABLE_MODELS if m["id"] != model_id]
        models_to_try = [model_id] + fallbacks

        full_prompt = f"{SYSTEM_PROMPT}\n\n{prompt}"

        for model in models_to_try:
            try:
                url     = f"https://api-inference.huggingface.co/models/{model}"
                headers = {"Authorization": f"Bearer {self.hf_key}"}
                payload = {
                    "inputs": full_prompt,
                    "parameters": {
                        "max_new_tokens":  512,
                        "temperature":     0.4,
                        "do_sample":       True,
                        "return_full_text": False,
                    },
                }
                resp = requests.post(url, headers=headers, json=payload, timeout=30)

                if resp.status_code == 503:
                    logger.warning(f"[HF] {model} lädt noch (503), nächstes Modell…")
                    continue
                if resp.status_code == 200:
                    data = resp.json()
                    if isinstance(data, list) and data:
                        text = data[0].get("generated_text", "").strip()
                        # System-Prompt aus der Antwort entfernen falls zurückgegeben
                        if text.startswith(SYSTEM_PROMPT):
                            text = text[len(SYSTEM_PROMPT):].strip()
                        if text:
                            logger.info(f"[HF] Antwort via {model}")
                            return text
                else:
                    logger.warning(f"[HF] {model} → HTTP {resp.status_code}: {resp.text[:200]}")

            except Exception as e:
                logger.warning(f"[HF] {model} Exception: {e}")
                continue

        raise Exception(
            f"Alle HuggingFace-Modelle fehlgeschlagen. "
            f"Bitte HF_API_KEY prüfen (https://huggingface.co/settings/tokens)."
        )

    def _ask_ollama(self, prompt: str) -> str:
        """Ruft ein lokales Ollama-Modell auf (z.B. für echtes FinGPT via LLaMA)."""
        full_prompt = f"{SYSTEM_PROMPT}\n\n{prompt}"
        url  = f"{self.ollama_url}/api/generate"
        resp = requests.post(
            url,
            json={"model": self.ollama_model, "prompt": full_prompt, "stream": False},
            timeout=60,
        )
        resp.raise_for_status()
        return resp.json().get("response", "").strip()


# ── Singleton ────────────────────────────────────────────────────────────────
_client: FinGPTClient | None = None

def get_fingpt_client() -> FinGPTClient:
    global _client
    if _client is None:
        _client = FinGPTClient()
    return _client
