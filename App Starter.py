import subprocess
import sys
import os
import importlib.metadata
from loguru import logger

def check_and_install_dependencies():
    """
    Prüft die requirements.txt und installiert fehlende Bibliotheken automatisch.
    Verwendet importlib.metadata (Standard ab Python 3.8).
    """
    # Pfad zur requirements.txt finden (gleiches Verzeichnis wie dieses Skript)
    base_dir = os.path.dirname(os.path.abspath(__file__))
    req_file = os.path.join(base_dir, "requirements.txt")
    
    if not os.path.exists(req_file):
        logger.warning(f"requirements.txt nicht gefunden unter: {req_file}")
        return

    logger.info("Überprüfe System-Abhängigkeiten...")
    
    try:
        # Anforderungen einlesen (UTF-8 ist Standard für .txt unter GitHub)
        with open(req_file, "r", encoding="utf-8") as f:
            requirements = [
                line.strip() for line in f 
                if line.strip() and not line.startswith("#")
            ]

        missing = []
        for req in requirements:
            # Trenne Paketnamen von Versionen (z.B. 'flask>=2.0' -> 'flask')
            package_name = req.split('>=')[0].split('==')[0].split('>')[0].strip()
            
            try:
                importlib.metadata.version(package_name)
            except importlib.metadata.PackageNotFoundError:
                missing.append(req)

        if missing:
            logger.warning(f"Folgende Pakete fehlen: {', '.join(missing)}")
            logger.info("Starte automatische Installation via pip... Bitte warten.")
            
            # Installation ausführen
            subprocess.check_call([
                sys.executable, "-m", "pip", "install", "--upgrade", "pip"
            ])
            subprocess.check_call([
                sys.executable, "-m", "pip", "install", "-r", req_file
            ])
            logger.success("✅ Alle Abhängigkeiten erfolgreich installiert.")
        else:
            logger.success("✅ Alle Abhängigkeiten sind bereits vorhanden.")

    except Exception as e:
        logger.error(f"Fehler bei der Paketprüfung: {e}")
        logger.info("Versuche trotzdem, die Anwendung zu starten...")

def start_application():
    """
    Startet die Hauptanwendung (main.py).
    """
    main_script = os.path.join(os.path.dirname(os.path.abspath(__file__)), "main.py")
    
    if not os.path.exists(main_script):
        logger.error(f"Kritischer Fehler: {main_script} wurde nicht gefunden!")
        return

    logger.info("Lade Backend-Dienste...")
    try:
        # Startet main.py als Unterprozess
        subprocess.run([sys.executable, main_script], check=True)
    except KeyboardInterrupt:
        logger.info("Anwendung durch Benutzer beendet.")
    except Exception as e:
        logger.error(f"Anwendung beendet mit Fehler: {e}")

if __name__ == "__main__":
    print("\n" + "="*55)
    print("      🚀 AI-TERMINAL: GOD-MODE INFRASTRUCTURE 🚀")
    print("="*55 + "\n")

    # 1. Abhängigkeiten fixen
    check_and_install_dependencies()

    # 2. Main App starten
    start_application()