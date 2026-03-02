import subprocess
import sys
import os

def check_and_install_requirements():
    """
    Prüft, ob die requirements.txt existiert und installiert 
    alle darin enthaltenen Pakete über pip.
    """
    req_file = "requirements.txt"
    
    if not os.path.exists(req_file):
        print(f"[WARNUNG] Keine {req_file} gefunden. Überspringe Installation.")
        return

    print("[INFO] Prüfe und installiere fehlende Bibliotheken aus requirements.txt...")
    try:
        # Führt 'pip install -r requirements.txt -q' im Hintergrund aus
        subprocess.check_call([sys.executable, "-m", "pip", "install", "-r", req_file, "-q"])
        print("[INFO] Alle Bibliotheken sind einsatzbereit!")
    except subprocess.CalledProcessError as e:
        print(f"[FEHLER] Die Installation der Bibliotheken ist fehlgeschlagen: {e}")
        sys.exit(1)

if __name__ == "__main__":
    # 1. Pakete installieren
    check_and_install_requirements()
    
    # 2. Server starten (erst nachdem alles installiert ist)
    print("\n[INFO] Starte den AI-Analyst Server...")
    
    # Wir importieren uvicorn und unsere App erst hier, 
    # damit es nicht kracht, falls uvicorn gerade erst installiert wurde.
    import uvicorn
    from main import app
    
    # Host 0.0.0.0 erlaubt auch den Zugriff aus dem lokalen WLAN
    uvicorn.run(app, host="0.0.0.0", port=8000)