#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Simulatore wearable - Heart Rate Monitoring MVP (V2)
----------------------------------------------------
Questo script genera una serie di eventi JSON realistici inviandoli via HTTP POST 
all'endpoint di Apache NiFi (http://localhost:8080/heartrate).

Utilizza esclusivamente librerie standard di Python per non avere dipendenze.
"""

import json
import time
import random
import urllib.request
import urllib.error
from datetime import datetime

# Configurazione endpoint NiFi
NIFI_URL = "http://localhost:8080/heartrate"

# Lista di pazienti simulati e configurazioni
PATIENTS = {
    "p001": {"device": "wearable_fit_01", "base_rate": 72, "activity": "rest"},
    "p002": {"device": "wearable_fit_02", "base_rate": 130, "activity": "running"},
    "p003": {"device": "wearable_fit_03", "base_rate": 45, "activity": "sleeping"}
}

def send_post(payload):
    """Invia il payload JSON a NiFi via HTTP POST."""
    data = json.dumps(payload).encode('utf-8')
    req = urllib.request.Request(
        NIFI_URL,
        data=data,
        headers={'Content-Type': 'application/json'}
    )
    try:
        with urllib.request.urlopen(req) as response:
            return response.status, response.read().decode('utf-8')
    except urllib.error.URLError as e:
        return None, str(e)

def generate_record(patient_id, hr, activity):
    """Genera un dizionario di record dati strutturato secondo lo schema."""
    return {
        "patient_id": patient_id,
        "device_id": PATIENTS[patient_id]["device"],
        "timestamp": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
        "heart_rate": hr,
        "activity": activity
    }

def main():
    print("=========================================================================")
    print("      SIMULATORE DI BATTITO CARDIACO WEARABLE (NiFi Ingestion v2)       ")
    print("=========================================================================")
    print(f"Target Endpoint: {NIFI_URL}")
    print("Invio di 15 record di test in corso...")
    print("-------------------------------------------------------------------------")

    # Creazione della sequenza di eventi pre-configurati per la demo
    events_sequence = [
        # --- Blocco 1: Paziente 1 in condizioni normali (a riposo) ---
        ("p001", 72, "rest", "Normale (rest)"),
        ("p001", 75, "rest", "Normale (rest)"),
        ("p001", 70, "rest", "Normale (rest)"),

        # --- Blocco 2: Paziente 2 in attività fisica intensa (sforzo) ---
        ("p002", 125, "running", "High Alert atteso (sforzo/running)"),
        ("p002", 130, "running", "High Alert atteso (sforzo/running)"),
        ("p002", 135, "running", "High Alert atteso (sforzo/running)"),

        # --- Blocco 3: Paziente 3 durante il sonno (bradicardia controllata) ---
        ("p003", 45, "sleeping", "Low Alert atteso (bradicardia/sleeping)"),
        ("p003", 42, "sleeping", "Low Alert atteso (bradicardia/sleeping)"),
        ("p003", 48, "sleeping", "Low Alert atteso (bradicardia/sleeping)"),

        # --- Blocco 4: Paziente 1 - Anomalie acute (Tachicardia improvvisa a riposo) ---
        ("p001", 120, "rest", "CRITICAL ALERT atteso (Tachicardia a riposo!)"),
        ("p001", 128, "rest", "CRITICAL ALERT atteso (Tachicardia a riposo!)"),
        
        # --- Blocco 5: Record misti aggiuntivi per consolidare il batch ---
        ("p001", 78, "walking", "Normale (walking)"),
        ("p002", 115, "walking", "High Alert atteso (walking)"),
        ("p003", 55, "rest", "Normale (rest dopo sonno)"),
        ("p001", 71, "rest", "Normale (rientro a valori base)")
    ]

    sent_count = 0
    failed_count = 0

    for i, (p_id, hr, act, desc) in enumerate(events_sequence, 1):
        payload = generate_record(p_id, hr, act)
        print(f"[{i}/15] Inviando -> Paziente: {p_id} | HR: {hr:3d} bpm | Attività: {act:8s} | Desc: {desc}")
        
        # Invio HTTP POST
        status, response = send_post(payload)
        
        if status == 200 or status == 201:
            sent_count += 1
        else:
            failed_count += 1
            print(f"   [!] ERRORE DI INVIO: {response} (Assicurarsi che NiFi sia attivo sulla porta 8080)")

        # Piccolo delay per simulare l'arrivo streaming dei dati (0.4 secondi)
        time.sleep(0.4)

    print("-------------------------------------------------------------------------")
    print("Simulazione completata.")
    print(f"Record inviati con successo: {sent_count}/15")
    print(f"Record falliti: {failed_count}/15")
    print("-------------------------------------------------------------------------")
    print("[INFO DEMO] Se i file sono stati inviati con successo:")
    print("  1. Controlla le code in NiFi: rimarranno in buffer per al massimo 10s.")
    print("  2. Controlla HDFS: vedrai comparire un file batch NDJSON aggregato.")
    print("  3. Controlla la console di Spark Structured Streaming per l'update analitico.")
    print("=========================================================================")

if __name__ == "__main__":
    main()
