#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Simulatore wearable ad Alto Carico - Heart Rate Monitoring MVP (V2)
------------------------------------------------------------------
Questo script genera e invia flussi continui di eventi JSON simulando 50 pazienti
in tempo reale. Ottimizzato per mostrare un carico di lavoro realistico in console.

Esecuzione:
  python3 simulate_high_load.py
"""

import json
import time
import random
import urllib.request
import urllib.error
from datetime import datetime

# Configurazione endpoint NiFi
NIFI_URL = "http://localhost:8080/heartrate"

# Configurazione simulazione
NUM_PATIENTS = 50
ACTIVITIES = ["rest", "walking", "running", "sleeping"]

# Inizializziamo i profili dei pazienti per generare dati coerenti
PATIENT_PROFILES = {}
for i in range(1, NUM_PATIENTS + 1):
    p_id = f"p{i:03d}"
    # Assegniamo a caso un profilo comportamentale al paziente
    profile_type = random.choice(["normal", "active", "bradicardiac", "anomalous"])
    if profile_type == "normal":
        base_rate = random.randint(65, 80)
        activity = "rest"
    elif profile_type == "active":
        base_rate = random.randint(110, 135)
        activity = "running"
    elif profile_type == "bradicardiac":
        base_rate = random.randint(40, 48)
        activity = "sleeping"
    else: # anomalous (tachicardico a riposo)
        base_rate = random.randint(110, 125)
        activity = "rest"
        
    PATIENT_PROFILES[p_id] = {
        "device_id": f"wearable_fit_{i:02d}",
        "base_rate": base_rate,
        "activity": activity,
        "profile": profile_type
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

def generate_record(patient_id):
    """Genera un record dati realistico basato sul profilo del paziente."""
    profile = PATIENT_PROFILES[patient_id]
    
    # Fluttuazione casuale del battito (-3 a +3 bpm)
    hr = profile["base_rate"] + random.randint(-3, 3)
    
    # Occasionalmente cambiamo attività per rendere i dati dinamici
    current_activity = profile["activity"]
    if random.random() < 0.05: # 5% di probabilità di cambiare attività a ogni lettura
        current_activity = random.choice(ACTIVITIES)
        # Adeguamento del battito all'attività
        if current_activity == "running":
            hr = random.randint(115, 145)
        elif current_activity == "sleeping":
            hr = random.randint(42, 55)
        else:
            hr = random.randint(60, 95)
            
    return {
        "patient_id": patient_id,
        "device_id": profile["device_id"],
        "timestamp": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
        "heart_rate": hr,
        "activity": current_activity
    }

def main():
    print("=========================================================================")
    print("      SIMULATORE AD ALTO CARICO: 50 PAZIENTI IN STREAMING CONTINUO       ")
    print("=========================================================================")
    print(f"Target Endpoint: {NIFI_URL}")
    print("Inizio invio dati (Premi Ctrl+C per interrompere)...")
    print("-------------------------------------------------------------------------")

    sent_count = 0
    failed_count = 0

    try:
        while True:
            # Scegliamo un paziente a caso a ogni ciclo per simulare arrivi asincroni
            p_id = f"p{random.randint(1, NUM_PATIENTS):03d}"
            payload = generate_record(p_id)
            
            # Invio HTTP POST
            status, response = send_post(payload)
            
            if status in [200, 201]:
                sent_count += 1
                if sent_count % 10 == 0:
                    print(f"[INFO] Inviati con successo {sent_count} eventi medici totali...")
            else:
                failed_count += 1
                print(f"   [!] ERRORE DI INVIO: {response}")
                time.sleep(2) # Se fallisce, aspetta un momento per non intasare la console di errori

            # Delay molto basso per simulare un carico di circa 10-15 eventi al secondo
            time.sleep(0.08)

    except KeyboardInterrupt:
        print("\n-------------------------------------------------------------------------")
        print("Simulazione interrotta dall'utente.")
        print(f"Record inviati con successo: {sent_count}")
        print(f"Record falliti: {failed_count}")
        print("=========================================================================")

if __name__ == "__main__":
    main()
