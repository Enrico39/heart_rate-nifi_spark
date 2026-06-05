# Guida Passo-Passo Definitiva: Presentazione Demo Heart Rate Monitoring

Questa guida è stata strutturata per essere seguita in modo lineare durante l'esame. Descrive esattamente **cosa fare**, **quali comandi lanciare**, **dove scriverli** e **cosa mostrare** al professore per convalidare il funzionamento del cluster Dataproc a 3 nodi (1 Master, 2 Worker).

---

## 📋 PREPARAZIONE (Prima dell'inizio dell'esame)

### 1. Accensione del Cluster Dataproc
Accedi alla Google Cloud Console tramite browser e verifica che tutte e 3 le istanze VM siano nello stato **RUNNING** (Verde). Se i nodi worker risultano fermi (TERMINATED), puoi riavviarli rapidamente lanciando questo comando sulla sessione SSH del Master:
```bash
gcloud compute instances start cluster-bfb8-w-0 cluster-bfb8-w-1 --zone europe-west1-c
```

### 2. Avvio di Apache NiFi sul Master VM
Se Apache NiFi non è già in esecuzione sul nodo Master (dopo un riavvio della VM), connettiti in SSH al Master VM ed avvialo:
```bash
~/nifi/bin/nifi.sh start
```
Per verificare lo stato del servizio:
```bash
~/nifi/bin/nifi.sh status
```
*(Nota: Il caricamento iniziale dell'interfaccia web di NiFi può richiedere da 1 a 2 minuti).*

### 3. Apertura del Tunnel SSH dal tuo Mac Locale
Apri il terminale del tuo **Mac locale** ed avvia il tunnel di inoltro porte sicuro (lascialo aperto per tutto l'esame):
```bash
ssh -i ~/.ssh/gcp_key -N -L 8090:localhost:8090 -L 8080:localhost:8080 -L 9870:localhost:9870 enricomadonna0@<IP_MASTER>
```
> [!NOTE]
> Questo tunnel ti permette di accedere dal tuo Mac a:
> * **NiFi Web UI**: `http://localhost:8090/nifi`
> * **NiFi HTTP Ingest**: `http://localhost:8080/heartrate`
> * **HDFS Web Browser**: `http://localhost:9870`

---

## 🚀 FASE 1: Configurazione ed Avvio del Flusso su Apache NiFi

**Dove**: Browser web sul tuo Mac.

1. Apri il browser all'indirizzo **`http://localhost:8090/nifi`**.
2. Verifica che sul canvas sia presente il flusso `HeartRate_MVP_v2` con gli 8 processori collegati.
3. Assicurati che **tutti i processori siano in esecuzione** (icona cerchio verde con simbolo "Play"). 
   * *Se fossero fermi (quadrato rosso)*: fai `Cmd+A` per selezionarli tutti, clicca con il tasto destro su un punto vuoto del canvas e premi **Start**.

---

## 🧠 FASE 2: Avvio del Job Spark Streaming sul Master VM

**Dove**: Terminale SSH del nodo **Master VM** (`cluster-bfb8-m`).

1. Collegati in SSH al nodo Master (tramite GCP console o terminale).
2. Spostati nella cartella home (se non ci sei già):
   ```bash
   cd ~
   ```
3. Lancia il job Spark Structured Streaming inviandolo a YARN in modalità client:
   ```bash
   spark-submit \
     --master yarn \
     --deploy-mode client \
     --num-executors 1 \
     --executor-cores 1 \
     --executor-memory 1G \
     --driver-memory 1G \
     heart_rate_streaming_v2.py
   ```
4. Attendi che Spark finisca l'inizializzazione e si posizioni in ascolto. Vedrai a schermo il log di avvio e una tabella vuota (in attesa di file da HDFS):
   ```text
   =========================================================================
   [SPARK V2] Inizializzazione completata. Avvio Job di Streaming Analitico.
   =========================================================================
   [SPARK V2] Lettura stream in corso dalla cartella HDFS: hdfs:///user/enricomadonna0/nifi-demo/output
   ```

---

## 📈 FASE 3: Simulazione Invio Wearable in Streaming

**Dove**: Terminale del tuo **Mac Locale** (apri una nuova scheda, posizionandoti nella cartella del progetto).

1. Esegui il simulatore Python che invierà 15 eventi JSON simulando dati clinici di tre pazienti diversi:
   ```bash
   python3 simulate_data.py
   ```
2. Vedrai lo script stampare a schermo l'invio riuscito dei singoli messaggi verso `localhost:8080/heartrate`.

---

## 🎓 FASE 4: Cosa mostrare al professore per l'esame

Mostra questi tre elementi chiave in sequenza per validare l'intero flusso dati:

### 1. Il transito dei dati in NiFi (Real-time Routing)
Mostra la UI di NiFi:
* Fai vedere che i contatori di input/output dei processori salgono a `15`.
* Fai notare che i file si accumulano nella coda prima di `MergeContent_Batcher`. 
* Spiega al professore: *"NiFi attende un massimo di 10 secondi prima di raggruppare i record JSON in un singolo file NDJSON da scrivere su HDFS. Questo risolve il **small files problem** tipico di Hadoop, evitando di intasare il NameNode con migliaia di piccoli file da 100 byte."*

### 2. La tabella analitica in tempo reale su Spark
Spostati sulla finestra del terminale del **Master VM** dove gira il job Spark:
* Entro pochissimi secondi dalla scrittura su HDFS, Spark rileverà automaticamente il file.
* Mostrerà a schermo la tabella aggregata in tempo reale con i risultati corretti divisi per paziente:
  ```text
  -------------------------------------------
  Batch: 1
  -------------------------------------------
  +----------+------------------+--------------+--------------+--------------+----------+-----------+---------------+
  |patient_id|average_heart_rate|min_heart_rate|max_heart_rate|total_readings|low_alerts|high_alerts|critical_alerts|
  +----------+------------------+--------------+--------------+--------------+----------+-----------+---------------+
  |      p003|              47.5|            42|            55|             4|         3|          0|              0|
  |      p002|            126.25|           115|           135|             4|         0|          4|              0|
  |      p001| 87.71428571428571|            70|           128|             7|         0|          0|              2|
  +----------+------------------+--------------+--------------+--------------+----------+-----------+---------------+
  ```
* Spiega al professore: *"Spark Structured Streaming analizza continuamente lo storico delle letture archiviate su HDFS per calcolare medie cumulative, range di battiti (min/max) e tracciare la quantità di allarmi generati da NiFi per ogni paziente."*

### 3. L'interfaccia HDFS (Grafica o da Riga di Comando)
Per convalidare la scrittura distribuita su HDFS, puoi scegliere una di queste opzioni:

* **Opzione A (Da terminale Master VM)**:
  Apri un altro terminale sul Master e lancia il comando per elencare i file scritti:
  ```bash
  hdfs dfs -ls /user/enricomadonna0/nifi-demo/output
  ```
  E per leggerne uno:
  ```bash
  hdfs dfs -cat /user/enricomadonna0/nifi-demo/output/*.json
  ```
* **Opzione B (Via Browser dal tuo Mac)**:
  Apri il browser su **`http://localhost:9870`**, clicca su **Utilities** -> **Browse the file system** e naviga fino al percorso `/user/enricomadonna0/nifi-demo/output`. Mostra al professore che i file sono fisicamente distribuiti sui nodi worker.
