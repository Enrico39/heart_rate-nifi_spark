# Apache NiFi v2 Flow: Heart Rate Monitoring MVP

Questo documento definisce il design dettagliato e le specifiche operative del flusso **Apache NiFi v2** per l'ingestione, la classificazione, l'arricchimento e il buffering dei dati cardiaci.

Il flow è progettato specificamente per evitare il *small files problem* su HDFS e per garantire la massima stabilità su un cluster Google Cloud Dataproc a 3 nodi.

---

## 1. Executive Summary

La soluzione v2 sostituisce i processori di manipolazione di stringhe (`ReplaceText`) con processori record-oriented e di trasformazione strutturale (`JoltTransformJSON`).

* **Ingestione**: REST HTTP sincrono.
* **Classificazione**: Gestita tramite le regole avanzate (`Advanced Rules`) del processore `UpdateAttribute`, evitando espressioni annidate complesse.
* **Arricchimento**: Eseguito tramite `JoltTransformJSON` che inserisce nativamente il campo `alert_type` nel JSON originale leggendolo dai metadati della FlowFile.
* **Buffering (NDJSON)**: Gestito tramite `MergeContent` con un demarcatore `\n` (newline), raggruppando i messaggi in micro-batch basati su tempo (max 10 secondi) o numero di record per evitare il sovraccarico di HDFS.
* **Scrittura**: Scrittura atomica (`Write and Rename`) su HDFS in un'unica cartella radice per semplificare la successiva lettura in streaming da parte di Spark.

---

## 2. Flow Design

Il flusso dei dati si sviluppa linearmente, con una diramazione temporanea per il logging degli alert in console e il ricongiungimento immediato prima della fase di scrittura:

```
                  +--------------------------+
                  |  ListenHTTP (Port 8080)  |
                  +--------------------------+
                               |
                               v
                  +--------------------------+
                  |    EvaluateJsonPath      |
                  +--------------------------+
                               | matched
                               v
                  +--------------------------+
                  | UpdateAttribute (Rules)  |
                  +--------------------------+
                               |
                               v
                  +--------------------------+
                  |    RouteOnAttribute      |
                  +--------------------------+
                   /                        \
                  / alert                    \ normal
                 v                            v
      +--------------------+                  |
      |     LogMessage     |                  |
      |   (Console Demo)   |                  |
      +--------------------+                  |
                 \                            /
                  \                          /
                   v                        v
                  +--------------------------+
                  |    JoltTransformJSON     |
                  +--------------------------+
                               | success
                               v
                  +--------------------------+
                  |       MergeContent       |
                  +--------------------------+
                               | merged
                               v
                  +--------------------------+
                  |         PutHDFS          |
                  +--------------------------+
```

---

## 3. Processor-by-Processor Configuration

### 3.1 ListenHTTP
* **Nome**: `ListenHTTP_Ingestion`
* **Classe**: `org.apache.nifi.processors.standard.ListenHTTP`
* **Proprietà**:
  * `Listening Port`: `8080`
  * `Base Path`: `heartrate`
* **Relazioni**:
  * `success` $\rightarrow$ Collegato a `EvaluateJsonPath_Extractor`.
* **Motivazione**: Fornisce un endpoint HTTP REST leggero e nativo per ricevere i JSON dai dispositivi remoti.

### 3.2 EvaluateJsonPath
* **Nome**: `EvaluateJsonPath_Extractor`
* **Classe**: `org.apache.nifi.processors.standard.EvaluateJsonPath`
* **Proprietà**:
  * `Destination`: `flowfile-attribute`
  * `Return Type`: `json`
  * `heart_rate`: `$.heart_rate`
  * `activity`: `$.activity`
* **Relazioni**:
  * `matched` $\rightarrow$ `UpdateAttribute_Classifier`.
  * `failure` / `unmatched` $\rightarrow$ `Terminate` (Scarta i record non validi per la demo).
* **Motivazione**: Estrae solo i due campi necessari per la logica di business (`heart_rate` e `activity`) inserendoli come attributi della FlowFile. Non tocca il contenuto del file, minimizzando il consumo di CPU.

### 3.3 UpdateAttribute
* **Nome**: `UpdateAttribute_Classifier`
* **Classe**: `org.apache.nifi.processors.attributes.UpdateAttribute`
* **Proprietà**:
  * *Uso della scheda "Advanced Rules"* (Vedi Sezione 4 per il dettaglio logico).
* **Relazioni**:
  * `success` $\rightarrow$ `RouteOnAttribute_DemoLogger`.
* **Motivazione**: Esegue la classificazione clinica impostando l'attributo `alert_status` in base a regole condizionali pulite, evitando script custom.

### 3.4 RouteOnAttribute
* **Nome**: `RouteOnAttribute_DemoLogger`
* **Classe**: `org.apache.nifi.processors.standard.RouteOnAttribute`
* **Proprietà**:
  * `Routing Strategy`: `Route to Property name`
  * `alert`: `${alert_status:equals('normal'):not()}`
  * `normal`: `${alert_status:equals('normal')}`
* **Relazioni**:
  * `alert` $\rightarrow$ `LogMessage_AlertWriter`.
  * `normal` $\rightarrow$ `JoltTransformJSON_Enricher`.
* **Motivazione**: Isola i record anomali al solo scopo di inviarli al log di demo, mantenendo il flusso pulito ed evitando il log di dati ordinari.

### 3.5 LogMessage
* **Nome**: `LogMessage_AlertWriter`
* **Classe**: `org.apache.nifi.processors.standard.LogMessage`
* **Proprietà**:
  * `Log Level`: `WARN`
  * `Log Message`: `[DEMO ALERT] Paziente: ${patient_id} | Battito: ${heart_rate} | Stato: ${alert_status} | Attività: ${activity}`
* **Relazioni**:
  * `success` $\rightarrow$ `JoltTransformJSON_Enricher`.
* **Motivazione**: Stampa nel log centrale di NiFi (`nifi-app.log`) gli allarmi critici per visualizzarli durante la presentazione.

### 3.6 JoltTransformJSON
* **Nome**: `JoltTransformJSON_Enricher`
* **Classe**: `org.apache.nifi.processors.standard.JoltTransformJSON`
* **Proprietà**:
  * `Jolt Transform DSL`: `Chain`
  * `Jolt Specification`:
    ```json
    [
      {
        "operation": "default",
        "spec": {
          "alert_type": "${alert_status}"
        }
      }
    ]
    ```
* **Relazioni**:
  * `success` $\rightarrow$ `MergeContent_Batcher`.
  * `failure` $\rightarrow$ `Terminate`.
* **Motivazione**: **Sostituisce ReplaceText**. Inietta in modo sicuro e sintatticamente corretto la proprietà `alert_type` nel JSON originario, preservando la formattazione.

### 3.7 MergeContent
* **Nome**: `MergeContent_Batcher`
* **Classe**: `org.apache.nifi.processors.standard.MergeContent`
* **Proprietà**:
  * `Merge Strategy`: `Bin-Packing Algorithm`
  * `Merge Format`: `Binary Concatenation`
  * `Demarcator`: `\n` (Literal newline o `${literal('\n')}`)
  * `Minimum Number of Entries`: `20`
  * `Max Bin Age`: `10 seconds` (Scadenza temporale per la demo)
* **Relazioni**:
  * `merged` $\rightarrow$ `PutHDFS_Writer`.
  * `original` / `failure` $\rightarrow$ `Terminate`.
* **Motivazione**: Esegue il batching sul flusso di testo JSON, concatenandoli riga per riga. L'output è un file NDJSON.

### 3.8 PutHDFS
* **Nome**: `PutHDFS_Writer`
* **Classe**: `org.apache.nifi.processors.hadoop.PutHDFS`
* **Proprietà**:
  * `Hadoop Configuration Resources`: `/etc/hadoop/conf/core-site.xml,/etc/hadoop/conf/hdfs-site.xml`
  * `Directory`: `/user/enricomadonna0/nifi-demo/output`
  * `Conflict Resolution Strategy`: `fail`
* **Relazioni**:
  * `success` / `failure` $\rightarrow$ `Terminate`.
* **Motivazione**: Scrive i file aggregati su HDFS. L'uso di file NDJSON garantisce che Spark Structured Streaming possa leggerli in streaming senza collisioni.

---

## 4. Alert Classification Logic

La classificazione clinica avviene dentro la scheda **Advanced** di `UpdateAttribute_Classifier`. Ciascuna regola ha una condizione associata ed esegue l'azione se la condizione è vera:

### Regola 1: `LowAlert`
* *Condizione*: `${heart_rate:toNumber():lt(50)}`
* *Azione*: Aggiungi attributo `alert_status` $\rightarrow$ `low_alert`

### Regola 2: `CriticalAlert`
* *Condizione*: `${heart_rate:toNumber():ge(110):and(${activity:equals('rest')})}`
* *Azione*: Aggiungi attributo `alert_status` $\rightarrow$ `critical_alert`

### Regola 3: `HighAlert`
* *Condizione*: `${heart_rate:toNumber():ge(110):and(${activity:equals('rest'):not()})}`
* *Azione*: Aggiungi attributo `alert_status` $\rightarrow$ `high_alert`

### Regola 4: `Normal`
* *Condizione*: `${heart_rate:toNumber():ge(50):and(${heart_rate:toNumber():lt(110)})}`
* *Azione*: Aggiungi attributo `alert_status` $\rightarrow$ `normal`

---

## 5. Payload Enrichment Strategy

Al posto di manipolare il JSON come una stringa di testo generica con espressioni regolari (tramite `ReplaceText`), che rischia di corrompere i caratteri speciali o fallire in presenza di spaziature differenti, NiFi v2 usa **`JoltTransformJSON`**. 

La specifica Jolt `default` definisce un valore predefinito che viene applicato alla struttura JSON radice. 
Utilizzando Expression Language all'interno della specifica Jolt (`"${alert_status}"`), NiFi preleva il valore dell'attributo FlowFile calcolato al passaggio precedente e lo inserisce come campo JSON `alert_type`:

```json
{
  "patient_id": "p001",
  "device_id": "d01",
  "timestamp": "2026-05-29T10:00:00Z",
  "heart_rate": 72,
  "activity": "rest",
  "alert_type": "normal"  <-- Inserito in modo nativo e pulito
}
```

---

## 6. Batching Strategy (Mitigazione Small Files)

La pipeline implementa un meccanismo di **Micro-Batching** per proteggere il NameNode di HDFS.

### 6.1 MergeContent vs MergeRecord
* **Scelta**: Si preferisce **`MergeContent`** a `MergeRecord`.
* **Motivazione**: `MergeRecord` richiede la configurazione di controller service complessi (`JsonTreeReader` e `JsonRecordSetWriter`) e consuma CPU significativa per serializzare/deserializzare ciascun record JSON. In un cluster Dataproc a 3 nodi con risorse scarse, `MergeContent` esegue una semplice concatenazione binaria (`Binary Concatenation`) di stringhe separate da newline (`\n`), che è un'operazione quasi istantanea e a costo computazionale zero.

### 6.2 Parametri di Flushing
* **Soglia Record**: `20`. Se arrivano 20 record in meno di 10 secondi, viene generato immediatamente il file HDFS.
* **Soglia Temporale (`Max Bin Age`)**: `10 seconds`. Se il traffico è basso (come tipico durante la demo live), NiFi non aspetterà indefinitamente il raggiungimento dei 20 record, ma scriverà il file HDFS allo scadere dei 10 secondi. Questo garantisce stabilità e reattività visiva durante la demo.

### 6.3 Formato NDJSON finale
Poiché il demarcatore configurato è `\n`, i singoli JSON validi vengono scritti su righe consecutive. Il file risultante è in formato **Newline-Delimited JSON (NDJSON)**, lo standard di riferimento per Spark:
```text
{"patient_id":"p001","heart_rate":72,"activity":"rest","alert_type":"normal"}
{"patient_id":"p002","heart_rate":45,"activity":"sleeping","alert_type":"low_alert"}
```

---

## 7. HDFS Layout

Il layout HDFS è estremamente semplificato per supportare la lettura in streaming senza partizionamento dinamico a directory, che causerebbe ritardi e complessità in Spark.

* **Directory Radice**: `/user/enricomadonna0/nifi-demo/output`
* **Struttura**:
  ```text
  /user/enricomadonna0/nifi-demo/output/
  ├── batch_node-1_1716960000000.json
  ├── batch_node-1_1716960010000.json
  └── batch_node-1_1716960020000.json
  ```
* **Nomenclatura**: `batch_${hostname}_${now():toNumber()}.json`. L'inclusione dell'hostname del nodo NiFi e del timestamp in millisecondi evita qualsiasi collisione di scrittura, garantendo l'assoluta unicità del file.

---

## 8. Demo Readiness

La demo universitaria si basa sulla reattività visiva:
1. **Verifica visiva delle code NiFi**: I record rimangono in coda prima del processore `MergeContent` fino allo scadere dei 10 secondi.
2. **Aggiornamento dei Log NiFi**: Il processore `LogMessage` stampa in console solo le righe degli allarmi (es. `critical_alert`).
3. **Ispezione HDFS**: Tramite CLI, si mostra che è presente un solo file aggregato per micro-batch.
4. **Trigger di Spark**: Spark Structured Streaming legge il nuovo file NDJSON e aggiorna le metriche a schermo in modalità `update`.

---

## 9. Risks and Mitigations

### 9.1 Perdita dati per crash NiFi durante il buffering
* *Rischio*: I record in attesa nel bin di `MergeContent` risiedono nella RAM/coda se il server si spegne.
* *Mitigazione*: Impostare la proprietà `FlowFile Storage` su `File` (default di NiFi) in modo che le code siano persistite su disco nel FlowFile Repository.

### 9.2 Latenza percepita nella demo
* *Rischio*: Attendere 10 secondi per vedere il dato in Spark può sembrare lento durante una presentazione.
* *Mitigazione*: Per la demo, il valore di `Max Bin Age` può essere impostato a `5 seconds` per velocizzare ulteriormente la visualizzazione, mantenendo comunque l'aggregazione dei record.

---

## 10. Final Recommendation

Il flow NiFi v2 è **completamente pronto per l'implementazione**. 

Utilizza esclusivamente processori standard ad alte prestazioni, risolve nativamente il small files problem e garantisce un formato dati NDJSON pulito e standardizzato per Apache Spark. È la configurazione ideale per una demo accademica solida, elegante e a prova di errore.
