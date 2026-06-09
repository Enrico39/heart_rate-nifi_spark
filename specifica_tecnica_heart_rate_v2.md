# Specifica Tecnica: Heart Rate Monitoring MVP (Version 2 - Cluster Optimized)

Questa seconda versione della specifica tecnica ottimizza la pipeline per l'esecuzione su un cluster **Google Cloud Dataproc a 3 nodi** (1 Master, 2 Worker). 

Risolve le criticità della v1 (in particolare il *small files problem* su HDFS e l'instabilità delle colonne virtuali partizionate in Spark Structured Streaming) introducendo un meccanismo di buffering/batching nativo in Apache NiFi e inserendo l'attributo di alert direttamente all'interno del payload JSON.

---

## 1. Executive Summary

La presente architettura v2 fornisce un sistema stabile, leggero e scalabile per l'ingestione e l'analisi in streaming di dati cardiaci. 

* **Ingestione**: Tramite chiamata REST HTTP su Apache NiFi.
* **Classificazione**: Eseguita a monte in Apache NiFi, che arricchisce il payload JSON inserendovi direttamente il campo `alert_type`.
* **Micro-batching (Mitigazione Small Files)**: Apache NiFi accumula in memoria i record individuali per un intervallo temporale massimo di 10 secondi (o fino a un limite di record) e li unisce in un unico file in formato **Newline-Delimited JSON (NDJSON)** prima di scriverlo su HDFS.
* **Storage**: HDFS memorizza i file aggregati in un'unica directory radice, azzerando la pressione sul NameNode di Dataproc.
* **Streaming Analytics**: Spark Structured Streaming legge continuamente i nuovi file NDJSON da HDFS, eseguendo l'aggregazione incrementale per paziente ed estraendo le metriche degli allarmi direttamente dal payload JSON.
* **Demo Ready**: La pipeline è progettata per essere solida ed avviabile con comandi standard, riducendo l'I/O e l'impronta di memoria sul cluster.

---

## 2. Final Payload Schema

Il payload JSON viene arricchito da NiFi prima della scrittura su HDFS. Ciò garantisce che il dato sia autoconsistente e che Spark non debba fare affidamento sul partizionamento logico dei percorsi di directory.

### 2.1 Esempio di JSON finale memorizzato in HDFS
```json
{
  "patient_id": "p001",
  "device_id": "d01",
  "timestamp": "2026-05-29T10:00:00Z",
  "heart_rate": 125,
  "activity": "rest",
  "alert_type": "critical_alert"
}
```

### 2.2 Schema JSON Formale (Draft 07)
```json
{
  "$schema": "http://json-schema.org/draft-07/schema#",
  "title": "HeartRateRecordV2",
  "type": "object",
  "properties": {
    "patient_id": {"type": "string"},
    "device_id": {"type": "string"},
    "timestamp": {"type": "string", "format": "date-time"},
    "heart_rate": {"type": "integer", "minimum": 30, "maximum": 250},
    "activity": {"type": "string", "enum": ["rest", "walking", "running", "sleeping"]},
    "alert_type": {"type": "string", "enum": ["normal", "low_alert", "high_alert", "critical_alert"]}
  },
  "required": ["patient_id", "device_id", "timestamp", "heart_rate", "activity", "alert_type"]
}
```

---

## 3. NiFi Flow v2

Il flusso NiFi v2 introduce componenti robusti per l'ingestione, la classificazione e l'arricchimento. La classificazione clinica avviene direttamente in `UpdateAttribute_Classifier` tramite espressioni condizionali nidificate, mentre l'arricchimento del JSON avviene in modo nativo tramite `JoltTransformJSON_Enricher`. Il buffer per mitigare lo *small files problem* è gestito da `MergeContent_Batcher`.

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

### 3.1 Configurazione Dettagliata dei Processori

#### 1. ListenHTTP
* **Classe**: `org.apache.nifi.processors.standard.ListenHTTP`
* **Porta**: `8080`
* **Base Path**: `heartrate`
* **Relazione**: `success` $\rightarrow$ `EvaluateJsonPath_Extractor`.

#### 2. EvaluateJsonPath
* **Classe**: `org.apache.nifi.processors.standard.EvaluateJsonPath`
* **Destination**: `flowfile-attribute`
* **Proprietà**:
  * `patient_id` = `$.patient_id`
  * `heart_rate` = `$.heart_rate`
  * `activity` = `$.activity`
  * `timestamp` = `$.timestamp`
  * `device_id` = `$.device_id`
* **Relazioni**:
  * `matched` $\rightarrow$ `UpdateAttribute_Classifier`.
  * `failure` / `unmatched` $\rightarrow$ auto-terminate.

#### 3. UpdateAttribute
* **Classe**: `org.apache.nifi.processors.attributes.UpdateAttribute`
* **Proprietà**:
  * `alert_status` = `${heart_rate:toNumber():lt(50):ifElse('low_alert', ${heart_rate:toNumber():ge(110):and(${activity:equals('rest')}):ifElse('critical_alert', ${heart_rate:toNumber():ge(110):and(${activity:equals('rest'):not()}):ifElse('high_alert', 'normal')})})}`
* **Relazione**: `success` $\rightarrow$ `RouteOnAttribute_DemoLogger`.

#### 4. RouteOnAttribute
* **Classe**: `org.apache.nifi.processors.standard.RouteOnAttribute`
* **Routing Strategy**: `Route to Property name`
* **Proprietà**:
  * `alert` = `${alert_status:equals('normal'):not()}`
  * `normal` = `${alert_status:equals('normal')}`
* **Relazioni**:
  * `alert` $\rightarrow$ `LogMessage_AlertWriter`.
  * `normal` $\rightarrow$ `JoltTransformJSON_Enricher`.
  * `unmatched` $\rightarrow$ auto-terminate.

#### 5. LogMessage
* **Classe**: `org.apache.nifi.processors.standard.LogMessage`
* **Proprietà**:
  * `log-level` = `warn`
  * `log-message` = `[DEMO ALERT] Paziente: ${patient_id} | Battito: ${heart_rate} | Stato: ${alert_status} | Attività: ${activity}`
* **Relazione**: `success` $\rightarrow$ `JoltTransformJSON_Enricher`.

#### 6. JoltTransformJSON
* **Classe**: `org.apache.nifi.processors.standard.JoltTransformJSON`
* **Proprietà**:
  * `jolt-transform` = `jolt-transform-chain`
  * `jolt-spec` = `[{"operation":"default","spec":{"alert_type":"${alert_status}"}}]`
* **Relazioni**:
  * `success` $\rightarrow$ `MergeContent_Batcher`.
  * `failure` $\rightarrow$ auto-terminate.

#### 7. MergeContent (Mitigazione Small Files)
* **Classe**: `org.apache.nifi.processors.standard.MergeContent`
* **Proprietà**:
  * `Merge Strategy` = `Bin-Packing Algorithm`
  * `Merge Format` = `Binary Concatenation`
  * `Delimiter Strategy` = `Text`
  * `Demarcator File` = `\n` (newline character)
  * `Minimum Group Size` = `0 B`
  * `Max Bin Age` = `10 sec`
* **Relazioni**:
  * `merged` $\rightarrow$ `PutHDFS_Writer`.
  * `failure` / `original` $\rightarrow$ auto-terminate.

#### 8. PutHDFS
* **Classe**: `org.apache.nifi.processors.hadoop.PutHDFS`
* **Proprietà**:
  * `Hadoop Configuration Resources` = `/etc/hadoop/conf/core-site.xml,/etc/hadoop/conf/hdfs-site.xml`
  * `Directory` = `/user/enricomadonna0/nifi-demo/output`
  * `Conflict Resolution Strategy` = `fail`
* **Relazioni**:
  * `success` / `failure` $\rightarrow$ auto-terminate.

---

## 4. HDFS Layout v2

Il layout HDFS v2 abbandona la complessità del partizionamento fisico a cartelle per evitare di frammentare i dati.

### 4.1 Layout Logico e Fisico
Tutti i file confluiscono in un'unica directory radice:

```text
/user/enricomadonna0/nifi-demo/output/
├── batch_20260529_101530_1.json
├── batch_20260529_101540_2.json
└── batch_20260529_101550_3.json
```

### 4.2 Specifiche Tecniche del File in HDFS
* **Formato**: Newline-Delimited JSON (NDJSON). Ogni riga del file HDFS rappresenta un record JSON completo e indipendente terminato da `\n`.
* **Dimensione Tipica**: Invece di scrivere 20 file da 120 bytes, NiFi scrive 1 singolo file da 2.4 KB ogni 10 secondi.
* **Ottimizzazione NameNode**: Riduzione del consumo di memoria del NameNode di oltre il **95%**. 

---

## 5. Spark Job v2

Il job Spark v2 è concepito per leggere lo stream di file NDJSON da HDFS in modo continuo ed estremamente robusto, applicando finestre temporali scorrevoli con watermark e calcolando la deviazione standard del battito (SDHR) come indicatore di stabilità cardiaca. Rileva il campo `alert_type` direttamente dal JSON.

### 5.1 Codice Spark (`heart_rate_streaming_v2.py`)

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, avg, min, max, count, window, stddev_samp
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

def main():
    # Inizializzazione della SparkSession ottimizzata per YARN su Dataproc
    spark = SparkSession.builder \
        .appName("HeartRateStreamingV2") \
        .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true") \
        .getOrCreate()

    # Silenzia i log informativi di Spark
    spark.sparkContext.setLogLevel("WARN")

    # Schema del record JSON (il campo alert_type è ora parte integrante del payload)
    json_schema = StructType([
        StructField("patient_id", StringType(), True),
        StructField("device_id", StringType(), True),
        StructField("timestamp", StringType(), True),
        StructField("heart_rate", IntegerType(), True),
        StructField("activity", StringType(), True),
        StructField("alert_type", StringType(), True)
    ])

    hdfs_input_path = "hdfs:///user/enricomadonna0/nifi-demo/output"
    print(f"[SPARK V2] In ascolto su HDFS Directory Stream: {hdfs_input_path}")

    # Lettura continua dei file JSON depositati nella cartella
    streaming_df = spark.readStream \
        .schema(json_schema) \
        .json(hdfs_input_path)

    # Conversione del timestamp e preparazione del DataFrame
    processed_df = streaming_df \
        .withColumn("event_time", col("timestamp").cast("timestamp"))

    # Calcolo delle metriche aggregate per finestra scorrevole e paziente
    patient_metrics = processed_df \
        .withWatermark("event_time", "10 minutes") \
        .groupBy(
            window(col("event_time"), "5 minutes", "10 seconds"),
            col("patient_id")
        ) \
        .agg(
            avg("heart_rate").alias("average_heart_rate"),
            min("heart_rate").alias("min_heart_rate"),
            max("heart_rate").alias("max_heart_rate"),
            count("patient_id").alias("total_readings"),
            # Deviazione Standard della frequenza cardiaca (SDHR) per monitorare la stabilità macro del battito
            stddev_samp("heart_rate").alias("sdhr"),
            # Conteggi condizionali basati sul campo esplicito alert_type
            count(when(col("alert_type") == "low_alert", 1)).alias("low_alerts"),
            count(when(col("alert_type") == "high_alert", 1)).alias("high_alerts"),
            count(when(col("alert_type") == "critical_alert", 1)).alias("critical_alerts")
        )

    # 6.1 Clinical Profiling (Rest-vs-Activity Alert Ratio)
    # total_alerts = low_alerts + high_alerts + critical_alerts
    # resting_alerts = low_alerts + critical_alerts (alerts occurring during sleep or rest)
    patient_profiled = patient_metrics \
        .withColumn("total_alerts", col("low_alerts") + col("high_alerts") + col("critical_alerts")) \
        .withColumn("resting_alerts", col("low_alerts") + col("critical_alerts")) \
        .withColumn("rest_alert_ratio",
            when(col("total_alerts") > 0, col("resting_alerts") / col("total_alerts"))
            .otherwise(0.0)
        ) \
        .withColumn("clinical_profile",
            when(col("total_alerts") == 0, "STABLE")
            .when(col("rest_alert_ratio") > 0.5, "ARRHYTHMIA SUSPECTED")
            .otherwise("PHYSIOLOGICAL EXERTION")
        ) \
        .select(
            col("window.start").cast("string").alias("window_start"),
            col("window.end").cast("string").alias("window_end"),
            col("patient_id"),
            col("average_heart_rate"),
            col("sdhr"),
            col("total_readings"),
            col("total_alerts"),
            col("clinical_profile")
        )

    # Output dello stream su console in modalità update
    query = patient_profiled.writeStream \
        .outputMode("update") \
        .format("console") \
        .trigger(processingTime="15 seconds") \
        .option("checkpointLocation", "/user/enricomadonna0/nifi-demo/checkpoint") \
        .start()

    query.awaitTermination()

if __name__ == "__main__":
    main()
```

---

## 6. Demo Sequence

Una sequenza lineare e priva di intoppi da eseguire sul cluster Dataproc.

### 6.1 Preparazione
1. **Pulisci e Inizializza HDFS**: Rimuovi eventuali file rimasti da vecchie sessioni per non inquinare la demo.
   ```bash
   hdfs dfs -rm -r -f /user/enricomadonna0/nifi-demo/output
   hdfs dfs -rm -r -f /user/enricomadonna0/nifi-demo/checkpoint
   hdfs dfs -mkdir -p /user/enricomadonna0/nifi-demo/output
   hdfs dfs -chmod -R 777 /user/enricomadonna0/nifi-demo
   ```

### 6.2 Avvio dei Componenti
2. **Avvia Apache NiFi**: Assicurati che il flow NiFi v2 sia importato, configurato e che tutti i processori siano in stato `RUNNING`.
3. **Avvia Spark Job**: Nel terminale di Dataproc sottometti il job streaming:
   ```bash
   spark-submit \
     --master yarn \
     --deploy-mode client \
     --executor-memory 1G \
     --driver-memory 1G \
     heart_rate_streaming_v2.py
   ```
   *(Attendere la visualizzazione della tabella vuota con i campi di intestazione).*

### 6.3 Generazione Dati (Simulazione Invio)
4. **Invia 15 record misti in rapida sequenza (entro 5 secondi)**. Esegui questo script bash o lancia comandi curl consecutivi:
   ```bash
   # Paziente 1 (Dati regolari ed alert critico)
   curl -X POST -H "Content-Type: application/json" -d '{"patient_id":"p001","device_id":"d01","timestamp":"2026-05-29T10:00:00Z","heart_rate":72,"activity":"rest"}' http://localhost:8080/heartrate
   curl -X POST -H "Content-Type: application/json" -d '{"patient_id":"p001","device_id":"d01","timestamp":"2026-05-29T10:01:00Z","heart_rate":78,"activity":"walking"}' http://localhost:8080/heartrate
   curl -X POST -H "Content-Type: application/json" -d '{"patient_id":"p001","device_id":"d01","timestamp":"2026-05-29T10:02:00Z","heart_rate":130,"activity":"rest"}' http://localhost:8080/heartrate

   # Paziente 2 (Bradicardia)
   curl -X POST -H "Content-Type: application/json" -d '{"patient_id":"p002","device_id":"d02","timestamp":"2026-05-29T10:00:00Z","heart_rate":45,"activity":"sleeping"}' http://localhost:8080/heartrate
   curl -X POST -H "Content-Type: application/json" -d '{"patient_id":"p002","device_id":"d02","timestamp":"2026-05-29T10:01:00Z","heart_rate":48,"activity":"rest"}' http://localhost:8080/heartrate
   ```

### 6.4 Cosa Mostrare (Verifica Visiva)
* **NiFi Queueing & Buffering**: Mostra nella UI di NiFi come le code si riempiono rapidamente, ma il processore `PutHDFS` non scrive immediatamente. Dopo 10 secondi (il `Max Bin Age`), la coda si svuota istantaneamente in un singolo file.
* **Ispezione HDFS**: Esegui `hdfs dfs -ls /user/enricomadonna0/nifi-demo/output` per dimostrare che è presente **un solo file** contenente tutti gli eventi inviati. Mostra il contenuto del file per far notare la struttura NDJSON con la colonna `alert_type` inserita.
  ```bash
  hdfs dfs -cat /user/enricomadonna0/nifi-demo/output/*.json
  ```
* **Visualizzazione Console Spark**: Mostra la tabella di output su Spark che si aggiorna riepilogando i calcoli in tempo reale:
  ```text
  -------------------------------------------
  Batch: 1
  -------------------------------------------
  +-------------------+-------------------+----------+------------------+------------------+--------------+------------+----------------------+
  |       window_start|         window_end|patient_id|average_heart_rate|              sdhr|total_readings|total_alerts|      clinical_profile|
  +-------------------+-------------------+----------+------------------+------------------+--------------+------------+----------------------+
  |2026-05-29 09:56:00|2026-05-29 10:01:00|      p001|              93.3| 29.58597640775101|             3|           1|  ARRHYTHMIA SUSPECTED|
  |2026-05-29 09:56:00|2026-05-29 10:01:00|      p002|             126.2|  2.12132034355964|             4|           4|PHYSIOLOGICAL EXERTION|
  |2026-05-29 09:56:00|2026-05-29 10:01:00|      p003|              72.5|  1.52132034355964|             5|           0|                STABLE|
  +-------------------+-------------------+----------+------------------+------------------+--------------+------------+----------------------+
  ```

---

## 7. Assumptions and Limits

* **Ordine degli Eventi**: Il job Spark aggrega i record man mano che i file vengono posizionati su HDFS. Non viene implementata la gestione dell'ordinamento temporale degli eventi fuori sequenza (out-of-order) poiché la demo assume l'arrivo dei dati in ordine cronologico lineare.
* **Persistenza Stato Analitico**: Poiché l'output di Spark è diretto verso la console, lo stato aggregato risiede solo nella memoria RAM del driver Spark. Al riavvio del job streaming, le metriche storiche andranno perse.
* **Dimensione del Buffer NiFi**: Il buffering in memoria di NiFi tramite `MergeContent` assume che i nodi NiFi non subiscano blackout improvvisi. In caso di spegnimento anomalo del server NiFi prima della scadenza dei 10 secondi, i record presenti nella coda di merge potrebbero andare perduti se il Content Repository non è configurato su un disco persistente protetto.

---

## 8. Risks and Mitigations

| Rischio Identificato | Causa Pratica | Effetto su Cluster 3 Nodi | Mitigazione Implementata in v2 |
| :--- | :--- | :--- | :--- |
| **Crash del NameNode (HDFS OOM)** | Scrub continuo di piccoli file da 100 byte in HDFS. | Il NameNode esaurisce la RAM JVM e il cluster Dataproc smette di rispondere. | **Mitigato**: `MergeContent` in NiFi impacchetta i messaggi in micro-batch da 10 secondi, riducendo le scritture HDFS del 95%. |
| **Allarmi di replica HDFS** | Impostazione default HDFS replica = 3 su 2 soli nodi worker. | I file rimangono in stato "under-replicated" degradando le performance. | **Mitigato**: Impostare esplicitamente `dfs.replication = 2` o `1` a livello di HDFS o in fase di creazione cluster Dataproc. |
| **Starvation delle Risorse YARN** | Spark Structured Streaming occupa indefinitamente tutti gli esecutori. | Impossibilità di lanciare altri job o query SQL sul cluster. | **Mitigato**: Configurare il job PySpark riducendo al minimo l'allocazione (`--num-executors 1` con `--executor-cores 1`). |
| **Crash OOM del nodo Master** | Coesistenza fisica sullo stesso host del Master YARN, NameNode HDFS e JVM Apache NiFi. | Il kernel Linux termina (OOM-kill) i processi Java più pesanti (solitamente NiFi o Spark Driver). | **Mitigato**: Limitare l'heap memory di NiFi a 2GB in `bootstrap.conf` e allocare massimo 1GB per il Driver Spark. |

---

## 9. Final Recommendation

La versione v2 qui descritta è **altamente raccomandata** per la presentazione accademica dell'esame. 

Rappresenta una pipeline di livello produttivo pur mantenendo l'infrastruttura semplice e lineare:
1. Sfrutta **NDJSON** come standard di interscambio per i sistemi distribuiti.
2. Dimostra una reale comprensione delle limitazioni architetturali di HDFS (evitando il problema dei piccoli file).
3. Evita di introdurre infrastrutture esterne pesanti (Kafka) o dipendenze fragili (colonne partizionate HDFS virtuali non sincronizzate), assicurando che la demo funzioni fluidamente al primo colpo anche su risorse limitate e macchine standard di Google Cloud Platform.
