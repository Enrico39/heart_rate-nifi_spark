# Spark Job Specification: Heart Rate Monitoring MVP (V2)

Questo documento definisce in modo completo l'architettura, la logica di elaborazione, la configurazione e i comandi operativi per il job **Apache Spark v2** dedicato all'aggregazione in streaming dei dati di frequenza cardiaca su Google Cloud Dataproc.

---

## 1. Executive Summary

La strategia scelta per l'analitica real-time è **Spark Structured Streaming** con sorgente basata su HDFS (`File Stream`). Questa scelta soddisfa i requisiti di elaborazione streaming tipici dei corsi universitari di Big Data, introducendo al contempo ottimizzazioni per la stabilità su un cluster piccolo a 3 nodi.

* **Modello di Ingestione**: Directory Streaming. Spark monitora costantemente la cartella HDFS `/user/enricomadonna0/nifi-demo/output/` alla ricerca di nuovi file in formato Newline-Delimited JSON (NDJSON) depositati da Apache NiFi.
* **Ottimizzazione Risorse**: Il job viene avviato in modalità Client limitando rigorosamente l'allocazione a **1 solo esecutore con 1 core** per prevenire la starvation delle risorse su YARN.
* **Output Mode**: `update`. Vengono emesse in console le sole righe relative ai pazienti per cui sono pervenuti nuovi dati nell'ultimo trigger temporale (impostato a 5 secondi), minimizzando l'overhead di I/O dello schermo.

---

## 2. Input Contract

Il job Spark si aspetta che i dati inseriti in HDFS rispettino il formato **Newline-Delimited JSON (NDJSON)**. Ogni riga all'interno del file HDFS deve contenere un oggetto JSON autonomo e validabile secondo la seguente struttura logica:

### 2.1 Esempio di Record in Ingresso (Singola riga del file)
```json
{"patient_id": "p001", "device_id": "d01", "timestamp": "2026-05-29T10:00:00Z", "heart_rate": 125, "activity": "rest", "alert_type": "critical_alert"}
```

### 2.2 Schema Esplicito (StructType)
Per evitare errori di interpretazione dei tipi (in particolare per il timestamp e l'heart_rate), viene forzato uno schema esplicito all'avvio dello stream:

* `patient_id` $\rightarrow$ `StringType` (Chiave di aggregazione)
* `device_id` $\rightarrow$ `StringType` (Identificativo del wearable)
* `timestamp` $\rightarrow$ `StringType` (Formato ISO-8601 UTC)
* `heart_rate` $\rightarrow$ `IntegerType` (Frequenza cardiaca in bpm)
* `activity` $\rightarrow$ `StringType` (Stato del paziente: rest, walking, running, sleeping)
* `alert_type` $\rightarrow$ `StringType` (Classificato da NiFi: normal, low_alert, high_alert, critical_alert)

---

## 3. Processing Logic

La pipeline di calcolo del job esegue i seguenti passaggi:

```
[HDFS raw/*.json] 
       |
       v  (spark.readStream)
[Streaming DataFrame (Unparsed)]
       |
       v  (Schema Validation & Cast)
[Processed DataFrame (timestamp -> TimestampType)]
       |
       v  (groupBy patient_id & aggregazioni condizionali)
[Aggregated DataFrame (average, min, max, alert counts)]
       |
       v  (writeStream in OutputMode update)
[Console Out (Micro-Batch triggers ogni 5 secondi)]
```

1. **Lettura Continua**: Carica i file NDJSON come flusso di dati non strutturato tramite `.readStream` sulla directory `/user/enricomadonna0/nifi-demo/output`.
2. **Validazione dello Schema**: Mappa i dati in ingresso sullo schema statico definito.
3. **Conversione Temporale**: Converte il campo stringa `timestamp` in un tipo temporale nativo (`TimestampType`) di Spark usando la funzione `.cast("timestamp")`. Questa colonna è pronta per eventuali future estensioni con finestre temporali (*windowing* o *watermarking*).
4. **Calcolo Metriche**: Raggruppa i record per paziente e applica le aggregazioni.
5. **Invio al Console Sink**: Scrive i risultati intermedi dell'aggregazione su stdout.

---

## 4. Aggregations

Il job effettua aggregazioni cumulative per ciascun `patient_id`. Vengono calcolati in tempo reale i seguenti indicatori statistici:

| Metrica | Funzione Spark Utilizzata | Descrizione |
| :--- | :--- | :--- |
| **Media Battiti** | `avg("heart_rate")` | Calcola la media mobile dei bpm storici ricevuti per il paziente. |
| **Minimo Battito** | `min("heart_rate")` | Registra il valore di bpm più basso rilevato dal wearable. |
| **Massimo Battito** | `max("heart_rate")` | Registra il valore di bpm più alto rilevato dal wearable. |
| **Letture Totali** | `count("patient_id")` | Numero totale di campionamenti inviati dal dispositivo. |
| **Low Alerts** | `count(when(col("alert_type") == "low_alert", 1))` | Conteggio di eventi con frequenza cardiaca inferiore a 50 bpm. |
| **High Alerts** | `count(when(col("alert_type") == "high_alert", 1))` | Conteggio di eventi tachicardici compatibili con attività fisica. |
| **Critical Alerts** | `count(when(col("alert_type") == "critical_alert", 1))` | Conteggio di eventi tachicardici rilevati a riposo (critici). |

---

## 5. Spark Implementation

Di seguito viene riportato il codice completo del job PySpark da salvare come [heart_rate_streaming_v2.py](file:///Users/enricomadonna/Documents/antigravity/heart_rate-nifi_spark/heart_rate_streaming_v2.py):

```python
#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, avg, min, max, count
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

def main():
    # Inizializzazione della sessione Spark ottimizzata per Dataproc
    spark = SparkSession.builder \
        .appName("HeartRateStreamingV2") \
        .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true") \
        .getOrCreate()

    # Disabilitazione dei log informativi eccessivi per mantenere la console pulita durante la demo
    spark.sparkContext.setLogLevel("WARN")

    # Definizione dello schema del record JSON arricchito da NiFi
    json_schema = StructType([
        StructField("patient_id", StringType(), True),
        StructField("device_id", StringType(), True),
        StructField("timestamp", StringType(), True),
        StructField("heart_rate", IntegerType(), True),
        StructField("activity", StringType(), True),
        StructField("alert_type", StringType(), True)
    ])

    hdfs_input_path = "hdfs:///user/enricomadonna0/nifi-demo/output"
    print(f"=========================================================================")
    print(f"[SPARK V2] Job Avviato. Monitoraggio della cartella HDFS: {hdfs_input_path}")
    print(f"=========================================================================")

    # Lettura dello stream da HDFS
    streaming_df = spark.readStream \
        .schema(json_schema) \
        .json(hdfs_input_path)

    # Conversione del timestamp in tipo Timestamp di Spark
    processed_df = streaming_df \
        .withColumn("event_time", col("timestamp").cast("timestamp"))

    # Calcolo delle metriche aggregate per paziente
    patient_metrics = processed_df \
        .groupBy("patient_id") \
        .agg(
            avg("heart_rate").alias("average_heart_rate"),
            min("heart_rate").alias("min_heart_rate"),
            max("heart_rate").alias("max_heart_rate"),
            count("patient_id").alias("total_readings"),
            # Conteggi condizionali basati sul campo esplicito alert_type
            count(when(col("alert_type") == "low_alert", 1)).alias("low_alerts"),
            count(when(col("alert_type") == "high_alert", 1)).alias("high_alerts"),
            count(when(col("alert_type") == "critical_alert", 1)).alias("critical_alerts")
        )

    # Scrittura dello stream analitico su console con outputMode "update"
    query = patient_metrics.writeStream \
        .outputMode("update") \
        .format("console") \
        .trigger(processingTime="5 seconds") \
        .option("checkpointLocation", "/user/enricomadonna0/nifi-demo/checkpoint") \
        .start()

    query.awaitTermination()

if __name__ == "__main__":
    main()
```

---

## 6. Run Instructions

Per lanciare il job Spark sul cluster Dataproc evitando la saturazione delle risorse di calcolo, attenersi scrupolosamente al seguente comando:

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

### Parametri di Avvio Spiegati:
* `--master yarn`: Delega ad Apache Hadoop YARN (gestore risorse di Dataproc) l'allocazione dei container di esecuzione.
* `--deploy-mode client`: Esegue il driver Spark direttamente sulla shell del nodo Master su cui si lancia il comando. Questo consente di visualizzare l'output in tempo reale direttamente sul terminale.
* `--num-executors 1`: Richiede a YARN un solo executor. In un cluster con soli 2 nodi worker, questo assicura che l'altro nodo worker rimanga interamente libero per altre elaborazioni o per la tolleranza ai guasti.
* `--executor-cores 1`: Alloca un solo core CPU per l'executor, riducendo l'overhead energetico e computazionale.

---

## 7. Expected Output

All'avvio, il job attende i file da HDFS. Quando il simulatore invia il batch di record e NiFi lo scrive in HDFS, lo schermo visualizzerà il seguente output strutturato:

```text
-------------------------------------------
Batch: 1
-------------------------------------------
+----------+------------------+--------------+--------------+--------------+----------+-----------+---------------+
|patient_id|average_heart_rate|min_heart_rate|max_heart_rate|total_readings|low_alerts|high_alerts|critical_alerts|
+----------+------------------+--------------+--------------+--------------+----------+-----------+---------------+
|      p001|              93.3|            72|           130|             3|         0|          0|              1|
|      p002|              46.5|            45|            48|             2|         2|          0|              0|
+----------+------------------+--------------+--------------+--------------+----------+-----------+---------------+
```

Se vengono successivamente inviati nuovi record (es. per il paziente `p001`), dopo 5 secondi comparirà il batch successivo contenente **solo** le informazioni aggiornate:

```text
-------------------------------------------
Batch: 2
-------------------------------------------
+----------+------------------+--------------+--------------+--------------+----------+-----------+---------------+
|patient_id|average_heart_rate|min_heart_rate|max_heart_rate|total_readings|low_alerts|high_alerts|critical_alerts|
+----------+------------------+--------------+--------------+--------------+----------+-----------+---------------+
|      p001|              89.0|            70|           130|             4|         0|          0|              1|
+----------+------------------+--------------+--------------+--------------+----------+-----------+---------------+
```

---

## 8. Risks and Caveats

* **YARN Queue Bloat (Starvation)**: Poiché Structured Streaming è un processo continuo, manterrà l'allocazione delle risorse YARN a tempo indeterminato. Se la demo prevede l'esecuzione concomitante di altri job (es. query SQL o job batch paralleli), questi rimarranno in stato `ACCEPTED` in attesa che lo streaming termini.
  * *Mitigazione*: Impostare rigorosamente i limiti di esecutori a `1` o arrestare il job streaming (tramite `Ctrl+C`) prima di lanciare altri script.
* **Lettura di file parziali (Race Condition)**: Se NiFi scrive un file direttamente in `/user/enricomadonna0/nifi-demo/output`, Spark potrebbe tentare di leggerlo mentre è ancora in fase di scrittura, generando eccezioni di file corrotto o parziale.
  * *Mitigazione*: Configurare la strategia di scrittura di `PutHDFS` su NiFi su **`Write and Rename`** (comportamento predefinito e sicuro). NiFi scriverà un file temporaneo nascosto ed eseguirà la rinomina atomica solo a scrittura ultimata.
* **Checkpoint e Stato Storico**: Lo stato di Structured Streaming (le medie accumulate) è conservato in memoria. Un riavvio del job Spark streaming senza una directory di checkpoint persistente su HDFS causerà il ricalcolo delle sole metriche contenute nei file attualmente presenti in HDFS, perdendo l'ordine storico dei batch precedenti se i file HDFS vecchi vengono archiviati.

---

## 9. Final Recommendation

La soluzione di streaming v2 basata su PySpark Structured Streaming è **pienamente idonea per la presentazione d'esame**. 

Offre il perfetto compromesso tra requisiti didattici (uso obbligatorio di streaming per l'analitica) e robustezza infrastrutturale reale. L'introduzione dello schema rigido con `alert_type` pre-calcolato a monte da NiFi rende il codice Spark incredibilmente snello (meno di 60 righe), pulito e facilissimo da spiegare alla commissione durante l'interrogazione orale.
