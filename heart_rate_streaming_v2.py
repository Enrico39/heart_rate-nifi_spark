#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Job Spark Structured Streaming - Heart Rate Monitoring MVP (V2)
---------------------------------------------------------------
Questo script legge in streaming i file NDJSON aggregati scritti da NiFi su HDFS,
esegue il parsing dei dati in tempo reale e calcola metriche aggregate per paziente.

Per eseguire il job su Dataproc:
  spark-submit --master yarn --deploy-mode client heart_rate_streaming_v2.py
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, avg, min, max, count, window, stddev_samp
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

def main():
    # 1. Inizializzazione della SparkSession
    # La configurazione 'forceDeleteTempCheckpointLocation' previene conflitti tra riavvii consecutivi nella demo
    spark = SparkSession.builder \
        .appName("HeartRateStreamingV2") \
        .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true") \
        .getOrCreate()

    # Impostiamo il livello di logging a WARN per evitare che i log di sistema nascondano i risultati in console
    spark.sparkContext.setLogLevel("WARN")
    
    print("=========================================================================")
    print("[SPARK V2] Inizializzazione completata. Avvio Job di Streaming Analitico.")
    print("=========================================================================")

    # 2. Definizione del record JSON (corrispondente al payload arricchito da NiFi)
    json_schema = StructType([
        StructField("patient_id", StringType(), True),
        StructField("device_id", StringType(), True),
        StructField("timestamp", StringType(), True),
        StructField("heart_rate", IntegerType(), True),
        StructField("activity", StringType(), True),
        StructField("alert_type", StringType(), True)  # Inserito direttamente da NiFi v2
    ])

    # 3. Configurazione del path HDFS di input
    # In Dataproc, il path punta al NameNode HDFS attivo
    hdfs_input_path = "hdfs:///user/enricomadonna0/nifi-demo/output"

    print(f"[SPARK V2] Lettura stream in corso dalla cartella HDFS: {hdfs_input_path}")

    # 4. Lettura dello stream di file JSON
    # Spark rileva automaticamente i nuovi file aggiunti a questa cartella
    streaming_df = spark.readStream \
        .schema(json_schema) \
        .json(hdfs_input_path)

    # 5. Elaborazione e casting dei tipi
    # Convertiamo la stringa timestamp ISO-8601 in un tipo Timestamp nativo di Spark
    processed_df = streaming_df \
        .withColumn("event_time", col("timestamp").cast("timestamp"))

    # 6. Aggregazione Real-Time per paziente con sliding window e watermark
    # Applichiamo un watermark di 10 minuti per consentire la gestione di dati tardivi e pulire lo stato in memoria.
    # La finestra scorrevole è di 5 minuti, con uno scorrimento di 10 secondi.
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
            # Calcolo della deviazione standard del battito cardiaco come proxy dell'HRV (Heart Rate Variability)
            stddev_samp("heart_rate").alias("hrv_sdhr"),
            # Conteggi condizionali degli allarmi basati sul campo 'alert_type' arricchito da NiFi
            count(when(col("alert_type") == "low_alert", 1)).alias("low_alerts"),
            count(when(col("alert_type") == "high_alert", 1)).alias("high_alerts"),
            count(when(col("alert_type") == "critical_alert", 1)).alias("critical_alerts")
        ) \
        .select(
            col("window.start").cast("string").alias("window_start"),
            col("window.end").cast("string").alias("window_end"),
            col("patient_id"),
            col("average_heart_rate"),
            col("min_heart_rate"),
            col("max_heart_rate"),
            col("total_readings"),
            col("hrv_sdhr"),
            col("low_alerts"),
            col("high_alerts"),
            col("critical_alerts")
        )

    # 7. Scrittura dei risultati sulla Console
    # 'update' mode assicura che in console compaiano solo le righe che hanno subito modifiche
    # nell'ultimo micro-batch (ottimale per la demo live)
    query = patient_metrics.writeStream \
        .outputMode("update") \
        .format("console") \
        .trigger(processingTime="5 seconds") \
        .option("checkpointLocation", "/user/enricomadonna0/nifi-demo/checkpoint") \
        .start()

    # Mantiene il job attivo in attesa di nuovi dati
    query.awaitTermination()

if __name__ == "__main__":
    main()
