# Heart Rate Monitoring MVP

Questo progetto implementa un MVP di una pipeline distribuita per il monitoraggio remoto del battito cardiaco usando **Apache NiFi**, **HDFS** e **Apache Spark** su un piccolo cluster Google Cloud Dataproc. L'obiettivo è dimostrare una pipeline end-to-end semplice, realistica e presentabile all'esame, evitando dipendenze legacy e componenti non necessari [web:248][web:256].

## Scenario

Un insieme di dispositivi wearable invia periodicamente dati di battito cardiaco in formato JSON. La pipeline riceve questi eventi via HTTP, li valida e li instrada con NiFi, li salva in HDFS e li analizza con Spark Structured Streaming per produrre statistiche e conteggi di alert per paziente [web:237][web:239][web:248].

## Obiettivo del MVP

Il sistema deve mostrare chiaramente il ruolo di ogni componente:
- **NiFi** riceve i dati, estrae i campi principali, classifica i record in base a soglie semplici e li salva in HDFS [web:217][web:221][web:227].
- **HDFS** conserva i dati grezzi della pipeline in cartelle facilmente ispezionabili [web:239].
- **Spark** legge i nuovi file JSON da HDFS e calcola aggregazioni elementari come media, min, max e numero di alert per paziente usando Structured Streaming [web:248][web:249].

## Payload di input

Ogni evento deve avere questo formato minimo:

```json
{
  "patient_id": "p001",
  "device_id": "d01",
  "timestamp": "2026-05-29T10:00:00Z",
  "heart_rate": 92,
  "activity": "rest"
}
```

Campi obbligatori:
- `patient_id`
- `device_id`
- `timestamp`
- `heart_rate`
- `activity`

## Regole di classificazione

Le regole del MVP sono volutamente semplici:
- `heart_rate < 50` -> `low_alert`
- `50 <= heart_rate <= 110` -> `normal`
- `heart_rate > 110` -> `high_alert`
- opzionale: se `activity = rest` e `heart_rate > 110` -> `critical_alert` [web:237][web:242]

## Architettura

```text
Producer (curl / script)
        |
        v
     Apache NiFi
(ListenHTTP -> EvaluateJsonPath -> RouteOnAttribute -> PutHDFS)
        |
        v
       HDFS
(/user/enricomadonna0/nifi-demo/output/)
        |
        v
Apache Spark Structured Streaming
        |
        v
Console / summary output
```

## Componenti del flow NiFi

Il flow NiFi minimo previsto è:
1. `ListenHTTP` per ricevere eventi JSON via HTTP [web:221].
2. `EvaluateJsonPath` per estrarre `patient_id`, `heart_rate`, `activity`, `timestamp` [web:217].
3. `RouteOnAttribute` per classificare gli eventi in `normal`, `low_alert`, `high_alert`, `critical_alert` [web:217].
4. `PutHDFS` per salvare gli eventi JSON in HDFS [web:227].
5. opzionale `LogMessage` per rendere visibili gli alert in demo [web:217].

## Struttura HDFS consigliata

Per semplicità il MVP può usare una sola directory:

```text
/user/enricomadonna0/nifi-demo/output/
```

In alternativa, se vuoi separare i casi:

```text
/user/enricomadonna0/nifi-demo/output/normal/
/user/enricomadonna0/nifi-demo/output/alerts/
```

## Comportamento Spark

Spark Structured Streaming legge file JSON dalla directory HDFS e produce metriche semplici per paziente [web:248][web:249].

Metriche MVP suggerite:
- media `heart_rate` per `patient_id`
- minimo e massimo `heart_rate`
- conteggio record per paziente
- conteggio alert per paziente

Per un cluster piccolo, l'output migliore per la demo è la **console** oppure una directory HDFS di summary [web:248][web:260].

## Sequenza demo

1. Avvia NiFi.
2. Avvia il job Spark Structured Streaming.
3. Invia 5-10 eventi JSON via `curl` o script.
4. Verifica che NiFi scriva i file in HDFS.
5. Mostra che Spark rileva i nuovi file e aggiorna le statistiche [web:248][web:257].

## Scope del MVP

### Incluso
- ingestione HTTP
- parsing JSON
- routing per soglie
- scrittura HDFS
- analytics Spark semplici

### Escluso
- Kafka
- WebSocket
- dashboard web avanzata
- machine learning
- email reali
- Cassandra / database esterni [web:244][web:260]

## Perché questo MVP è adatto all'esame

Questo MVP è abbastanza semplice da essere dimostrabile su un cluster Dataproc con 3 nodi, ma abbastanza ricco da mostrare una vera pipeline distribuita con ingestione, persistenza e analytics [web:248][web:256]. La semplicità è un vantaggio: riduce il rischio di rotture in demo e rende più chiaro il ruolo di NiFi rispetto a Spark [web:219][web:244].
