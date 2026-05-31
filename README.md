# Real-Time Heart Rate Monitoring & Clinical Analytics Pipeline (IoMT MVP)

This repository contains the complete implementation of a Lambda/Streaming-style Big Data pipeline designed for **Remote Patient Monitoring (RPM)** and the **Internet of Medical Things (IoMT)**. 

The pipeline ingests real-time heart rate metrics from wearable devices, performs clinical-grade classification and data enrichment on the fly, buffers events into micro-batches to prevent filesystem degradation, stores them in a distributed filesystem, and computes running analytics (average, min, max, alert counts) in real time.

---

## 🏗️ Architecture & Flow Overview

The architecture is deployed on a **3-node Google Cloud Dataproc cluster** (1 Master, 2 Workers running Ubuntu 22 LTS) and is designed to showcase the specialized roles of HDFS, Apache NiFi, and Apache Spark:

```
[ Wearable Devices / Simulators ]
               │
               ▼  (HTTP POST to port 8080/heartrate via SSH Tunnel)
      ┌─────────────────────────────────┐
      │          Apache NiFi            │ (Data Ingestion, Validation,
      │    (Master Node Ingestion)      │  Enrichment, and Micro-Batching)
      └─────────────────────────────────┘
               │
               ▼  (Saves 10s NDJSON batches to HDFS)
      ┌─────────────────────────────────┐
      │         Hadoop HDFS             │ (Distributed Storage: Data blocks
      │      (Distributed Disks)        │  replicated across Worker Nodes)
      └─────────────────────────────────┘
               │
               ▼  (Continuous Directory File Stream)
      ┌─────────────────────────────────┐
      │ Apache Spark Structured Stream  │ (Distributed Analytics: Runs tasks on
      │  (YARN Client Deploy on YARN)   │  Workers, updates metrics in memory)
      └─────────────────────────────────┘
               │
               ▼  (Triggered every 5 seconds)
     [ Real-Time Console Dashboard ]
```

### Component Breakdown
1. **Apache NiFi (Ingestion & Routing)**: Acts as the entry gate. It ingests the raw JSON payloads, extracts data fields into attributes, evaluates clinical rules, logs critical events, enriches the JSON body with a calculated alert status, and groups the individual records into 10-second micro-batches (NDJSON format) before writing to HDFS.
2. **Hadoop HDFS (Distributed Storage)**: The storage layer. By using NiFi to batch files, we mitigate the **"small files problem"** on HDFS, reducing NameNode memory consumption by over 95%.
3. **Apache Spark Structured Streaming (Analytics)**: The processing engine. It reads the HDFS file stream, aggregates data by patient, and computes running statistics (averages, min/max range, and alert counts) across a distributed cluster managed by YARN.

---





## 📁 Repository Structure

* `heart_rate_nifi_flow.xml`: Apache NiFi template file containing the fully validated 8-processor pipeline.
* `heart_rate_streaming_v2.py`: PySpark Structured Streaming job.
* `simulate_data.py`: Python wearable simulator sending 15 pre-configured events (good for deterministic test cases).
* `simulate_high_load.py`: Python wearable simulator running in an infinite loop, generating data for 50 active patients with different medical profiles (ideal for showing continuous streaming load).
* `install_nifi.sh`: Automated shell script to install and configure Apache NiFi 1.19.1 in HTTP mode on the Dataproc Master node.
* `flow_nifi_v2.md`: Technical specification document for the NiFi components.
* `spark_job_v2.md`: Technical specification document for the PySpark job.
* `specifica_tecnica_heart_rate_v2.md`: Overall technical specification in Italian.

---

## 🛠️ Step-by-Step Setup & Execution

### Phase 1: Booting the Cluster Nodes
Ensure that your GCP Dataproc cluster is active. If the worker nodes are in a `TERMINATED` state to conserve cloud credits, you can start them by running the following command in your Master VM SSH console:
```bash
gcloud compute instances start <WORKER_0_NAME> <WORKER_1_NAME> --zone <GCP_ZONE>
```

### Phase 2: Opening the SSH Tunnel
Since the cluster ports are secured inside a private VPC, establish a secure SSH tunnel from your **local machine** to map the required interfaces. Replace `<GCP_USERNAME>` and `<EXTERNAL_MASTER_IP>` with your credentials:
```bash
ssh -i ~/.ssh/id_rsa -N \
  -L 8090:localhost:8090 \
  -L 8080:localhost:8080 \
  -L 9870:localhost:9870 \
  <GCP_USERNAME>@<EXTERNAL_MASTER_IP>
```
Leaving this terminal window open forwards:
* **NiFi Web UI**: `http://localhost:8090/nifi`
* **NiFi HTTP Ingest Endpoint**: `http://localhost:8080/heartrate`
* **HDFS Namenode Web Browser**: `http://localhost:9870`

---

### Phase 3: Configuring the NiFi Flow
1. Open your local web browser and navigate to `http://localhost:8090/nifi`.
2. Click the **Upload Template** icon on the left toolbar and upload the `heart_rate_nifi_flow.xml` file.
3. Drag and drop the **Template** icon from the top menu onto the canvas, select `HeartRate_MVP_v2`, and click **Add**.
4. Select all imported components (`Cmd+A` or `Ctrl+A`), right-click on the canvas, and click **Start**. 
5. Verify that all 8 processors display a green **Running** (play) icon in their top-left corner. *(Note: The PutHDFS processor also displays a red shield icon, which is a normal security badge indicating it is a Restricted component accessing the filesystem).*

---

### Phase 4: Submitting the Spark Streaming Job
1. Connect via SSH to the Master VM.
2. Ensure you are in your user home directory where `heart_rate_streaming_v2.py` is stored:
   ```bash
   cd ~
   ```
3. Submit the streaming job to YARN in `client` deploy mode to view the console output directly in your shell session. We restrict resources to 1 executor to avoid starvation on YARN:
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
4. Once initialized, the job prints the active listening logs and waits for new HDFS files:
   ```text
   =========================================================================
   [SPARK V2] Inizializzazione completata. Avvio Job di Streaming Analitico.
   =========================================================================
   [SPARK V2] Lettura stream in corso dalla cartella HDFS: hdfs:///user/<GCP_USERNAME>/nifi-demo/output
   ```

---

### Phase 5: Simulating Wearable Data Influx
Open a terminal on your **local machine** inside the project folder. You have two options depending on your presentation style:

#### Option A: Deterministic 15-Record Run (Standard Demo)
Sends exactly 15 pre-configured events simulating three patients (regular state, physical exercise, and tachycardia at rest):
```bash
python3 simulate_data.py
```

#### Option B: Continuous High-Load Stream (Realistic Stress Test)
Launches an infinite generator simulating **50 concurrent patients** in real-time. It randomly fluctuates metrics and activities, sending roughly 10 events per second. Press `Ctrl+C` to terminate the stream:
```bash
python3 simulate_high_load.py
```

---

## 🔍 Validation & Clinical Outcomes

### 1. Ingested Events (NiFi)
On the NiFi canvas, you will observe the flow files processing. The items will buffer in the queue before `MergeContent_Batcher` for a maximum of 10 seconds before being concatenated into a single NDJSON file and written to HDFS.

### 2. File Verification (HDFS)
Open your browser at `http://localhost:9870`, click **Utilities** -> **Browse the file system**, and navigate to `/user/<GCP_USERNAME>/nifi-demo/output` to see the generated JSON files.
Alternatively, inspect HDFS from the Master VM command line:
```bash
hdfs dfs -ls /user/<GCP_USERNAME>/nifi-demo/output
hdfs dfs -cat /user/<GCP_USERNAME>/nifi-demo/output/*.json
```

### 3. Real-Time Analytics Dashboard (Spark Console)
In the Master VM SSH session, Spark will process the incoming files every 5 seconds, displaying the updated health analytics:
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
* **Clinical Interpretation**: 
  * Patient `p003` has a slow heart rate while sleeping (`low_alerts = 3` due to bradycardia).
  * Patient `p002` is running, showing high heart rates (`high_alerts = 4` due to exertion).
  * Patient `p001` has high heart rates while resting (`critical_alerts = 2` due to acute tachycardia), triggering immediate concern for medical responders.


