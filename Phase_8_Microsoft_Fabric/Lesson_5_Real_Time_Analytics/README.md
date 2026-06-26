# Lesson 5: Real-Time Analytics in Microsoft Fabric

> **Goal:** Understand how Microsoft Fabric handles real-time data — from ingesting live event streams with Eventstream, storing and querying them with KQL Database (Kusto), triggering automated alerts with Activator, and visualizing live data in Real-Time Dashboards.

---

## 🏗️ Phase 1: The Real-Time Analytics Stack in Fabric

### 1. What is Real-Time Analytics?

**Batch analytics** = Process data that already arrived (every hour, daily).
**Real-time analytics** = Query and react to data *as it arrives* (within seconds or milliseconds).

```
Examples of real-time analytics use cases:
──────────────────────────────────────────
• IoT sensor monitoring    → Machine vibration spikes above threshold → ALERT maintenance team
• E-commerce monitoring    → Orders per minute drops by 30% → ALERT dev ops (possible outage)
• Fraud detection          → Card used in two countries within 3 minutes → BLOCK transaction
• Log analytics            → Error rate in app logs exceeds 5% → ALERT on-call engineer
• Supply chain             → Truck GPS shows 2-hour delay → Re-route downstream shipment
```

### 2. Fabric Real-Time Analytics Architecture

```
Real-Time Data Sources
        │
        ▼
┌───────────────────┐
│   Eventstream     │  ← The ingestion layer (Kafka-compatible, no-code)
│  (like Kafka)     │       • Azure Event Hubs
└────────┬──────────┘       • Azure IoT Hub
         │                  • Custom App (SDK)
         │ routes to        • Kafka endpoint
         ▼
┌───────────────────────────────────────────────────────────────┐
│                     KQL Database (Kusto)                      │  ← Storage + query engine
│  Tables auto-ingest from Eventstream in real-time            │      (time-series optimized)
│  Query with KQL (Kusto Query Language)                        │
└────────────────────────────────┬──────────────────────────────┘
                                 │
          ┌──────────────────────┼──────────────────────┐
          ▼                      ▼                      ▼
 ┌─────────────────┐  ┌──────────────────┐  ┌────────────────────┐
 │  Real-Time      │  │   Activator      │  │  Fabric Lakehouse  │
 │  Dashboard      │  │   (Alerts)       │  │  (OneLake mirror)  │
 │  (live charts)  │  │  Send email,     │  │  Archive events    │
 └─────────────────┘  │  Teams msg,      │  │  in Delta format   │
                       │  trigger pipeline│  └────────────────────┘
                       └──────────────────┘
```

---

## 🚀 Phase 2: Eventstream — Real-Time Data Ingestion

### 1. What is Eventstream?

**Eventstream** is Fabric's fully managed, no-code event streaming service. Think of it as **Apache Kafka** — but with a drag-and-drop visual editor and zero infrastructure.

```
Eventstream = Kafka managed by Microsoft
• You don't manage brokers, partitions, or replication
• 200+ connectors: Event Hubs, IoT Hub, custom apps, Kafka clusters
• Built-in transformations (filter, project, aggregate) before storing
• Routes the same stream to MULTIPLE destinations simultaneously
```

### 2. Creating an Eventstream

```
Step-by-Step: Create an Eventstream
────────────────────────────────────
1. Workspace → New → Eventstream
   Name: "IoTSensorStream"

2. Add Source (click + Source):
   Choose: Azure Event Hub
   Connection: <your Event Hub namespace>
   Event Hub: "iot-sensors"
   Consumer Group: "$Default"
   Format: JSON

3. The stream preview appears showing live JSON events:
   {"sensor_id": "S-42", "temperature": 72.3, "timestamp": "2024-04-20T10:31:22Z"}
   {"sensor_id": "S-17", "temperature": 68.1, "timestamp": "2024-04-20T10:31:23Z"}

4. Add Destination (click + Destination):
   Choose: KQL Database
   KQL Database: "SensorKQLDB"
   Table: "sensor_readings"   (auto-created if not existing)
   Ingestion format: JSON

5. (Optional) Add a SECOND Destination simultaneously:
   Choose: Lakehouse
   Lakehouse: "SensorLakehouse"
   Table: "bronze_iot_events"   (archives events in Delta format)
```

### 3. Adding Real-Time Transformations in Eventstream

Before data reaches the KQL Database, you can transform it inline:

```
Eventstream Transformations (no-code, visual):

[Source: Event Hub]
      ↓
[Filter: temperature > 0 AND sensor_id IS NOT NULL]   ← Remove bad data
      ↓
[Expand: Parse JSON payload column]                    ← Unnest nested JSON
      ↓
[Aggregate: Tumbling window 1 minute]                  ← Compute avg temp per sensor per minute
  GROUP BY sensor_id, TumblingWindow(1 minute)
  AVG(temperature) AS avg_temp
  MAX(temperature) AS max_temp
      ↓
[Destination: KQL Database → "sensor_1min_agg" table]
```

```
Supported window types:
• Tumbling Window  → Fixed, non-overlapping time buckets (e.g., every 1 min)
• Hopping Window   → Overlapping buckets (e.g., 5-min window sliding every 1 min)
• Session Window   → Closes after a gap of inactivity
```

### 4. Sending Custom Events via SDK

If you don't use Event Hubs, you can send events directly from your app:

```python
# Send events to Fabric Eventstream Custom App endpoint
# Install: pip install azure-eventhub

from azure.eventhub import EventHubProducerClient, EventData
import json
from datetime import datetime, timezone

# Get the connection string from Eventstream → Source → Custom App
CONNECTION_STR = "Endpoint=sb://xyz.servicebus.windows.net/;..."
EVENTHUB_NAME  = "fabric-eventstream-custom"

producer = EventHubProducerClient.from_connection_string(
    conn_str=CONNECTION_STR,
    eventhub_name=EVENTHUB_NAME
)

# Send a batch of events
with producer:
    event_batch = producer.create_batch()
    for i in range(100):
        event = {
            "sensor_id": f"S-{i:03d}",
            "temperature": 65.0 + (i % 20),
            "humidity": 40 + (i % 15),
            "timestamp": datetime.now(timezone.utc).isoformat()
        }
        event_batch.add(EventData(json.dumps(event)))
    producer.send_batch(event_batch)
    print("100 events sent to Fabric Eventstream")
```

---

## 🔍 Phase 3: KQL Database — Querying Real-Time Data

### 1. What is KQL (Kusto Query Language)?

KQL is a **read-only query language** optimized for **time-series and log analytics**. It is used in:
- Microsoft Fabric (Real-Time Analytics)
- Azure Monitor / Log Analytics
- Microsoft Sentinel (SIEM)
- Azure Data Explorer

```
KQL vs SQL — Side-by-Side:

SQL:                                         KQL:
────────────────────────────────────────     ────────────────────────────────────────
SELECT *                                     sensor_readings
FROM sensor_readings                         | where timestamp > ago(1h)
WHERE timestamp > DATEADD(hour,-1,GETDATE()) | where temperature > 70
  AND temperature > 70                       | project sensor_id, temperature, timestamp
ORDER BY timestamp DESC                      | order by timestamp desc

Key difference: KQL uses PIPE (|) to chain operations, like Unix shell pipes.
Each | takes the result from the previous step and transforms it further.
```

### 2. Essential KQL Queries

```kql
// ── Basic selection ──────────────────────────────────────────────
sensor_readings
| where timestamp > ago(1h)          // Last 1 hour
| where sensor_id startswith "S-4"   // Sensor IDs starting with S-4
| limit 100                           // Show only first 100 rows

// ── Aggregation over time ────────────────────────────────────────
sensor_readings
| where timestamp > ago(24h)
| summarize
    avg_temp = avg(temperature),
    max_temp = max(temperature),
    reading_count = count()
  by sensor_id, bin(timestamp, 1h)   // bin() = group by 1-hour buckets
| order by timestamp desc

// ── Find anomalies: sensors above threshold ──────────────────────
sensor_readings
| where timestamp > ago(1h)
| summarize max_temp = max(temperature) by sensor_id
| where max_temp > 85                 // Sensors that spiked above 85°C
| join kind=inner (                   // Enrich with sensor metadata
    sensor_metadata
    | project sensor_id, location, machine_name
  ) on sensor_id
| project sensor_id, machine_name, location, max_temp
| order by max_temp desc

// ── Render a time chart ─────────────────────────────────────────
sensor_readings
| where timestamp > ago(6h)
| summarize avg_temp = avg(temperature) by sensor_id, bin(timestamp, 10m)
| render timechart                    // ← Renders a beautiful live chart in the KQL editor!

// ── Detect consecutive errors (session analysis) ─────────────────
app_logs
| where timestamp > ago(1h)
| where level == "ERROR"
| summarize error_count = count() by service, bin(timestamp, 5m)
| where error_count > 10
| order by timestamp desc
```

### 3. KQL Database Internals — Why It's So Fast

```
KQL Database (Kusto) uses column-store + streaming ingestion:

Traditional DB:          KQL Database (Kusto):
─────────────────        ─────────────────────────────────────────
Row-based storage    →   Column-store (like Parquet, but optimized for queries)
Index-based reads    →   Encoding + compression per column (10–20× smaller than raw)
Slow for full scans  →   Parallel shard scanning (1 billion rows in < 1 second)
Needs SQL ANALYZE    →   Auto-builds extents (data shards) on ingest

Ingestion latency:   →   <1 second from Eventstream to queryable!
```

### 4. KQL Database Tables — Retention and Caching

```
Creating a table with custom retention:

// In KQL Query editor:
.create table sensor_readings (
    sensor_id:  string,
    temperature: real,
    humidity:   real,
    timestamp:  datetime
)

// Set hot cache = 7 days (data in SSD — fastest queries)
// Set retention = 90 days (data on HDD/blob — still queryable, slightly slower)
.alter table sensor_readings
  policy caching hot = 7d

.alter table sensor_readings
  policy retention softdelete = 90d

### 5. KQL Update Policies — The "Real-Time ETL"
**Update Policies** in KQL allow you to automatically transform data as it lands in a source table and write the result into a different "derived" table.
*   **The Move:** It's like a database trigger. When data hits the `raw_telemetry` table, an Update Policy runs a KQL query to clean/standardize it and inserts it into `clean_telemetry`.
*   **Architect Note:** This is the most efficient way to do "Real-Time ETL" in Fabric without needing a Spark Streaming cluster.
```

---

## 🚨 Phase 4: Activator — Automated Alerts

### 1. What is Activator?

**Activator** (previously called "Data Activator") is Fabric's no-code **alert and action engine**. It watches your KQL Database or Power BI report for conditions and triggers actions automatically.

```
Activator = "If-This-Then-That" for real-time data

Examples:
  IF  avg temperature of Sensor-42 > 85°C for 5 consecutive minutes
  THEN  send Teams message to #operations channel
        AND  trigger Pipeline "shutdown_machine_42"

  IF  orders per minute drops below 50 (vs. 30-day avg of 200)
  THEN  send email to on-call engineer
        AND  create PagerDuty incident

  IF  a Power BI value (daily revenue) is 30% below target
  THEN  send email to sales manager
```

### 2. Creating an Activator Rule

```
Step-by-Step: Set up an Activator alert
─────────────────────────────────────────
1. Open your KQL Database → Right-click table → "Set Alert"
   (Or: Workspace → New → Activator)

2. Define the data source:
   Source: KQL Database "SensorKQLDB"
   Table:  "sensor_1min_agg"
   Column to monitor: "max_temp"
   Group by: "sensor_id"         ← Monitor EACH sensor independently

3. Define the condition:
   WHEN: max_temp > 85
   FOR: 3 consecutive windows     ← Sustained breach (avoids false alerts from spikes)

4. Define the action:
   Action type: Send Email
   To: ops-team@company.com
   Subject: "ALERT: Sensor {sensor_id} overheating — {max_temp}°C"
   Body: "Machine at {location} has exceeded 85°C for 3 minutes. Please investigate."

5. (Optional) Add a second action:
   Action type: Start Fabric Pipeline
   Pipeline: "emergency_shutdown_pipeline"
   Parameters: {sensor_id: {sensor_id}}

6. Set a cooldown period: 30 minutes
   (Don't re-alert for the same sensor for 30 min after the first alert)
```

### 3. Activator from Real-Time Dashboard

You can also set Activator alerts directly on a Power BI or Real-Time Dashboard visual:

```
Dashboard → Click on a chart → "Set Alert"
  Condition: "total_orders" in the last 5 minutes < 50
  Action: Send Teams notification to #ecommerce-ops

This alert refreshes every minute and fires whenever the condition is true.
```

---

## 📊 Phase 5: Real-Time Dashboards

### 1. Creating a Real-Time Dashboard

A **Real-Time Dashboard** in Fabric is connected live to a KQL Database. Unlike Power BI (which caches data), this dashboard auto-refreshes on a set interval (as fast as every 30 seconds):

```
New → Real-Time Dashboard → "SensorMonitoringDashboard"

Add a tile:
  Data source: "SensorKQLDB"
  KQL Query:
    sensor_readings
    | where timestamp > ago(1h)
    | summarize avg_temp = avg(temperature) by sensor_id, bin(timestamp, 1m)
    | render timechart

  Visual type: Line chart
  Refresh: Every 30 seconds

Add another tile:
  KQL Query:
    sensor_readings
    | where timestamp > ago(5m)
    | summarize latest_temp = max(temperature) by sensor_id
    | where latest_temp > 80
    | order by latest_temp desc

  Visual type: Table (highlights sensors currently running hot)
  Refresh: Every 30 seconds

Add a stat tile (single big number):
  KQL Query:
    sensor_readings
    | where timestamp > ago(5m)
    | count

  Shows: "Total readings in last 5 min" — useful for confirming data is flowing
```

### 2. KQL Query with Dashboard Parameters

Dashboards support **parameter filters** that apply across all tiles:

```kql
// In your KQL query, reference dashboard parameters with:
// _parameter_name

sensor_readings
| where timestamp between (_start_time .. _end_time)   // Time range filter
| where sensor_id in (_selected_sensors)                // Multi-select filter
| summarize avg_temp = avg(temperature) by sensor_id, bin(timestamp, 1m)
| render timechart

// Parameters are defined in:
//   Dashboard → Edit → Parameters → New Parameter
//   Parameter name: _start_time, Type: DateTime, Default: ago(1h)
//   Parameter name: _end_time,   Type: DateTime, Default: now()
```

---

## 🏛️ Phase 6: Architecture Patterns

### Pattern 1: Lambda Architecture on Fabric

```
Event Source (IoT / App / API)
          │
          ├────────────────────────────────────────────────────────┐
          │ (real-time path — seconds latency)                     │ (batch path — daily)
          ▼                                                         ▼
   Eventstream                                             Data Factory Pipeline
          │                                                         │
          ▼                                                         ▼
   KQL Database                                            Fabric Lakehouse
   (hot queries, alerts)                                   (Bronze → Silver → Gold)
          │                                                         │
          ▼                                                         ▼
   Real-Time Dashboard                                       Power BI Report
   (ops / live monitoring)                                   (business / daily KPIs)
```

### Pattern 2: Eventstream to Lakehouse (Long-Term Storage)

```python
# After real-time events are processed, archive them to the Lakehouse for long-term analytics
# Eventstream handles this automatically with a Lakehouse destination

# The Lakehouse will receive partitioned Delta files:
# SensorLakehouse/Tables/bronze_iot_events/
#   _partition_date=2024-04-20/
#     part-00000.parquet
#     part-00001.parquet

# Then your nightly batch notebook picks these up:
df = spark.read.table("bronze_iot_events") \
    .filter("_partition_date = '2024-04-20'")

# Run aggregations → store in silver/gold for Power BI
```

---

### ✅ Key Takeaways

1. **Eventstream** = Fabric's Kafka. Drag-and-drop real-time ingestion from Event Hubs, IoT Hub, Kafka, or custom apps. Zero broker management.
2. **KQL Database** = ultra-fast time-series store. Queries over billions of rows in milliseconds. Optimized for logs, metrics, and IoT data.
3. **KQL language** = pipe-based syntax (`|`). Learn `where`, `summarize`, `bin()`, `render` — these 4 cover 80% of real-time analytics queries.
4. **Activator** = no-code alerting engine. Define conditions on KQL data → trigger emails, Teams messages, or Fabric Pipelines automatically.
5. **Real-Time Dashboards** = live-refreshing charts (as fast as 30 seconds) from KQL Database. Perfect for ops monitoring screens.
6. **Lambda pattern** — send the same stream to both KQL (real-time ops) and Lakehouse (batch history). Two consumers, one source, zero data duplication.
7. **Hot cache + retention** = key KQL optimization. Keep 7 days "hot" (SSD), store 90 days total. Pay only for what you query frequently.

---

## 🧪 Practice Exercises

### Exercise 1 — KQL Query Fundamentals (Beginner)
**Goal:** Learn the 5 most essential KQL operators by querying a sample dataset.

```
Setup: In Fabric, create a KQL Database "PracticeKQL"
Then, in the KQL Query editor, create and populate a sample table:

.create table web_logs (
    timestamp:   datetime,
    user_id:     string,
    page:        string,
    status_code: int,
    response_ms: int,
    country:     string
)

.ingest inline into table web_logs <|
2024-04-20T10:00:01Z,U001,/home,200,45,India
2024-04-20T10:00:02Z,U002,/products,200,120,USA
2024-04-20T10:00:03Z,U003,/checkout,500,3200,India
2024-04-20T10:00:05Z,U004,/home,200,38,UK
2024-04-20T10:00:08Z,U001,/checkout,200,980,India
2024-04-20T10:00:10Z,U005,/products,404,55,USA
2024-04-20T10:00:15Z,U002,/checkout,500,4100,USA
2024-04-20T10:00:20Z,U006,/home,200,42,India

Now write KQL queries to answer each question:
```

```kql
// Q1: How many requests had an error (status >= 400)?
web_logs
| where status_code >= 400
| count

// Q2: What is the average response time per page?
web_logs
| summarize avg_response_ms = avg(response_ms) by page
| order by avg_response_ms desc

// Q3: Which country had the most requests?
web_logs
| summarize request_count = count() by country
| order by request_count desc
| limit 1

// Q4: Show all errors with their response times, sorted slowest first
web_logs
| where status_code >= 400
| project timestamp, user_id, page, status_code, response_ms
| order by response_ms desc

// Q5: How many requests per minute (group by time bucket)?
web_logs
| summarize requests = count() by bin(timestamp, 1m)
| render timechart

// BONUS: Find users who experienced at least one error
web_logs
| where status_code >= 400
| summarize error_pages = make_set(page) by user_id
| where array_length(error_pages) >= 1
```

---

### Exercise 2 — Simulated Eventstream with Fake Data (Intermediate)
**Goal:** Simulate a real-time event stream using a Fabric Spark Notebook sending events every second.

```python
# In a Fabric Notebook: "simulate_eventstream"
# This simulates IoT sensor data by directly ingesting into a KQL Database

# NOTE: In a real setup you'd use azure-eventhub SDK to send to Event Hubs
# For this exercise, we use the Fabric REST API to ingest directly into KQL

import json
import random
import time
from datetime import datetime, timezone

# Simulate 30 seconds of sensor data (30 batches)
sensor_ids = ["S-001", "S-002", "S-003", "S-042", "S-099"]
base_temps  = {"S-001": 65, "S-002": 70, "S-003": 72, "S-042": 68, "S-099": 80}

print("Starting simulation — generating 30 batches of sensor events...")

for batch in range(30):
    events = []
    for sensor_id in sensor_ids:
        # Normal temperature with occasional spike
        base = base_temps[sensor_id]
        spike = 20 if (batch == 15 and sensor_id == "S-042") else 0  # S-042 spikes at t=15
        temp = base + spike + random.uniform(-2, 2)

        events.append({
            "sensor_id":   sensor_id,
            "temperature": round(temp, 1),
            "humidity":    round(40 + random.uniform(-5, 5), 1),
            "timestamp":   datetime.now(timezone.utc).isoformat()
        })

    # In a real Eventstream setup, send via Event Hub SDK here
    # For demo: print the events that would be sent
    for e in events:
        print(f"[Batch {batch+1:02d}] {e['sensor_id']}: {e['temperature']}°C")

    time.sleep(1)

print("Simulation complete! In real setup these would appear in KQL Database within 1 second.")
# After setup with real Eventstream → check KQL: sensor_readings | where timestamp > ago(1m)
```

```kql
// After running with real Eventstream → query the spike:
sensor_readings
| where timestamp > ago(5m)
| summarize max_temp = max(temperature) by sensor_id, bin(timestamp, 10s)
| where max_temp > 85           // Should highlight S-042 at the spike time
| render timechart
```

---

### Exercise 3 — Design an Activator Alert (Advanced)
**Goal:** Design (and if you have Eventstream set up, implement) a multi-condition alert.

```
Scenario: E-commerce Platform Monitoring
─────────────────────────────────────────────────────────────────────────────
You receive events in a KQL Database "EcommerceKQL", table "order_events":
  {order_id, user_id, status, amount, timestamp}

Business Requirements:
  1. ALERT: If orders per minute drops below 30 (baseline is 200/min)
             → Send email to engineering team
             → Include: current count, timestamp, % drop from baseline

  2. ALERT: If any single order has amount > 50,000 USD
             → Send Teams message to fraud team immediately
             → Include: order_id, amount, user_id

  3. ALERT: If error status ("FAILED") rate exceeds 10% in a 5-minute window
             → Trigger pipeline "rollback_and_notify"

Design document:

For each alert, specify:
  a. KQL query that detects the condition:
     Alert 1: order_events | where timestamp > ago(1m) | count  →  < 30
     Alert 2: order_events | where amount > 50000
     Alert 3: order_events | where timestamp > ago(5m)
               | summarize total=count(), errors=countif(status=="FAILED")
               | where toreal(errors)/total > 0.10

  b. Column to monitor (Alert 1: "Count", Alert 2: "amount", Alert 3: derived)
  c. Cooldown period (Alert 1: 10 min, Alert 2: 0 — every occurrence!, Alert 3: 15 min)
  d. Action (Alert 1: Email, Alert 2: Teams, Alert 3: Pipeline trigger)

Implementation: In Fabric Workspace → New → Activator → configure each rule
```

---

## 🎯 Phase 7: Certification & Interview Drill

### 🛡️ DP-600 (Microsoft Fabric) Drill
*   **Eventstreams:** Can an Eventstream have multiple destinations? 
    *   **Answer:** **Yes**. One Eventstream can send data to KQL, Lakehouse, and a Custom App simultaneously. This is perfect for the Lambda Architecture.
*   **KQL Language:** What is the operator used to filter data in KQL?
    *   **Answer:** `where`. Example: `| where temperature > 30`.

### 🏢 Consultancy Scenario: "The Smart Factory"
**Scenario:** A manufacturing client needs to monitor 10,000 sensors. If a machine vibrates too much, they need an alert in less than 5 seconds.
*   **Architect Answer:** **Eventstream + KQL + Activator.**
*   **The Move:** Route sensor data through **Eventstream** into **KQL**. Use **Activator** to monitor the vibration column. Activator will trigger a Teams alert and an email to the factory floor manager in near real-time (usually < 2 seconds).

### 🚀 Startup Scenario: "The Fraud Detector"
**Scenario:** "We are a fintech startup. We need to detect if a user is logging in from two different countries in 5 minutes. We don't want to hire a 24/7 Ops team."
*   **Answer:** **Activator + KQL.**
*   **The Drill:** Write a KQL query that uses `prev()` or `windowing` functions to find location changes for the same user ID within a 5-minute bucket. Set an **Activator alert** on that query result. The alert can automatically call a **Fabric Pipeline** that disables the user account—stopping fraud without human intervention.

### 🏛️ FAANG Scenario: "The 1M Events Per Second"
**Scenario:** "How do we scale Fabric to handle 1 Million events per second for a global game platform?"
*   **Answer:** **Event Hubs Sharding and KQL Ingestion Batching.**
*   **The Drill:** You don't send 1M events directly to KQL. You use **Azure Event Hubs** with at least 32 partitions (shards). Fabric **Eventstream** will pull from these shards in parallel. In KQL, you fine-tune the **Ingestion Batching Policy** to group small writes into larger chunks (e.g., every 1GB or 5 minutes) to ensure OneLake storage doesn't get overwhelmed by "tiny files."

---

### 🧪 Hands-on Labs
- [kql_iot_lab.md](kql_iot_lab.md) (Step-by-step guide to ingesting simulated IoT data and writing your first 5 KQL queries)

---

### ✅ Key Takeaways
1. **Real-Time Analytics** is about speed (seconds, not hours).
2. **Eventstream** is the "Central Nervous System"—routing data anywhere.
3. **KQL (Kusto)** is the world's fastest log/metric query engine.
4. **Activator** turns data into action (alerts/triggers) without code.
5. **Lambda Architecture** is easy in Fabric: 1 Stream → 2 Destinations (KQL + Lake).
6. **Hot Cache** vs. **Cold Storage** allows you to balance speed vs. cost.

[Next: Lesson 6: Power BI & Semantic Models (The Last Mile) →](../Lesson_6_PowerBI_and_Semantic_Models/README.md)

