# Lesson 2: Data Factory Pipelines in Fabric

> **Goal:** Ingest data from any source into your Fabric Lakehouse using Data Factory's Copy Activity and Dataflows Gen2, and orchestrate complex multi-step pipelines with branching, looping, and error handling.

---

## 🏗️ Phase 1: Foundations — Data Factory in Fabric

### 1. Two Ways to Move Data in Fabric Data Factory

| Tool | What It Is | Best For |
|------|-----------|---------|
| **Copy Activity** | A single high-throughput data copy step | Moving data from source to Lakehouse (Bronze ingestion) |
| **Dataflows Gen2** | A Power Query / M-code based visual transform tool | Low-code data transformations (no-code ETL for analysts) |
| **Notebooks** | Full Spark Python/SQL | Complex transformations (Silver, Gold layers) |
| **Pipeline** | An orchestrator that sequences the above | Connecting Copy → Notebook → Dataflow in order |

### 2. The Pipeline Building Blocks

```
Fabric Pipeline = A visual workflow of Activities:

START
  ↓
[Copy Activity: S3 → Lakehouse Files]         ← Ingest raw CSV
  ↓
[Notebook Activity: Bronze → Silver]          ← Spark transformation
  ↓
[If Condition: row_count > 1000?]
  │── YES → [Notebook: Silver → Gold]         ← Run full aggregation
  └── NO  → [Send Alert: "Low row count!"]    ← Notify team
  ↓
END
```

---

## 🚀 Phase 2: Copy Activity — The Data Ingestion Workhorse

### 1. Supported Connectors (200+!)

```
Sources (reads from):            Destinations (writes to):
──────────────────────           ──────────────────────────
• Azure SQL Database             • Fabric Lakehouse Files
• Azure Synapse Analytics        • Fabric Lakehouse Tables
• Amazon S3                      • Fabric Data Warehouse
• Google BigQuery                • Azure Data Lake Gen2
• Snowflake                      • Azure Blob Storage
• SAP HANA / SAP BW              • Amazon S3
• Oracle / SQL Server / MySQL    (Generally: anywhere you can read from,
• PostgreSQL                      you can also write to)
• REST APIs (HTTP)
• FTP / SFTP
• SharePoint Online
• Salesforce / Dynamics 365
• MongoDB / Cosmos DB
• Kafka (via Eventstream)
• Files: CSV, JSON, Parquet, Avro, ORC, Excel, XML
```

### 2. Configuring a Copy Activity

```json
// A fully configured Copy Activity (shown as JSON, but configured visually in UI)
{
  "name": "CopyFromS3ToLakehouse",
  "type": "Copy",
  "inputs": [{
    "type": "AmazonS3",
    "linkedService": "AmazonS3Connection",
    "dataset": {
      "type": "Parquet",
      "location": {
        "type": "AmazonS3Location",
        "bucketName": "company-data-lake",
        "folderPath": "orders/@{formatDateTime(pipeline().parameters.RunDate, 'yyyy/MM/dd')}/"
      }
    }
  }],
  "outputs": [{
    "type": "LakehouseTable",
    "lakehouse": "SalesLakehouse",
    "tableType": "Delta",
    "rootFolder": "Files",
    "tablePath": "bronze/raw_orders"
  }],
  "settings": {
    "copyBehavior": "MergeFiles",        // Combine all Parquet files into one in the destination
    "parallelCopies": 8,                 // 8 parallel copy threads (for large datasets)
    "enableStaging": true,               // Staged copy (better for large data)
    "enableSkipIncompatibleRow": true,   // Skip bad rows instead of failing
    "logSettings": {
      "enableCopyActivityLog": true,     // Log skipped rows for debugging
      "logLevel": "Warning"
    }
  }
}
```

### 3. Incremental Copy Pattern — Only Copy New Data

```json
// The best pattern for daily ingestion: only copy rows that are NEW since last run
// Uses a "watermark" — the max timestamp from the last successful run

// Pipeline Variables:
// - @pipeline().parameters.LastRunTimestamp  → passed in from Workflow
// - @pipeline().parameters.CurrentTimestamp  → current run time

{
  "name": "IncrementalCopyOrders",
  "type": "Copy",
  "query": "SELECT * FROM orders WHERE updated_at > '@{pipeline().parameters.LastRunTimestamp}' AND updated_at <= '@{pipeline().parameters.CurrentTimestamp}'",
  // ⬆️ Only copies rows updated between last run and now!
  "copyBehavior": "AppendDynamicFolder"  // Appends to a date-partitioned folder
}

// After the copy, update the watermark in a Lookup activity:
// Store new watermark: @pipeline().parameters.CurrentTimestamp
// Next run reads it from: Azure Key Vault or a Fabric Warehouse "control" table
```

---

## 🚀 Dataflows Gen2 — Low-Code Transformations

### 1. What is Dataflows Gen2?

**Dataflows Gen2** is a **visual, drag-and-drop data transformation tool** powered by Power Query. No code needed — business analysts and data engineers can both use it.

Think: **Excel's Power Query, but at cloud scale and writing to a Lakehouse.**

```
Dataflows Gen2 = Power Query Online + Fabric Scale

Steps:
1. Connect to source (SQL Server, CSV, SharePoint, API...)
2. Transform with built-in steps (filter, group, pivot, merge, clean)
3. Output to Fabric Lakehouse table or Data Warehouse table

Behind the scenes: Fabric converts Power Query steps into Spark SQL or Dataflow Gen2 engine.
```

### 2. Common Power Query Transformations in Dataflows Gen2

```
Available transformations (all no-code in the visual editor):

Data Cleaning:
  • Remove empty rows
  • Fill down null values
  • Trim whitespace from text columns
  • Replace values (e.g., replace "N/A" with null)
  • Remove duplicates by column

Type Casting:
  • Change column type (text → number, text → date)
  • Parse dates in custom formats (dd/MM/yyyy → date type)
  • Split column by delimiter ("Priya,Sharma" → First Name + Last Name)

Joining Data:
  • Merge queries (JOIN two tables on a key)
  • Append queries (UNION ALL of two tables)
  • Expand related table (similar to a left join + expand)

Aggregation:
  • Group By (SUM, COUNT, AVG, MIN, MAX)
  • Pivot / Unpivot columns
  • Rolling totals

Advanced (via M-code formula bar):
  = Table.SelectRows(Source, each [Amount] > 0 and [Status] = "COMPLETED")
  = Table.AddColumn(prev, "MonthName", each Date.MonthName([OrderDate]))
```

---

## 🏛️ Phase 3: Pipeline Orchestration — The Full Pattern

### 1. Complete ETL Pipeline with Error Handling

```
Pipeline: "Daily Sales Ingestion"
─────────────────────────────────────────────────────────────────
Activities:
┌─────────────────────────────────────────────────────────────┐
│ 1. Get Watermark (Lookup)                                    │
│    → SELECT MAX(ingested_at) FROM control.pipeline_state    │
│    → Stores result in @activity('GetWatermark').output.value│
└────────────────────────┬────────────────────────────────────┘
                         ↓ (on success)
┌────────────────────────────────────────────────────────────┐
│ 2. Copy Raw Orders (Copy Activity)                          │
│    → FROM: SQL Server orders WHERE updated_at > @watermark │
│    → TO: SalesLakehouse/Files/bronze/raw_orders/           │
└────────────────────────┬───────────────────────────────────┘
                ↙ success  ↘ failure
┌──────────────────┐    ┌──────────────────────────────────┐
│ 3a. Transform    │    │ 3b. Send Failure Email            │
│     Notebook     │    │     To: data-team@company.com    │
│     (Silver)     │    │     Body: @activity error message │
└────────┬─────────┘    └──────────────────────────────────┘
         ↓ success
┌─────────────────────────────────┐
│ 4. Aggregate Notebook (Gold)    │
└────────────────────┬────────────┘
                     ↓ success
┌───────────────────────────────────────────────────────────┐
│ 5. Update Watermark (Script Activity)                      │
│    UPDATE control.pipeline_state SET last_run = getdate() │
└───────────────────────────────────────────────────────────┘
```

### 2. Loop Activity — Process Multiple Tables

```json
// ForEach loop: Run the same pipeline for multiple tables
{
  "name": "ForEach Table",
  "type": "ForEach",
  "items": {
    "value": "@json('[\"orders\",\"customers\",\"products\",\"inventory\"]')"
    // Or dynamically: "@activity('GetTableList').output.value"
  },
  "isSequential": false,     // Run all iterations in PARALLEL!
  "batchCount": 4,            // But max 4 at a time (don't overload source)
  "activities": [
    {
      "name": "Copy Single Table",
      "type": "Copy",
      "source": {
        "query": "@concat('SELECT * FROM ', item(), ' WHERE updated_at > ''2024-01-01''')"
      },
      "destination": {
        "path": "@concat('bronze/', item())"
      }
    }
  ]
}
```

### 3. Pipeline Triggers

```
Three trigger types in Fabric Data Factory:

1. ⏰ Scheduled Trigger:
   → Run daily at 2:00 AM UTC
   → CRON expression: 0 2 * * *
   → Can pass parameters: {RunDate: @formatDateTime(trigger().scheduledTime, 'yyyy-MM-dd')}

2. 📁 Storage Event Trigger:
   → When a new file arrives in a Lakehouse Files folder OR ADLS container
   → Parameters: {FileName: @triggerBody().fileName, FilePath: @triggerBody().folderPath}
   → Great for: Partner data feeds, API file drops

3. 🔗 Manual / API Trigger:
   → Run via Fabric UI button
   → Run via REST API (for CI/CD integration)
   → Run from another pipeline (Pipeline Invoke Activity)
```

---

### 4. Web Activity & API Integration
**Web Activity** allows you to make HTTP requests (GET, POST, etc.) to external services.
*   **The Move:** Use it to trigger an external REST API (e.g., refreshing a Snowflake table or sending a Slack notification via webhook).
*   **Architect Note:** In Fabric, you can now use the **Service Principal** of the workspace to authenticate these web calls automatically.

---

## 🎯 Phase 4: Certification & Interview Drill

### 🛡️ DP-600 (Microsoft Fabric) Drill
*   **Dataflows Gen2 vs. Spark:** When should you use Dataflows Gen2? 
    *   **Answer:** Use Dataflows for **low-code** transformations and when the data volume is small to medium. For large-scale joins or complex logic, **Fabric Spark Notebooks** are the standard.
*   **Pipeline Failures:** If a pipeline fails, where do you find the error?
    *   **Answer:** **Monitoring Hub** or the **Output** tab of the Pipeline run. You should know how to parse the JSON error message to find the "errorCode".

### 🏢 Consultancy Scenario: "The 500 SSIS Packages"
**Scenario:** A client has 500 legacy SSIS packages. They want to move to Fabric.
*   **Architect Answer:** **Evaluate before Lift-and-Shift.**
*   **The Move:** Don't try to run SSIS in Fabric (it's not natively supported). Instead, analyze the packages. 80% are likely simple "Table Move" jobs—replace these with **Copy Activities**. 10% are transformations—replace with **Dataflows Gen2**. The remaining 10% are complex—use **Spark Notebooks**. This "modernization" is better than a literal "migration."

### 🚀 Startup Scenario: "Cost-Efficient Ingestion"
**Scenario:** Your startup is burning CUs too fast. Every time you run a 2-minute notebook, Fabric bills you for the "Startup time" of the Spark cluster.
*   **Answer:** **Script Activity over Notebooks.**
*   **The Drill:** If all you are doing is a simple `UPDATE` or `DELETE` on a SQL Warehouse table, don't use a Notebook. Use a **Script Activity**. It runs instantly on the SQL Warehouse and consumes significantly fewer CUs than spinning up a whole Spark session.

### 🏛️ FAANG Scenario: "The 1,000 Parallel Ingests"
**Scenario:** "We have 1,000 tables to ingest every hour. Our Source system (Oracle) crashes if we try to read more than 10 at a time. How do you design the pipeline?"
*   **Answer:** **ForEach + BatchCount.**
*   **The Drill:** Use a **ForEach** activity with `isSequential: false` (to run in parallel) but set the `batchCount: 10`. This ensures you are always processing 10 tables at a time—maximizing your throughput without killing the source database.

---

### 🧪 Hands-on Labs
- [pipeline_automation_lab.md](pipeline_automation_lab.md) (Build a pipeline that reads from an API, saves to Lakehouse, and sends a Slack alert on failure)

---

### ✅ Key Takeaways
1. **Pipelines** are your orchestrators (the "Managers").
2. **Copy Activity** is your heavy lifter (the "Movers").
3. **Dataflows Gen2** is your visual editor (the "No-Code" way).
4. **Watermarks** are essential for incremental data flow.
5. **Parallelism** is a double-edged sword—use `batchCount` to protect your source.
6. **Error handling** (on-failure paths) is the difference between an amateur and a pro pipeline.

[Next: Lesson 3: Lakehouse & Data Warehouse (The Storage Choice) →](../Lesson_3_Lakehouse_and_Warehouse/README.md)

---

## 🧪 Practice Exercises

### Exercise 1 — Build Your First Copy Pipeline (Beginner)
**Goal:** Ingest a CSV file from a public URL into your Fabric Lakehouse.

```
Steps:
1. In your "MyFabricLearning" workspace → New → Data Pipeline
   Name: "Exercise_CopyCSV"

2. Add a Copy Activity:
   Source:
   • Connector: HTTP
   • URL: https://raw.githubusercontent.com/datasciencedojo/datasets/master/titanic.csv
   • Method: GET
   • File format: DelimitedText (CSV), first row = header

   Destination:
   • Connector: Fabric Lakehouse
   • Lakehouse: "LearningLakehouse"
   • Root folder: Files
   • File path: landing/titanic.csv
   • File format: DelimitedText (CSV)

3. Click "Run" → verify the file appears in Lakehouse → Files → landing → titanic.csv

4. Extension: Add a Notebook Activity AFTER the Copy Activity
   • Notebook reads the CSV and writes it as a Delta table:
     df = spark.read.option("header","true").csv("Files/landing/titanic.csv")
     df.write.format("delta").saveAsTable("bronze_titanic")
```

**Expected outcome:** A working 2-step pipeline: HTTP CSV → Lakehouse Files → Delta Table.

---

### Exercise 2 — Implement the Watermark Pattern (Intermediate)
**Goal:** Build an incremental copy pipeline that only processes new records.

```
Scenario: A SQL Server table "orders" has an "updated_at" timestamp column.
You want to copy only rows updated since the last pipeline run.

Design the pipeline with these activities:

Activity 1: Lookup — "GetLastWatermark"
  Query: SELECT MAX(last_watermark) FROM control.pipeline_state
          WHERE pipeline_name = 'orders_copy'
  Output variable: @activity('GetLastWatermark').output.firstRow.last_watermark

Activity 2: Copy Activity — "CopyNewOrders"
  Source: SQL Server
  Query: SELECT * FROM orders
         WHERE updated_at > '@{activity('GetLastWatermark').output.firstRow.last_watermark}'
         AND updated_at <= '@{utcNow()}'
  Destination: LearningLakehouse/Files/bronze/orders/

Activity 3: Script Activity — "UpdateWatermark"
  On success of Activity 2:
  SQL: UPDATE control.pipeline_state
       SET last_watermark = '@{utcNow()}'
       WHERE pipeline_name = 'orders_copy'

Draw this pipeline on paper first, then try to build it in Fabric.

Questions to answer:
  → What happens if the pipeline fails at Activity 2?
     (Answer: watermark is NOT updated → next run re-processes the same window → safe!)
  → What happens if you run Activity 2 twice with the same window?
     (Answer: depends on write mode — use "append" + dedup in Silver, not "overwrite" in Bronze)
  → How would you store the watermark in Fabric instead of SQL Server?
     (Hint: Use a Warehouse control table, or a Lakehouse Delta table)
```

---

### Exercise 3 — Parallel ForEach Pipeline (Advanced)
**Goal:** Process 4 CSV files simultaneously using a ForEach loop.

```python
# First, upload 4 CSV files to Lakehouse → Files → landing/:
# products.csv, customers.csv, orders.csv, inventory.csv
# (Use any sample CSVs — even copy the titanic.csv 4 times with different names for practice)

# Build this pipeline:

# Activity 1: Set Variable — "TableList"
#   Variable name: table_names
#   Value: ["products","customers","orders","inventory"]

# Activity 2: ForEach — "ProcessEachTable"
#   Items: @variables('table_names')
#   Is Sequential: false
#   Batch count: 4
#
#   Inside ForEach → add a Notebook Activity:
#     Parameters:
#       table_name = @item()
#     Notebook code (parameter cell):
#       table_name = "products"  # default, overridden by pipeline
#
#       df = spark.read.option("header","true") \
#                .csv(f"Files/landing/{table_name}.csv")
#       df.write.format("delta").mode("overwrite") \
#              .saveAsTable(f"bronze_{table_name}")
#       print(f"Done: bronze_{table_name}")

# Expected result after pipeline run:
# LearningLakehouse/Tables/
#   bronze_products/
#   bronze_customers/
#   bronze_orders/
#   bronze_inventory/
```

**Verify:** Check LearningLakehouse → Tables — all 4 Delta tables should appear.

---

## 💼 Common Interview Questions

**Q1: What is the difference between Copy Activity and Dataflows Gen2? When would you use each?**
> **Copy Activity** is a high-throughput binary/structured data mover (200+ connectors). It does minimal transformation — copy rows from A to B, apply column mapping, maybe filter. It's the fastest way to land raw data into Bronze. **Dataflows Gen2** is a Power Query-based visual ETL tool for data *transformation* (filter, join, pivot, clean types). Use Copy for Bronze ingestion, use Dataflows for analyst-driven Silver transformations where team members prefer a no-code UI over Python notebooks.

**Q2: Explain the incremental copy / watermark pattern.**
> The watermark pattern avoids re-processing all historical data on every run. Before each run, look up the "last successful run" timestamp (the watermark) from a control table. Use it to filter the source: `WHERE updated_at > last_watermark`. After a successful copy, update the watermark to the current time. If the pipeline fails mid-run, the watermark is NOT updated — so the next run reprocesses the same window safely (idempotent).

**Q3: What are the three trigger types in Fabric Data Factory and when do you use each?**
> (1) **Scheduled** — run on a CRON schedule (e.g., 2 AM daily). Best for batch ETL. (2) **Storage Event** — fires when a file arrives in a Lakehouse folder or ADLS container. Best for partner file drops or API exports that land unpredictably. (3) **Manual/API** — triggered from the UI or a REST API call. Best for on-demand runs or triggering from an external CI/CD system (e.g., Azure DevOps pipeline calls Fabric pipeline after a deployment).

**Q4: How does a ForEach loop work and what does `isSequential: false` do?**
> ForEach iterates over an array of items (e.g., table names) and runs the same inner activities for each item. `isSequential: false` runs all iterations in **parallel** (up to the `batchCount` limit). This dramatically speeds up bulk ingestion — e.g., copying 20 tables simultaneously instead of one by one. Use `batchCount` to throttle parallelism and avoid overwhelming the source system.

**Q5: A Copy Activity fails halfway through. What happens to the data already written?**
> It depends on the write mode. If writing to **Lakehouse Files** (raw files), partial files may be written — the next run should overwrite or use append with dedup. If writing to a **Delta table**, the Copy Activity in Fabric wraps the write in a Delta transaction — on failure, the partial write is rolled back (ACID). Always design your Bronze layer for safe re-runs: use append + downstream dedup (MERGE in Silver) rather than relying on a single overwrite.

