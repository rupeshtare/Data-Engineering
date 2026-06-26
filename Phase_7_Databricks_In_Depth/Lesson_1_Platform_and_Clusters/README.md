# Lesson 1: Databricks Platform & Clusters (The Complete Guide)

> **Goal:** Understand the full anatomy of a Databricks workspace, choose the right cluster type for every situation, manage code with Databricks Repos, and know the difference between DBFS and Unity Catalog storage.

---

## 🏗️ Phase 1: Foundations — Understanding the Workspace

### 1. What is a Databricks Workspace?

A **Databricks Workspace** is your personal cloud "workbench" — it lives inside your cloud account (Azure / AWS / GCP) and contains everything:

```
📁 Databricks Workspace
├── 💻 Notebooks          → Python, SQL, Scala, R notebooks
├── 📂 Repos             → Git-connected code (your pipelines, libraries)
├── ⚙️  Compute           → Clusters + SQL Warehouses
├── 📋 Workflows          → Scheduled job definitions
├── 🗃️  Data              → Unity Catalog tables, Delta Lake files
├── 🔬 Experiments        → MLflow ML experiment tracking
├── 🏪 Feature Store      → ML features
├── 🔐 Secrets            → API keys, passwords (Databricks Secret Scopes)
└── ⚙️  Settings          → Access control, user management
```

### 2. The Three Cloud Deployments

| Cloud | Databricks Product Name | Storage Used |
|-------|------------------------|-------------|
| **Azure** | Azure Databricks | Azure Data Lake Storage Gen2 (ADLS) |
| **AWS** | Databricks on AWS | Amazon S3 |
| **GCP** | Databricks on GCP | Google Cloud Storage (GCS) |

> 💡 The Databricks experience is nearly identical across all three clouds. Learn it once, apply it everywhere.

---

## 🚀 Phase 2: Compute — Choosing the Right Cluster

Databricks has **4 types of compute**. Choosing the wrong one wastes money or slows you down.

### 1. All-Purpose Clusters (Interactive Development)

```
USE CASE:
✅ Development / exploration / debugging
✅ Shared by multiple notebooks simultaneously
✅ Long-running (hours)
✅ Run any language (Python, SQL, Scala, R)

WHEN NOT TO USE:
❌ Production scheduled jobs (use Job Clusters instead — cheaper!)
❌ BI SQL queries (use SQL Warehouses instead — optimized for SQL)

COST: Runs all the time you have it on → set auto-termination!
```

```python
# Best practices for All-Purpose Clusters:

# 1. ALWAYS set auto-termination (in cluster config UI):
#    Auto terminate after: 30 minutes of inactivity

# 2. Use cluster-scoped init scripts for shared libraries
# (In cluster config → Advanced → Init Scripts)
# /dbfs/init_scripts/install_libs.sh:
#!/bin/bash
pip install great-expectations==0.18.0 pydantic==2.5.0

# 3. Use environment variables for secrets (not hardcoded!)
import os
db_password = os.environ.get("DB_PASSWORD")  # Set via Databricks Secrets scope
```

### 2. Job Clusters (Automated Pipeline Runs)

```
USE CASE:
✅ Production scheduled Workflows
✅ Auto-starts when the job runs, auto-terminates when done
✅ Isolated — not shared with anyone else
✅ Cheapest for production jobs

COST: Pay only for the runtime of the job (sometimes 70% cheaper than all-purpose!)
```

```json
// In Databricks Workflow → Task → Compute → "Create new job cluster"
{
  "spark_version": "13.3.x-scala2.12",
  "node_type_id": "Standard_DS4_v2",
  "num_workers": 4,
  "autoscale": {
    "min_workers": 2,
    "max_workers": 8         
  },
  "aws_attributes": {
    "availability": "SPOT_WITH_FALLBACK"  // Use Spot instances (70% cheaper!)
  }
}
```

### 3. SQL Warehouses (For SQL / BI Queries)

```
USE CASE:
✅ Running SQL queries (Databricks SQL editor)
✅ Powering BI tools (PowerBI, Tableau, Looker connecting via JDBC/ODBC)
✅ Analyst-facing dashboards and alerts
✅ Optimized with Photon (C++ vectorized query engine — NOT just Spark)

TYPES:
• Serverless SQL Warehouse → Fastest startup (2-3 seconds vs 5-10 min) — RECOMMENDED
• Pro SQL Warehouse        → Classic, can use Photon, persisted cluster
• Classic SQL Warehouse    → Legacy, basic performance
```

```sql
-- Connect to a SQL Warehouse from BI tools:
-- JDBC URL: jdbc:spark://<workspace>.azuredatabricks.net:443/default;...
-- HTTP Path: /sql/1.0/warehouses/<warehouse-id>

-- Run queries designed for SQL Warehouses (Photon optimized):
SELECT
    DATE_TRUNC('month', order_date) AS month,
    region,
    SUM(amount)                     AS revenue,
    COUNT(DISTINCT customer_id)     AS unique_buyers
FROM gold.fact_sales
WHERE order_date >= '2024-01-01'
GROUP BY ALL   -- "GROUP BY ALL" is a Databricks SQL shorthand!
ORDER BY month, revenue DESC;
```

### 4. Instance Pools — Pre-Starting VMs to Reduce Startup Time

```
PROBLEM: Cluster startup takes 5-8 minutes (VM provisioning from cloud)
SOLUTION: Instance Pools pre-warm VMs so clusters start in <30 seconds!

USE CASE:
✅ Teams with frequent short cluster needs
✅ Reducing wait time for morning jobs
✅ Shared pool across multiple teams

COST: You pay for idle VMs in the pool → only use if the team actually needs it
```

### 5. Cluster Configuration Best Practices

```python
# Choosing the right VM type:
# ┌──────────────────┬──────────────────────────────────────────────────────┐
# │ VM Type           │ Best For                                             │
# ├──────────────────┼──────────────────────────────────────────────────────┤
# │ Memory Optimized  │ Large caching, wide DataFrames, MLlib               │
# │ (e.g. DS14_v2)   │                                                      │
# ├──────────────────┼──────────────────────────────────────────────────────┤
# │ Compute Optimized │ CPU-heavy: ML training, complex aggregations        │
# │ (e.g. F16s_v2)   │                                                      │
# ├──────────────────┼──────────────────────────────────────────────────────┤
# │ Storage Optimized │ Very large shuffles, Delta OPTIMIZE, VACUUM         │
# │ (e.g. L8s_v3)    │                                                      │
# ├──────────────────┼──────────────────────────────────────────────────────┤
# │ GPU               │ Deep Learning, LLM fine-tuning, TensorFlow/PyTorch │
# │ (e.g. NC12s_v3)  │                                                      │
# └──────────────────┴──────────────────────────────────────────────────────┘

# Autoscaling configuration (for variable workloads):
# min_workers: 2  (never go below 2 — avoids cold start)
# max_workers: 20 (cap spend)
# scale-up: aggressive (add workers fast when queue builds up)
# scale-down: conservative (keep workers for 2 min after idle — avoid thrashing)
```

---

## 🏛️ Phase 3: Architect Level — Repos, Secrets, and DBFS

### 1. Databricks Repos — Git Integration

Repos connects Databricks directly to your GitHub/GitLab/Azure DevOps repository. This is how professional teams work — **no more emailing notebooks around**.

```bash
# In Databricks UI: Workspace → Repos → Add Repo → Paste your GitHub URL

# Your repo structure (industry standard):
📁 my_lakehouse_pipeline/
├── notebooks/
│   ├── 01_bronze_ingest.py
│   ├── 02_silver_transform.py
│   └── 03_gold_aggregate.py
├── src/                         # Reusable Python modules (importable!)
│   ├── __init__.py
│   ├── validators.py
│   └── transformers.py
├── tests/
│   ├── test_validators.py
│   └── test_transformers.py
├── pipelines/                   # DLT pipeline definitions
│   └── sales_pipeline.py
├── terraform/                   # Infrastructure as code
│   └── main.tf
├── requirements.txt
└── .github/
    └── workflows/
        └── ci.yml               # Auto-test on every PR
```

```python
# Import from your src/ folder (Repos makes this possible!):
import sys
sys.path.append('/Workspace/Repos/apple/my_lakehouse_pipeline/src')

from validators import validate_orders
from transformers import clean_customer_records

# Or use %pip install for external packages:
# %pip install pydantic==2.5.0 great-expectations==0.18.0
```

### 2. Databricks Secrets — Secure Credential Management

**NEVER store passwords or API keys in notebooks.** Use Databricks Secret Scopes.

```bash
# Step 1: Create a Secret Scope using Databricks CLI
databricks secrets create-scope --scope "prod-credentials"

# Step 2: Add secrets to the scope
databricks secrets put --scope "prod-credentials" --key "db_password"
      # (prompts for value — never echoed to terminal)

databricks secrets put --scope "prod-credentials" --key "kafka_api_key"
databricks secrets put --scope "prod-credentials" --key "stripe_secret_key"
```

```python
# Step 3: Use secrets in notebooks/code — value is NEVER visible in logs or UI!
db_password = dbutils.secrets.get(scope="prod-credentials", key="db_password")

# Use the password to connect:
jdbc_url = f"jdbc:postgresql://prod-db.internal.com:5432/warehouse"
df = spark.read \
    .format("jdbc") \
    .option("url", jdbc_url) \
    .option("dbtable", "orders") \
    .option("user", "pipeline_svc") \
    .option("password", db_password)  # ← Value is masked in Spark UI! \
    .load()

# Pro Tip: You can also link a Databricks Scope to Azure Key Vault
# → All your organization's secrets centralized in Key Vault
# → Databricks just reads from it (no duplication!)
databricks secrets create-scope \
    --scope "keyvault-scope" \
    --scope-backend-type AZURE_KEYVAULT \
    --resource-id "/subscriptions/.../vaults/my-key-vault" \
    --dns-name "https://my-key-vault.vault.azure.net/"
```

### 3. DBFS vs Unity Catalog — Understanding Storage

```
DBFS (Databricks File System):
├── Legacy storage layer — think of it like a virtual file system
├── Paths like: dbfs:/mnt/mydata/file.parquet OR /dbfs/mnt/mydata/file.parquet
├── Files are stored in the cloud bucket behind the scenes
├── NOT governed by Unity Catalog (no access control, no lineage)
├── ⚠️ Deprecated for new projects — use Unity Catalog Volumes instead!

Unity Catalog Volumes (Modern):
├── Strongly typed file storage with access control
├── Paths like: /Volumes/catalog/schema/volume_name/file.parquet
├── Full RBAC (who can read/write each folder)
├── Lineage tracking (Unity Catalog knows which job wrote this file)
└── ✅ Use this for ALL new projects!
```

```sql
-- Create a Unity Catalog Volume (replaces DBFS mounts):
CREATE VOLUME prod.landing.raw_files
COMMENT 'Landing zone for partner raw file uploads';

-- Access from Python:
-- spark.read.csv('/Volumes/prod/landing/raw_files/orders_2024.csv')

-- Access from terminal/Repos:
-- dbutils.fs.ls('/Volumes/prod/landing/raw_files/')
```

### 4. Cluster Policies — Governance for Cost Control

Cluster Policies let administrators enforce rules on what clusters users can create:

```json
// In Databricks Admin Console → Cluster Policies → Create Policy
// Example: "Data Engineering Standard Policy"
{
  "spark_version": {
    "type": "allowlist",
    "values": ["13.3.x-scala2.12", "14.3.x-scala2.12"],
    "defaultValue": "13.3.x-scala2.12"
  },
  "node_type_id": {
    "type": "allowlist",
    "values": ["Standard_DS3_v2", "Standard_DS4_v2"],
    "defaultValue": "Standard_DS3_v2"
  },
  "autotermination_minutes": {
    "type": "range",
    "minValue": 10,
    "maxValue": 60,
    "defaultValue": 30
  },
  "num_workers": {
    "type": "range",
    "maxValue": 10
  },
  "custom_tags.team": {
    "type": "fixed",
    "value": "data-engineering"    // Cost attribution tag — always set!
  }
}
// Now even if a user tries to create a 50-worker cluster, the policy blocks them!
```

---

### 6. Liquid Clustering — The Self-Tuning Storage
**Liquid Clustering** is the successor to Z-Ordering and Partitioning. Instead of you deciding how to organize folders, Databricks does it automatically based on how you query the data.
*   **The Problem:** Traditional partitioning by `date` is great until you need to query by `customer_id`. You can't have both easily.
*   **The Fix:** `CLUSTER BY (customer_id, order_date)`. Databricks manages the internal layout.
*   **Pros:** Incremental (doesn't rewrite the whole table), self-tuning, and no "partition skew" problems.

---

## 🎯 Phase 4: Certification & Interview Drill

### 🛡️ Databricks Data Engineer Professional Drill
*   **Photon vs. Spark:** Photon is a **vectorized execution engine** written in C++. Spark is JVM-based. Photon speeds up nearly all queries but is primarily effective for **wide transformations** (joins, aggregations) on Delta/Parquet.
*   **Serverless SQL startup:** Serverless SQL Warehouses start in <10 seconds. In the exam, if the question asks for "Instant BI access with no management," the answer is **Serverless**.

### 🛡️ DP-600 (Microsoft Fabric) Drill
*   **Fabric Capacity vs. Databricks DBUs:** In Fabric, you buy a fixed amount of "Capacity" (Memory/CPU). In Databricks, you pay as you go per "DBU" (Databricks Unit). 
*   **The Drill:** Know that Databricks DBUs represent a combined cost of the software license, whereas the cloud VM cost is billed separately by AWS/Azure (unless using Serverless).

### 🏢 Consultancy Scenario: "The Cost Audit"
**Scenario:** A client says, "We spent $100,000 on Databricks last month, and 50% of it was for clusters that were idle."
*   **Architect Answer:** **Enforce Cluster Policies and Auto-termination.**
*   **The Move:** Apply a global policy that forces `autotermination_minutes` to a maximum of 15 minutes. Also, migrate production jobs to **Job Clusters** which terminate instantly once the notebook finishes.

### 🚀 Startup Scenario: "The 1-Worker Startup"
**Scenario:** "We only have $500 total for our data stack this month. How do we run Databricks?"
*   **Answer:** **Single Node Clusters.** 
*   **The Drill:** For small datasets, you don't need a cluster. Use a "Single Node" cluster (0 workers, Driver only). It's much cheaper and handles <10GB data perfectly fine.

### 🏛️ FAANG Scenario: "The Concurrent BI Storm"
**Scenario:** "It's 9 AM on Monday. 500 managers just opened their PowerBI dashboards at the same time. The SQL Warehouse is showing a massive queue. What do you do?"
*   **Answer:** **SQL Warehouse Auto-Scaling.**
*   **The Drill:** Ensure the SQL Warehouse has `Min Clusters: 1` and `Max Clusters: 10`. Databricks will automatically spin up "Cluster 2, 3, 4..." to handle the load and shut them down when the managers go to lunch.

---

### 🧪 Hands-on Labs
- [cluster_config_lab.py](cluster_config_lab.py) (Simulating different cluster configs and measuring performance)

---

### ✅ Key Takeaways
1. **Photon** is your C++ secret weapon for speed.
2. **Serverless** removes the "Plumbing" and lets you focus on SQL.
3. **Liquid Clustering** makes your tables "self-healing" and fast regardless of how you query.
4. **Job Clusters** are non-negotiable for production (70% savings).
5. **Cluster Policies** are the only way to manage cost at scale in a large company.
6. **Git Repos** are the only way to manage code professionally on the platform.

[Next: Lesson 2: Delta Live Tables (DLT Masterclass) →](../Lesson_2_Delta_Live_Tables/README.md)

---

## 🧪 Practice Exercises

### Exercise 1 — Cluster Explorer (Beginner)
**Goal:** Create your first cluster and use `dbutils` to inspect the environment.

```python
# 1. Create an All-Purpose Cluster (1 node, F-series or D-series)
# 2. Open a new Notebook and attach it to the cluster
# 3. Run these foundational commands:

# List the root of DBFS
files = dbutils.fs.ls("/")
display(files)

# Create a dummy folder and file
dbutils.fs.mkdirs("/tmp/learning_databricks/")
dbutils.fs.put("/tmp/learning_databricks/test.txt", "Hello Databricks!", True)

# List the folder to verify
display(dbutils.fs.ls("/tmp/learning_databricks/"))

# Read the file back
content = dbutils.fs.head("/tmp/learning_databricks/test.txt")
print(f"File content: {content}")

# 4. Clean up
dbutils.fs.rm("/tmp/learning_databricks/", True)
```

---

### Exercise 2 — Secret Scopes & Secure Access (Intermediate)
**Goal:** Practice creating a secret scope and using it to hide credentials.

```bash
# 1. Install Databricks CLI on your machine: pip install databricks-cli
# 2. Configure CLI: databricks configure --token
# 3. Create a scope:
databricks secrets create-scope --scope "academy-lab"

# 4. Add a dummy password:
databricks secrets put --scope "academy-lab" --key "mysql-password"
# (Enter 'Admin123!' when prompted)
```

```python
# 5. In your Notebook, retrieve and "try" to see the secret
pass = dbutils.secrets.get(scope="academy-lab", key="mysql-password")

print(f"The password is: {pass}") 
# EXPECTED OUTCOME: You see [REDACTED]. Databricks never prints secrets to the UI!

# 6. Use it in a connection (simulated)
print(f"Connecting to MySQL with password: {pass[:2]}...") # Still redacted!
```

---

### Exercise 3 — Cost Optimization Analysis (Architect)
**Goal:** Compare the cost of All-Purpose vs. Job Clusters for a production run.

**Scenario:**
- You have a notebook that runs for 60 minutes.
- All-Purpose Cluster Cost: 1.5 DBUs per hour.
- Job Cluster Cost: 0.4 DBUs per hour.
- Cloud VM Cost: $0.50 per hour.

**Calculate:**
1. Cost of running this as an "All-Purpose" interactive task.
2. Cost of running this as a "Scheduled Job" task.
3. If you run this 30 times a month, how much do you save by switching to Job Clusters?

**Answer:**
- All-Purpose: (1.5 DBU * $0.40/DBU) + $0.50 = $1.10 per run.
- Job: (0.4 DBU * $0.40/DBU) + $0.50 = $0.66 per run.
- Monthly Savings: (1.10 - 0.66) * 30 = **$13.20/month per job.**
- *Architect Note:* Scale this to 1,000 jobs and you save **$13,200 per month**.

---

## 💼 Common Interview Questions

**Q1: What is the difference between an All-Purpose Cluster and a Job Cluster?**
> **All-Purpose Clusters** are for interactive development; they are shared, stay on until manually stopped (or auto-terminated), and have a higher DBU cost. **Job Clusters** are for automated tasks; they are created specifically for one job, terminated immediately after, and have a much lower DBU cost (~3x cheaper). Always use Job Clusters for production.

**Q2: How does Photon improve Spark performance?**
> **Photon** is a vectorized execution engine written in C++. It bypasses the JVM for the most expensive part of a query (the execution plan). It is significantly faster for "wide" operations like joins, aggregations, and string processing. It's built into Databricks SQL Warehouses and can be enabled on standard clusters.

**Q3: What are Init Scripts and why are they risky?**
> **Init Scripts** are shell scripts that run during cluster startup (to install libraries or configure OS settings). They are risky because if a script fails (e.g., a `pip install` fails due to a network error), the entire cluster fails to start. For libraries, it is better to use **Cluster-Scoped Libraries** or **Docker Containers**.

**Q4: Explain the difference between DBFS and Unity Catalog Volumes.**
> **DBFS** is the legacy storage layer with no security—anyone with cluster access can read any file in DBFS. **Unity Catalog Volumes** are the modern, governed alternative. They provide **RBAC (Role-Based Access Control)**, meaning you can grant `READ_VOLUME` to specific users only. Volumes also support lineage and auditing.

**Q5: How would you handle a "Hot Partition" (Skew) in a Databricks cluster?**
> Data Skew happens when one worker gets 90% of the data while others are idle. Fixed by: (1) Using **Salting** (adding a random suffix to the join key to redistribute the data). (2) Enabling **AQE (Adaptive Query Execution)** which can detect and fix skew automatically during the join. (3) Checking if a **Broadcast Join** can be used instead of a Shuffle Join.
