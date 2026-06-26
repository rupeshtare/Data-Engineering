# Lesson 6: Databricks Cost Management & Operations

> **Goal:** Control costs, monitor pipeline health, implement observability best practices, and operate Databricks as a production platform — not just a development tool.

---

## 🏗️ DBUs — Understanding How Databricks Billing Works

### 1. What is a DBU?

A **DBU (Databricks Unit)** is the billing unit for Databricks compute. Different cluster types consume DBUs at different rates:

| Cluster Type | DBU Rate (approx) | Use For |
|-------------|------------------|---------|
| All-Purpose (Jobs Light) | 0.1 DBU/hour per core | Dev, exploration |
| All-Purpose (Standard) | 0.15 DBU/hour per core | Interactive work |
| Jobs (Standard) | 0.1 DBU/hour per core | Scheduled pipelines |
| Jobs (Photon) | 0.25 DBU/hour per core | SQL/BI optimized pipelines |
| SQL Serverless | ~0.07 DBU/second (variable) | BI queries, pay per query |
| DLT Core | 0.2 DBU/hour per core | Basic DLT pipelines |
| DLT Advanced | 0.35 DBU/hour per core | DLT + Enhanced Autoscaling |

**Total Cost = DBUs consumed × DBU price (set by your cloud agreement)**

### 2. Cost Saving Strategies

```python
# ============================================
# Strategy 1: Use Spot/Preemptible VMs
# ============================================
# Spot = 70-80% cheaper than on-demand VMs!
# Risk: VM can be preempted (returned to cloud) mid-job
# Mitigation: Use SPOT_WITH_FALLBACK (try spot, fall back to on-demand)

# In cluster config:
{
  "aws_attributes": {
    "availability": "SPOT_WITH_FALLBACK",    # AWS
    "spot_bid_price_percent": 100            # Bid up to 100% of on-demand price
  }
}
# For Azure:
{
  "azure_attributes": {
    "availability": "SPOT_AZURE",
    "spot_bid_max_price": -1               # Bid current spot price (recommended)
  }
}

# ✅ Best for: Batch jobs (not streaming), non-time-critical workloads
# ❌ Avoid for: DLT streaming pipelines, real-time serving

# ============================================
# Strategy 2: Auto-termination on All-Purpose Clusters
# ============================================
# Default: Cluster stays on until manually stopped → silent money drain!
# Set: Auto-terminate after 30 minutes of inactivity

# ============================================
# Strategy 3: Right-size your clusters
# ============================================
# ANTI-PATTERN: Always using d4s_v3 (16 cores, 64GB) for every job
# BEST PRACTICE: Match VM to the workload

# Lightweight ETL (small tables): 2 workers × d2s_v3 (2 cores, 8GB) = 4 cores total
# Heavy analytics: 4 workers × d16s_v3 (16 cores, 64GB) = 64 cores total
# ML training: 2 workers × N6C-24GB-GPU = GPU cluster

# ============================================
# Strategy 4: Use Serverless SQL for BI queries
# ============================================
# Classic "always-on" SQL Warehouse: costs money even when no queries run!
# Serverless SQL Warehouse: costs money ONLY when a query is running!
# → For a team running 100 queries/hour (each 5s): 
#    Classic: ~$200/day (8 hours × $25/cluster-hour)
#    Serverless: ~$12/day (pay only for actual query seconds)

# ============================================
# Strategy 5: Cluster Policies to prevent overspending
# ============================================
# See Lesson 1 — Cluster Policies enforce max_workers, VM types, etc.
```

### 3. Cost Attribution with Tags

```python
# EVERY cluster must have tags for cost attribution!
# Without tags: "$50,000 Databricks bill this month — who spent what?" → Unknown!
# With tags: Easy breakdown by team, project, environment

# In cluster config:
{
  "custom_tags": {
    "team":        "data-engineering",
    "project":     "sales-lakehouse",
    "environment": "production",
    "cost_center": "CC-1234",
    "owner":       "apple@company.com"
  }
}

# Query cost by team using system tables:
SELECT
    usage_metadata.custom_tags.team     AS team,
    usage_metadata.custom_tags.project  AS project,
    SUM(usage_quantity)                 AS total_dbus,
    SUM(usage_quantity * pricing.dbu_price) AS estimated_cost_usd
FROM system.billing.usage
JOIN system.billing.list_prices pricing
    ON usage.sku_name = pricing.sku_name
WHERE usage_date >= date_trunc('month', current_date())
GROUP BY team, project
ORDER BY total_dbus DESC;
```

---

## 🚀 Observability — Monitoring a Production Platform

### 1. System Tables — Databricks' Built-in Telemetry

```sql
-- Unity Catalog system tables give you full visibility into your platform:

-- (1) Cluster usage and DBU consumption:
SELECT
    cluster_name,
    SUM(dbu_quantity)          AS total_dbus,
    MIN(start_time)            AS first_run,
    MAX(end_time)              AS last_run
FROM system.compute.clusters
WHERE start_time >= date_sub(current_date(), 30)
GROUP BY cluster_name
ORDER BY total_dbus DESC;

-- (2) Failed jobs (last 7 days):
SELECT
    job_name,
    run_name,
    trigger,
    state.result_state,
    state.state_message        AS error_message,
    start_time,
    end_time
FROM system.lakeflow.job_runs
WHERE start_time >= date_sub(current_timestamp(), 7)
  AND state.result_state = 'FAILED'
ORDER BY start_time DESC;

-- (3) DLT pipeline health:
SELECT
    pipeline_name,
    latest_update.state,
    latest_update.creation_time,
    latest_update.full_refresh
FROM system.lakeflow.pipelines
ORDER BY pipeline_name;

-- (4) Slow queries (SLA monitoring):
SELECT
    statement_text,
    user_name,
    total_duration_ms / 1000.0 AS duration_sec,
    warehouse_id
FROM system.query.history
WHERE total_duration_ms > 60000   -- Queries > 60 seconds
  AND start_time >= date_sub(current_date(), 1)
ORDER BY total_duration_ms DESC;
```

### 2. Setting Up Automated Alerts

```python
# ============================================
# Pattern: Monitor failed jobs and alert via Slack
# ============================================
from databricks.sdk import WorkspaceClient
import requests, json

w = WorkspaceClient()

def check_failed_jobs_and_alert(lookback_hours: int = 1):
    """Check for failed jobs and send Slack alert."""
    from datetime import datetime, timedelta

    # Get failed runs from the last hour
    failed_runs = []
    for run in w.jobs.list_runs(
        completed_only=True,
        start_time_from=int((datetime.now() - timedelta(hours=lookback_hours)).timestamp() * 1000)
    ):
        if run.state.result_state.value == "FAILED":
            failed_runs.append({
                "job_id":   run.job_id,
                "run_id":   run.run_id,
                "job_name": run.run_name or f"Job {run.job_id}",
                "error":    run.state.state_message or "No error message"
            })

    if not failed_runs:
        return {"status": "ok", "failed_count": 0}

    # Format Slack message
    blocks = [
        {"type": "header", "text": {"type": "plain_text", "text": f"🚨 {len(failed_runs)} Pipeline Failure(s)"}},
    ]
    for run in failed_runs[:5]:  # Show top 5
        blocks.append({
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": f"*{run['job_name']}*\nRun ID: `{run['run_id']}`\nError: _{run['error'][:150]}_"
            }
        })

    response = requests.post(
        SLACK_WEBHOOK_URL,
        json={"blocks": blocks}
    )
    return {"status": "alerted", "failed_count": len(failed_runs)}

# Schedule this function in an Airflow DAG or Databricks Workflow
# to run every 15 minutes!
```

### 3. Operational Runbook — What To Do When Things Break

```markdown
# Databricks Production Runbook

## 🔴 INCIDENT: DLT Pipeline Stuck / Not Updating

1. Check the Pipeline UI:
   Databricks → Delta Live Tables → [pipeline name] → Events tab
   Look for: red events, "BACKLOG" state, or "STOPPED" state

2. Check the event log:
   SELECT * FROM prod.system.dlt_event_log 
   WHERE timestamp > current_timestamp() - INTERVAL 2 HOURS
   ORDER BY timestamp DESC

3. Common causes and fixes:
   a) Kafka consumer lag: Source has too many messages
      → Temporarily increase DLT cluster num_workers
   b) Schema mismatch: Source added a new column
      → Add column to bronze schema definition + re-queue run
   c) OOM (Out of Memory): Data volume spike
      → Increase min/max workers in pipeline config

4. Restart the pipeline:
   Databricks → DLT → [pipeline] → Start → Full Refresh (if corrupted)
   OR: Just restart without full refresh (faster, safer for incremental)

---

## 🔴 INCIDENT: SQL Warehouse Very Slow

1. Check active queries:
   Databricks SQL → SQL Warehouses → [warehouse] → Monitoring tab
   Look for: queries in "QUEUED" state (warehouse is overloaded!)

2. Scale up temporarily:
   SQL Warehouse → Edit → Max Cluster Count → Increase to 4-8

3. Identify the bad query:
   SELECT statement_text, user_name, total_duration_ms
   FROM system.query.history
   WHERE start_time >= current_timestamp() - INTERVAL 1 HOUR
   ORDER BY total_duration_ms DESC
   LIMIT 5;
   → Kill the runaway query: Query History → SELECT query → Kill

4. Long-term fix:
   → Add OPTIMIZE + Z-ORDER to the queried tables
   → Add result caching for repeated queries
```

---

## 🏛️ DBX CLI & Automation

```bash
# Install Databricks CLI
pip install databricks-cli
# Or: pip install databricks-sdk

# Authenticate
databricks configure --token
  # Databricks Host: https://adb-XXXXX.azuredatabricks.net
  # Token: <your-personal-access-token>

# Common CLI operations:
databricks clusters list                            # See all clusters
databricks clusters start --cluster-id <id>         # Start a cluster
databricks clusters delete --cluster-id <id>        # Delete a cluster
databricks jobs list                                 # See all jobs
databricks jobs run-now --job-id 12345               # Trigger a job

# Deploy notebooks from local to workspace:
databricks workspace import_dir ./notebooks /Repos/prod/pipeline --overwrite

# Run a notebook and wait for result:
databricks runs submit \
  --existing-cluster-id <id> \
  --notebook-path /Repos/prod/pipeline/01_bronze \
  --timeout-seconds 3600

# Download a notebook for local editing:
databricks workspace export /Repos/prod/pipeline/01_bronze ./01_bronze.py --format SOURCE
```

---

### 4. Predictive Optimization
**Predictive Optimization** is a Databricks service that uses AI to automatically handle `OPTIMIZE` and `VACUUM` for your managed tables.
*   **The Problem:** You forget to run `OPTIMIZE`, and your queries slow down due to the "Small File Problem."
*   **The Fix:** Enable Predictive Optimization at the Catalog or Schema level. Databricks runs these maintenance tasks in the background using its own serverless compute.
*   **Cost:** You only pay for the Serverless compute used for the optimization.

---

## 🎯 Phase 4: Certification & Interview Drill

### 🛡️ Databricks Data Engineer Professional Drill
*   **DBU Calculation:** Understand that DBUs are calculated based on **Total Cores × Time Running × DBU Factor**. If a 10-node cluster (8 cores each) runs for 30 minutes, it's 80 cores for 0.5 hours = 40 core-hours.
*   **System Tables:** Know that `system.billing.usage` is the source of truth for all billing. You should know how to join this with `system.compute.clusters` to find which team is spending the most.

### 🛡️ DP-600 (Microsoft Fabric) Drill
*   **Fabric Smoothing:** Microsoft Fabric "smooths" out compute spikes over 24 hours. Databricks does not; you pay for the peak. For the exam, know that "Smoothing" allows you to run large jobs on smaller capacities as long as your 24-hour average stays within limits.

### 🏢 Consultancy Scenario: "The 40% Reduction"
**Scenario:** A client says, "Our Cloud bill is $1M/year. We need to cut it by 40% without deleting any data or stopping any pipelines."
*   **Architect Answer:** **The Multi-Pronged Attack.**
    1.  **Move to Job Clusters:** 20% savings.
    2.  **Enable Spot with Fallback:** 15% savings.
    3.  **Aggressive Auto-termination (10 mins):** 10% savings.
    4.  **Right-sizing:** Switch from 'High Memory' to 'Standard' VMs where memory isn't the bottleneck.

### 🚀 Startup Scenario: "The Cloud Quota Wall"
**Scenario:** Your startup just got a huge new customer. You try to spin up a 50-node cluster, and it fails with "OperationNotAllowed: Quota Exceeded."
*   **Answer:** **Cloud Quota Management.**
*   **The Drill:** Databricks clusters rely on your underlying cloud's (Azure/AWS/GCP) Core Quotas. You must proactively request a **Quota Increase** from your cloud provider before a big launch. Also, use **Serverless** where possible, as it uses Databricks' quotas, not yours.

### 🏛️ FAANG Scenario: "The Central Governance Dashboard"
**Scenario:** "Build a dashboard that monitors the health and cost of 5,000 separate pipelines across 10 different business units."
*   **Answer:** **Unity Catalog System Tables + Databricks SQL.**
*   **The Drill:** Use the `system.lakeflow.job_runs` and `system.billing.usage` tables to build a centralized **Observability Dashboard**. Use **Catalog Tags** to group accounts by business unit. Set up an **Alert** to ping the owner if a job fails 3 times in a row.

---

## 🧪 Practice Exercises

### Exercise 1 — Budget Alerts (Beginner)
**Goal:** Learn how to prevent "Bill Shock" by setting up a threshold alert.

```text
Step-by-Step (Simulation):
1. Log in to the Databricks Account Console (accounts.cloud.databricks.com).
2. Go to 'Usage' -> 'Budgets'.
3. Click 'Create Budget'.
   Name: "Monthly Data Engineering Sandbox"
   Amount: 1000  (USD)
4. Set Alert:
   When usage exceeds 80% (800 USD) -> Send Email to data-leads@company.com.
5. Click 'Create'.

Question: Does this budget stop your clusters from running?
Answer: NO. Budgets in Databricks are purely informational alerts. To stop clusters, you need to use Cluster Policies.
```

---

### Exercise 2 — Analyzing DBU Consumption (Intermediate)
**Goal:** Use SQL to find the "Top N" most expensive clusters in your workspace.

```sql
-- Query the Billing System Table
-- Requires Unity Catalog and System Tables enabled
SELECT
    usage_date,
    sku_name,
    usage_metadata.cluster_id,
    SUM(usage_quantity) as total_dbus
FROM system.billing.usage
WHERE usage_date >= date_add(current_date(), -7)
GROUP BY 1, 2, 3
ORDER BY total_dbus DESC
LIMIT 10;

-- Insight: If you see 'All-Purpose' clusters at the top, 
-- those are candidates for migration to Job Clusters.
```

---

### Exercise 3 — Tagging for Cost Attribution (Architect)
**Goal:** Enforce that every cluster MUST have a 'Department' tag for billing.

```json
/* 
1. Go to Compute -> Cluster Policies -> Create Policy
2. Name: "Department-Enforced-Policy"
3. Paste this rule:
*/
{
  "custom_tags.Department": {
    "type": "fixed",
    "value": "Data-Engineering",
    "hidden": false
  },
  "autotermination_minutes": {
    "type": "fixed",
    "value": 20
  }
}
/*
Result: Users cannot create a cluster without this tag, and they cannot 
disable auto-termination.
*/
```

---

## 💼 Common Interview Questions

**Q1: What is a DBU (Databricks Unit) and how is it different from the cloud VM cost?**
> A **DBU** is a unit of processing power per hour, billed by Databricks for the software license. The **Cloud VM cost** (EC2/Azure VM/Compute Engine) is billed separately by the cloud provider (AWS/Azure/GCP). Total Cost = (DBUs * Price per DBU) + (Cloud VM Price).

**Q2: When would you use a Spot Instance vs. an On-Demand Instance?**
> Use **Spot Instances** for non-critical, fault-tolerant workloads (like daily batch pipelines or Dev sandboxes) to save up to 90% on VM costs. Use **On-Demand** for time-sensitive, business-critical pipelines where you cannot afford the VM being reclaimed by the cloud provider.

**Q3: How does Databricks Serverless save money if the DBU rate is higher than standard?**
> While the DBU rate is higher, you only pay for the **exact seconds** the query is running. In a standard warehouse, you pay for the cluster to be "On" even if it's idle for 5 minutes between queries. For unpredictable, "spiky" BI workloads, Serverless is almost always cheaper overall.

**Q4: What is the most common cause of high Databricks bills for beginners?**
> Leaving **All-Purpose Clusters** running overnight with no auto-termination. Always ensure a 15-30 minute auto-termination policy is applied to all development clusters.

**Q5: What is the advantage of using Multi-Workspace Account Architecture?**
> It provides **Isolation**. You can have a "Dev Workspace" and a "Prod Workspace". If a developer accidentally runs a heavy job in Dev, it cannot steal compute resources or access sensitive data in the Prod environment. Unity Catalog allows you to govern both from one place.

---

### ✅ Key Takeaways
1. **DBUs** are your currency. Spend them wisely on **Job Clusters**.
2. **Predictive Optimization** automates the boring parts of Delta Lake maintenance.
3. **Tags** are not optional. If you can't attribute cost, you can't manage it.
4. **Serverless** is the "Zero-Ops" path to low-cost BI.
5. **System Tables** are the ultimate source of truth for platform security and cost.
6. **Observability** is proactive. Don't wait for the bill to arrive; monitor your usage daily.

# 🎉 End of Chapter 7: Databricks In-Depth
Congratulations! You have completed the technical deep dive into the Databricks Lakehouse Platform. You are now prepared for the Databricks Data Engineer Professional certification and can architect complex, cost-efficient data platforms at scale.

[Next Chapter: Phase 8: Microsoft Fabric (The Enterprise Analytics Suite) →](../../Phase_8_Microsoft_Fabric/README.md)
