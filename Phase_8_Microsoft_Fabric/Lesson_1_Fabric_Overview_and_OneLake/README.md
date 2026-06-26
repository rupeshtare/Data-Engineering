# Lesson 1: Microsoft Fabric Overview & OneLake

> **Goal:** Understand the Microsoft Fabric platform architecture, how OneLake unifies all storage, and how the workspace and security model works.

---

## 🏗️ Phase 1: What Makes Fabric Unique?

### 1. Everything in One SaaS Platform

Fabric is a **Software-as-a-Service (SaaS)** platform. Unlike Azure Data Factory + Synapse + Power BI (which were separate services you stitched together), Fabric is:
-  **One product** — single sign-on, single billing, single admin portal
-  **No infrastructure to manage** — no clusters to create (Fabric manages its own Spark)
-  **Native integration** — Power BI, Spark, SQL, and pipelines share the same data automatically

### 2. The Fabric Workspace

A **Workspace** in Fabric is like a project folder. It contains all the items for one team or project:

```
📁 Workspace: "Sales Analytics"
├── 🏠 Lakehouse: "SalesLakehouse"         (Delta Lake storage + SQL endpoint)
├── 📋 Pipeline: "DailySalesPipeline"       (Data Factory ETL)
├── 📓 Notebook: "02_transform_silver.ipynb" (Spark notebook)
├── 🗄️  Warehouse: "SalesWarehouse"         (Dedicated SQL warehouse)
├── ⚡ KQL Database: "RealtimeEvents"        (Real-time analytics)
├── 📊 Report: "SalesDashboard"             (Power BI report)
└── 📐 Semantic Model: "SalesModel"          (Power BI dataset / data model)
```

### 3. OneLake — The Foundation of Fabric

**OneLake** is the single, unified storage layer for ALL of Fabric. Think of it as **"OneDrive for data"** — one place, automatic organization.

```
OneLake Architecture:

├── Tenant Level: one OneLake per Microsoft tenant (organization)
│
├── Workspace A: Sales Team
│   ├── SalesLakehouse/
│   │   ├── Tables/          → Delta Lake tables (structured data)
│   │   │   ├── bronze_orders/
│   │   │   ├── silver_sales/
│   │   │   └── gold_revenue/
│   │   └── Files/           → Unstructured files (CSVs, JSONs, images)
│   │       └── landing/
│   └── SalesWarehouse/
│       └── Tables/          → Warehouse-managed tables
│
└── Workspace B: Marketing Team
    └── MarketingLakehouse/
        ├── Tables/
        └── Files/
```

**Why OneLake is powerful:**
-  **No data copies between Fabric items** — A Pipeline writes to OneLake, a Notebook reads it, Power BI queries it. Same files. No duplication.
-  **Open format** — All tables stored as **Delta Lake Parquet** files — readable by any tool (Spark, Python, Azure Synapse, even Databricks!)
-  **Shortcuts** — Reference data from Azure Data Lake, AWS S3, or Google Cloud Storage WITHOUT copying it into OneLake.

### 4. OneLake Shortcuts — The Game Changer

```
The Problem WITHOUT Shortcuts:
   Azure Data Lake (where SAP ERP writes data) → Copy to Fabric → Process → Report
   = One more copy = more cost, more latency, more sync issues!

The Solution WITH Shortcuts:
   Azure Data Lake (where SAP ERP writes data) → Shortcut in Fabric OneLake
   = Fabric READS the data directly from ADLS as if it's in OneLake
   = No copy at all!
```

```
Create a Shortcut in Fabric:
1. Open your Lakehouse → Files → New Shortcut
2. Choose External Source:
   • Azure Data Lake Storage Gen2
   • Azure Blob Storage
   • Amazon S3
   • Google Cloud Storage
   • Another Fabric Lakehouse (cross-workspace!)
3. Provide connection details + path
4. Done! The folder appears in your Lakehouse — reads data directly from source.
```

---

## 🚀 Phase 2: Capacity, Licensing, and Security

### 1. Capacity Model — F-SKUs (How Fabric is Priced)

Unlike Databricks (pay-per-DBU), Fabric is **capacity-based** — you buy a fixed monthly compute block:

| SKU | Capacity Units (CUs) | Approx Monthly Cost | Best For |
|-----|---------------------|--------------------|---------| 
| F2  | 2 CUs               | ~$262/month         | Experiments, dev |
| F4  | 4 CUs               | ~$524/month         | Small team |
| F8  | 8 CUs               | ~$1,048/month       | Medium team |
| F16 | 16 CUs              | ~$2,096/month       | Large workloads |
| F64 | 64 CUs              | ~$8,384/month       | Enterprise |
| F128| 128 CUs             | ~$16,768/month      | Large enterprise |

**CUs = how much compute power you have at any moment**

```
Think of it like internet bandwidth:
• Fabric is your "internet plan" — you buy 100 Mbps (F8 = 8 CUs)
• When you run a pipeline — uses 4 CUs
• When you run a notebook — uses 2 CUs
• When Power BI refreshes — uses 2 CUs
• Running all at once = 8 CUs → at your limit (slower performance)
• Burst: Fabric can "borrow" extra CUs from Microsoft for temporary spikes
```

### 2. Fabric Security Model

```
Three Levels of Security in Fabric:

1. Workspace-Level (who can access the workspace):
   ├── Admin    → Full control, manage members, delete workspace
   ├── Member   → Create and edit all items, can't manage members
   ├── Contributor → Create and edit items they own
   └── Viewer   → Read-only access to everything in the workspace

2. Item-Level (who can access specific Lakehouse, Warehouse, etc.):
   ├── Read       → Can read data
   ├── Write      → Can write/modify data
   ├── ReadAll    → Can read all data including low-level Delta files
   └── Execute    → Can run notebooks/pipelines

3. Data-Level (Row-Level Security, Column Security):
   → Managed in Power BI Semantic Models (RLS rules)
   → Or managed via SQL in the Warehouse (GRANT/REVOKE)
   → Or via Microsoft Purview for sensitivity labels
```

### 3. Microsoft Purview Integration — Enterprise Governance

```
Fabric integrates with Microsoft Purview for:
├── Data Catalog     → Automatically discover and document all Fabric tables
├── Sensitivity Labels → Apply "Confidential", "PII", "Internal" labels to tables/columns
├── Data Loss Prevention (DLP) → Block export of "Confidential" data to unmanaged devices
├── Lineage          → Track data flow from source → Lakehouse → Warehouse → Power BI
└── Audit            → Who accessed what, when (integrated with Azure AD audit logs)
```

---

## 🏛️ Phase 3: Fabric vs Azure Data Services

### When to Use What

```
You need to...                                 Use This in Fabric
──────────────────────────────────────────────────────────────────
Store and process large datasets (Spark)   → Fabric Lakehouse + Notebooks
Run SQL analytics queries                  → Fabric Data Warehouse
Ingest data from 200+ connectors          → Data Factory (Copy Activity)
Transform data with low-code GUI          → Dataflows Gen2
Real-time event processing               → Eventstream + KQL Database
Create interactive dashboards             → Power BI (built-in!)
Build ML models                           → Data Science (notebooks + MLflow)
Govern data across the organization       → Microsoft Purview
```

---

### 5. Mirroring — Real-time Replication without ETL
**Mirroring** in Fabric is a zero-ETL integration that automatically synchronizes your existing operational databases (Cosmos DB, Azure SQL, Snowflake) into OneLake.
*   **How it works:** Once enabled, it replicates your database into a **read-only Delta Lake table** in OneLake.
*   **The Benefit:** Your BI analysts can query live sales data in Power BI without you ever writing a single line of ingestion code.

---

## 🎯 Phase 4: Certification & Interview Drill

### 🛡️ DP-600 (Microsoft Fabric) Drill
*   **OneLake Metadata:** Know that every item in OneLake is represented as a **folder**. Tables are folders containing `.parquet` and `_delta_log` files.
*   **Smoothing & Throttling:** Understand the concept of **Smoothing**. Spikes in compute (like a huge notebook run) are averaged over a 24-hour window. If your *average* usage exceeds your F-SKU capacity, your workspace will be **throttled** (slowed down).

### 🏢 Consultancy Scenario: "The 100 Subsidiary Chaos"
**Scenario:** A client has 100 small subsidiaries, each with its own data. They want a central view but need strict isolation.
*   **Architect Answer:** **Fabric Domains.**
*   **The Move:** Organize the 100 workspaces into **Domains** (e.g., "Finance Domain", "Logistics Domain"). This allows for domain-scoped administration and auditing in Microsoft Purview while still keeping all data in the same OneLake tenant for central reporting.

### 🚀 Startup Scenario: "Surviving on F2"
**Scenario:** Your startup is on the smallest F2 SKU. A developer accidentally runs a massive Spark cross-join that tries to consume 100 CUs.
*   **Answer:** **Bursting and Smoothing.**
*   **The Drill:** Fabric will allow the job to "Burst" and use more than 2 CUs if they are available in the shared pool. However, the cost of that burst is "smoothed" over the next 24 hours. The startup won't get a surprise bill, but their Fabric environment might be "Rejected/Throttled" for a few hours later in the day if they don't manage their usage.

### 🏛️ FAANG Scenario: "The S3 Ghost Data"
**Scenario:** "Our marketing data (1PB) lives in AWS S3. We don't want to pay egress fees to move it to Azure/Fabric, but we want to use it in Power BI. What do we do?"
*   **Answer:** **Shortcuts.**
*   **The Drill:** You create an **S3 Shortcut** in OneLake. Fabric will read the data *on-demand* when the report is opened. While there are still some egress costs for the data read, you avoid the massive "One-time move" cost and the complexity of maintaining two copies of 1PB of data.

---

### 🧪 Hands-on Labs
- [onelake_explorer_lab.md](onelake_explorer_lab.md) (Step-by-step guide to setting up a workspace and creating your first shortcut)

---

### ✅ Key Takeaways
1. **Fabric is SaaS.** You don't manage VMs; you manage "Capacities" (F-SKUs).
2. **OneLake** is your single source of truth. One tenant, one lake.
3. **Shortcuts** are virtual pointers to S3, ADLS, or other Fabric workspaces.
4. **Mirroring** is the "Zero-ETL" dream for databases.
5. **Smoothing** protects you from surprise bills but requires monitoring.
6. **Purview** is the brain of your governance, handling labels and lineage.

[Next: Lesson 2: Data Factory & Pipelines (Modern Ingestion) →](../Lesson_2_Data_Factory_Pipelines/README.md)

---

## 🧪 Practice Exercises

### Exercise 1 — Explore the Fabric Portal (Beginner)
**Goal:** Get familiar with the Fabric interface.

```
Steps:
1. Sign up for the Microsoft Fabric free trial:
   https://app.fabric.microsoft.com → Start free trial (60 days)

2. Create a new Workspace:
   Name: "MyFabricLearning"
   License mode: Trial

3. Inside the workspace, create these items (just create, not configure yet):
   a. A Lakehouse: "LearningLakehouse"
   b. A Data Pipeline: "TestPipeline"
   c. A Notebook: "TestNotebook"

4. Open the Lakehouse → observe the two zones:
   • Files/ (for raw unprocessed files)
   • Tables/ (for Delta Lake-registered tables)

5. Answer:
   → Where in the OneLake storage hierarchy does your Lakehouse sit?
   → What is the OneLake path? (Look: workspace_name / lakehouse_name.Lakehouse)
```

**Expected outcome:** You can navigate Fabric, understand workspace structure, and see how OneLake organizes data.

---

### Exercise 2 — Create a OneLake Shortcut (Intermediate)
**Goal:** Connect an external data source without copying data.

```
Steps:
1. In your Lakehouse → Files → click "..." → New Shortcut

2. If you have an Azure subscription:
   Source: Azure Data Lake Storage Gen2
   → Enter your ADLS connection string
   → Choose a container/folder to shortcut

   If you DON'T have Azure:
   Source: Another Fabric Lakehouse (use your own LearningLakehouse as source!)
   → This creates an internal shortcut (still demonstrates the concept)

3. After creation, the shortcut folder appears in Files/
   → Click it → verify you can see the files without having moved them

4. Open a Notebook in the same workspace → read the shortcut:
   df = spark.read.csv("Files/shortcut_folder_name/*.csv", header=True)
   display(df)
```

**Expected outcome:** Understand that shortcuts are virtual pointers — no data was copied, yet Spark can read it natively.

---

### Exercise 3 — Capacity Planning Calculation (Architect Level)
**Goal:** Practice estimating Fabric capacity requirements.

```
Scenario: You are designing Fabric for a mid-size company with:
  • 3 data engineers running Spark notebooks (2 hours each, 9 AM–11 AM daily)
  • 1 daily pipeline: runs 1 hour at 2 AM (Copy Activity + 2 Notebooks)
  • 50 Power BI users, peak 20 concurrent during 9 AM–5 PM

Estimate:
  • Spark small node = ~2 CUs each
  • Each Notebook session uses up to 4 nodes = 8 CUs
  • Pipeline copy activity ≈ 2 CUs
  • Power BI Direct Lake query ≈ 0.1 CUs per user at peak

Calculate:
  1. Peak CU consumption (9 AM, all engineers start notebooks simultaneously):
     3 engineers × 8 CUs = 24 CUs from notebooks
     + 20 Power BI users × 0.1 = 2 CUs from BI
     Total peak = 26 CUs

  2. Which F-SKU do you recommend? (F32 = 32 CUs ≈ $4,192/month)
     
  3. How would you reduce cost?
     Hint: Stagger engineer start times, use autoscale Spark pools, enable burst.

Answer the following:
  → What F-SKU do you recommend? Why?
  → What architectural change saves the most CUs?
  → If the company adds 5 more engineers next quarter, does your SKU still work?
```

---

## 💼 Common Interview Questions

**Q1: What is OneLake and why is it significant?**
> OneLake is a single, tenant-wide data lake that underpins all of Microsoft Fabric. Every Fabric item (Lakehouse, Warehouse, KQL Database) stores its data in OneLake as open Delta Lake Parquet files. Its significance: (1) No data duplication between Fabric services — a Spark notebook and a Power BI report read the same physical files. (2) Open format means no vendor lock-in — Databricks, Synapse, or plain Spark can read the files. (3) Shortcuts allow federating data from S3/ADLS/GCS without any ETL copy.

**Q2: How does Fabric pricing differ from Databricks?**
> Fabric uses a **capacity model** (F-SKUs, measured in Capacity Units). You buy a fixed monthly block (e.g., F8 = $1,048/month) and ALL Fabric workloads (Spark, pipelines, Power BI, SQL) share that pool. Databricks uses a **consumption model** (DBUs) — you pay per second of cluster usage. Fabric is more predictable for steady workloads; Databricks is better for bursty, unpredictable ML training jobs.

**Q3: What is a Fabric Workspace and what does it contain?**
> A Workspace is a project-level container in Fabric. It groups together related Fabric items: Lakehouses, Pipelines, Notebooks, SQL Warehouses, KQL Databases, Semantic Models, and Power BI Reports. It also controls access (Admin/Member/Contributor/Viewer roles). Think of it as a "project folder" where all the data, transformation, and reporting items for one team or domain live together.

**Q4: What makes OneLake Shortcuts a "game changer"?**
> Before Shortcuts, you had to run a copy job to bring external data (from S3, ADLS, etc.) into your platform — costing time, money, and creating sync drift. A Shortcut is a virtual pointer in OneLake that lets Fabric items read data directly from the external location as if it were local. No pipeline, no copy, no duplicate storage costs. The data stays in the source system; Fabric reads it transparently.

**Q5: How does Fabric's security model work at different levels?**
> Three levels: (1) **Workspace level** — Admin/Member/Contributor/Viewer control who can access all items in a workspace. (2) **Item level** — Fine-grained permissions on individual Lakehouses, Warehouses, or reports (Read, Write, ReadAll, Execute). (3) **Data level** — Row-Level Security in Semantic Models (DAX rules per role), column-level via Object-Level Security, and sensitivity labels + DLP policies via Microsoft Purview.

