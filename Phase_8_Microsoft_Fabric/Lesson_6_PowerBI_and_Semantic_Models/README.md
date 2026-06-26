# Lesson 6: Power BI & Semantic Models in Microsoft Fabric

> **Goal:** Understand how Power BI is natively integrated into Microsoft Fabric — including the revolutionary Direct Lake mode that eliminates the need to import data, how to build Semantic Models (the governed data layer), enforce Row-Level Security, and deploy reports across dev/test/prod environments using Deployment Pipelines.

---

## 🏗️ Phase 1: Power BI in Fabric — What Changed

### 1. Power BI Before Fabric vs. Inside Fabric

```
Power BI Before Fabric (2023 and earlier):
──────────────────────────────────────────────────────────────────
1. Store data in Azure Data Lake / Synapse
2. IMPORT data into Power BI's in-memory engine (VertiPaq)
   → Full copy of data inside Power BI — no longer live!
3. Schedule refresh: "Refresh dataset every 8 hours"
   → Data stale between refreshes (up to 8 hours old)
4. Large datasets hit 1 GB limit (Premium only: up to 400 GB)
5. SSAS (Analysis Services) processes data separately

Power BI INSIDE Fabric (Direct Lake mode):
──────────────────────────────────────────────────────────────────
1. Store data in Fabric Lakehouse (Delta Lake on OneLake)
2. Power BI reads DIRECTLY from OneLake Delta files — NO import!
   → Data is always current (same files the Notebook just wrote)
3. No scheduled refresh needed — reports show latest data every query
4. No file size limits for Direct Lake tables
5. Power BI's engine (VertiPaq) and Spark now share the same OneLake files
```

### 2. The Three Power BI Connection Modes

| Mode | How It Works | Data Freshness | Performance | When to Use |
|------|-------------|----------------|------------|-------------|
| **Import** | Copies data into Power BI memory | Up to 8h stale | Fastest (in-memory) | Small tables, frequent complex calculations |
| **DirectQuery** | Queries source DB live on every report click | Always live | Slowest (round-trips to source) | When you can't import (privacy, size) |
| **Direct Lake** ⭐ | Reads Delta files in OneLake directly | Always live | Near-Import speed | Fabric Lakehouse + large datasets |

> 💡 **Direct Lake is the #1 reason to choose Fabric over standalone Power BI Premium.** It gives you live data at near-in-memory speed — the best of both worlds.

---

## 🚀 Phase 2: Direct Lake Mode — Deep Dive

### 1. How Direct Lake Works Internally

```
Traditional Import Mode:
  Lakehouse Delta Table → [COPY] → Power BI VertiPaq Engine (in-memory)
     Changes in Lakehouse → Must re-trigger Refresh → 30 mins to update

Direct Lake Mode:
  Lakehouse Delta Table ← Power BI reads Delta Parquet files directly from OneLake
     Notebook writes new data → Power BI sees it immediately (next query)

The key technology: "Framing"
  • When you open a Direct Lake report, Fabric takes a consistent snapshot
    of the Delta table's current state (like reading a Delta "version")
  • VertiPaq loads only the columns queried (columnar read) straight from Parquet
  • No copy, no import, no latency
```

### 2. Setting Up a Direct Lake Semantic Model

```
Step-by-Step: Create a Direct Lake Semantic Model
──────────────────────────────────────────────────
1. In your Workspace → New → Semantic Model
   Name: "SalesSemanticModel"
   Source: "SalesLakehouse" (Fabric Lakehouse)

   Fabric auto-detects all Delta tables:
   ✅ bronze_orders      (select: No — don't expose raw data to business users)
   ✅ silver_orders      (select: No — also internal)
   ✅ gold_daily_revenue (select: YES — BI-ready aggregated data)
   ✅ dim_customer       (select: YES)
   ✅ dim_date           (select: YES)

2. The Semantic Model editor opens (same as Power BI Desktop model view)

3. Define Relationships:
   gold_daily_revenue[date_key]     → dim_date[date_key]      (Many-to-One)
   gold_daily_revenue[customer_key] → dim_customer[cust_key]  (Many-to-One)

4. Add Measures (DAX):
   Total Revenue = SUM(gold_daily_revenue[total_revenue])
   Total Orders  = SUM(gold_daily_revenue[total_orders])
   Avg Order Value = DIVIDE([Total Revenue], [Total Orders])
   MoM Growth %  = DIVIDE(
       [Total Revenue] - CALCULATE([Total Revenue], PREVIOUSMONTH(dim_date[date])),
       CALCULATE([Total Revenue], PREVIOUSMONTH(dim_date[date]))
   )

5. Save → Semantic Model published to Workspace
   → All Power BI reports in the workspace can now use this shared model
   → Connection mode: Direct Lake (automatic — because source is Fabric Lakehouse)
```

### 3. DAX Measures — The Language of Business Logic

DAX (Data Analysis Expressions) is the formula language for Power BI Semantic Models:

```dax
// ── Basic aggregations ────────────────────────────────────────────

Total Revenue =
    SUM(gold_daily_revenue[total_revenue])

Total Orders =
    COUNT(gold_daily_revenue[order_id])

Unique Customers =
    DISTINCTCOUNT(gold_daily_revenue[customer_id])

// ── Time intelligence (requires dim_date table with proper Date column) ────

Revenue YTD =
    CALCULATE(
        [Total Revenue],
        DATESYTD(dim_date[date])      // Year-to-date from Jan 1
    )

Revenue Previous Month =
    CALCULATE(
        [Total Revenue],
        PREVIOUSMONTH(dim_date[date])
    )

MoM Growth % =
    VAR current = [Total Revenue]
    VAR prior   = [Revenue Previous Month]
    RETURN
        DIVIDE(current - prior, prior, BLANK())   // Returns BLANK if no prior data

// ── Conditional / filtered measures ──────────────────────────────

Revenue from Enterprise Customers =
    CALCULATE(
        [Total Revenue],
        dim_customer[tier] = "Enterprise"
    )

Top 10 Revenue =
    CALCULATE(
        [Total Revenue],
        TOPN(10, dim_customer, [Total Revenue])
    )

// ── What-if analysis (with a parameter table) ─────────────────────

// After creating a "Discount Rate" what-if parameter slider (0% to 50%):
Adjusted Revenue =
    [Total Revenue] * (1 - 'Discount Rate'[Discount Rate Value])
```

---

## 🛡️ Phase 3: Row-Level Security (RLS)

### 1. What is RLS?

**Row-Level Security** restricts which rows a user sees in Power BI based on their identity — without creating separate reports per user.

```
Example: Regional Sales Reports
─────────────────────────────────────────────────────────────────────
Without RLS:
  • User "John (Asia Manager)" opens the Sales Dashboard
  • He sees ALL regions: America, Europe, Asia, Africa
  • He could accidentally view data he shouldn't

With RLS:
  • User "John (Asia Manager)" opens the SAME Sales Dashboard
  • He only sees: Asia — because Fabric filters all visuals automatically
  • No separate report needed — one report, personalized by identity
```

### 2. Implementing RLS on a Semantic Model

```
Step 1: Define RLS Roles in the Semantic Model
  Semantic Model → Manage Roles → New Role

Role Name: "RegionViewer"

DAX Filter on table "dim_region":
  [region_name] = USERPRINCIPALNAME()
  // ⬆️ Matches region to the logged-in user's email — not practical
  // Better approach: use a mapping table

// Better: User-Region Mapping Table
// Create table "user_region_map" in your Lakehouse:
//   email                         | region
//   john@company.com              | Asia
//   priya@company.com             | Europe
//   carlos@company.com            | Americas

Role Name: "RegionViewer"
DAX Filter on "gold_daily_revenue":
  [region] IN
      CALCULATETABLE(
          VALUES(user_region_map[region]),
          user_region_map[email] = USERPRINCIPALNAME()
      )
// ⬆️ Dynamically filters to the regions the logged-in user is allowed to see

Step 2: Assign Users to the Role
  Semantic Model → Security → RegionViewer role → Add: john@company.com
  
  Or: Assign an Azure AD SECURITY GROUP (best for large orgs):
  Add "Sales Managers - Asia" security group → All members get Asia data only

Step 3: Test RLS before publishing
  Semantic Model → Model View → View As Role → "RegionViewer"
  Enter test username: john@company.com
  → Visuals now show only Asia data — verify correctness
```

### 3. Object-Level Security (OLS) — Hiding Columns

While RLS hides rows, **Object-Level Security** hides entire **columns or tables** from specific roles:

```
Use case: Hide "cost_price" column from external sales reps
          but show it to Finance team

In the Semantic Model → Manage Roles → "SalesRep" role:
  OLS → Table: "gold_daily_revenue" → Column: "cost_price" → Hidden

Now "SalesRep" users cannot see or reference [cost_price] in any report.
(They don't even see it in the field list — it's completely invisible to them)
```

---

## 🏗️ Phase 4: Deployment Pipelines — Dev / Test / Prod

### 1. Why Deployment Pipelines?

Without deployment pipelines, teams either:
- Build in production (risky — a broken report goes live instantly)
- Manually export PBIX files and re-import (error-prone, no version control)

Fabric's **Deployment Pipelines** solve this with a 3-stage promote-and-review flow:

```
┌─────────────────┐     Promote      ┌─────────────────┐     Promote      ┌─────────────────┐
│   Development   │ ───────────────► │     Test        │ ───────────────► │   Production    │
│   Workspace     │                  │   Workspace     │                  │   Workspace     │
│                 │                  │                 │                  │                 │
│ • Engineers     │                  │ • QA team tests │                  │ • Business users│
│   build here    │                  │   with real-    │                  │   view reports  │
│ • Sandbox data  │                  │   like data     │                  │ • Live data     │
└─────────────────┘                  └─────────────────┘                  └─────────────────┘
```

### 2. Setting Up a Deployment Pipeline

```
Step-by-Step:
─────────────────────────────────────────────────────
1. Workspace menu → Deployment Pipelines → New Pipeline
   Name: "Sales Analytics Pipeline"

2. Assign workspaces to stages:
   DEV  → "SalesAnalytics-Dev" workspace
   TEST → "SalesAnalytics-Test" workspace
   PROD → "SalesAnalytics-Prod" workspace

3. Click "Deploy" from DEV → TEST:
   Fabric shows a diff of what changed:
   ✅ Modified: "SalesSemanticModel" (2 measures changed)
   ✅ Modified: "SalesDashboard" report (3 visuals updated)
   ⬜ Unchanged: "SalesLakehouse", Pipelines (not re-deployed)
   
   Click "Deploy" → Fabric copies only the changed items to TEST

4. QA team tests in TEST workspace with TEST data

5. Click "Deploy" from TEST → PROD
   → Only the validated, reviewed items go to production
```

### 3. Deployment Rules — Different Config per Stage

You don't want the PROD Semantic Model pointing to the DEV Lakehouse. **Deployment Rules** solve this:

```
In the Deployment Pipeline → PROD stage → Deployment Rules:

Rule 1: "SalesSemanticModel" → Data Source
  DEV value:  SalesLakehouse-Dev  (sandbox data)
  PROD value: SalesLakehouse-Prod (real production data)

Rule 2: "DailySalesPipeline" → Parameter "environment"
  DEV value:  "dev"
  PROD value: "prod"

Rule 3: "SalesSemanticModel" → Data Source Connection
  DEV value:  dev-sql-server.database.windows.net
  PROD value: prod-sql-server.database.windows.net

→ When you promote to PROD, Fabric automatically substitutes these values.
  No manual editing needed after promotion.
```

---

## ⚡ Phase 5: Performance & Best Practices

### 1. Direct Lake Guardrails — When It Falls Back to DirectQuery

Direct Lake has limits. If exceeded, the model silently falls back to DirectQuery (slower):

```
Direct Lake fallback triggers (as of Fabric Runtime 2024):
  • > 300,000 unique values in a single column  → falls back
  • A computed column that requires data not in Delta → falls back
  • A measure with unsupported DAX functions    → falls back

How to check if you're hitting fallback:
  In Power BI Desktop: View → Performance Analyzer → Record
  Run a visual → check if query shows "Direct Lake" or "DirectQuery"

Fix: Reduce cardinality of high-cardinality columns
     Use SUMMARIZE pre-aggregation in notebooks
     Avoid complex nested calculated columns in the model
```

### 2. Semantic Model Best Practices

```
✅ DO:

• Keep the Semantic Model thin — only include tables needed for BI.
  Heavy transformations belong in Spark notebooks (Silver/Gold), not in DAX.

• Use star schema — one fact table (fact_sales) + multiple dimension tables
  (dim_customer, dim_date, dim_product). Avoid snowflake schemas.

• Create a Calendar / Date table — time intelligence DAX functions require
  a proper continuous date table:
  dim_date with columns: date, year, quarter, month_name, day_of_week, is_weekend

• Mark your date table:
  Table tools → Mark as date table → Date column = dim_date[date]

• Prefix measures with 📊 icons via display folders:
  Folder: "Revenue Metrics" → Total Revenue, Revenue YTD, MoM Growth %
  Folder: "Order Metrics"   → Total Orders, Avg Order Value

❌ DON'T:

• Don't import raw Bronze tables into the Semantic Model
• Don't put business logic (IF/THEN, lookups) in Power Query — do it in Spark
• Don't create many-to-many relationships unless absolutely necessary
  (they cause performance issues and ambiguous filter directions)
• Don't use bidirectional relationship filters by default
  (they confuse filters and cause slow queries — use CROSSFILTER() in DAX instead)
```

### 3. Incremental Refresh — For Large Lakehouse Tables

For Direct Lake tables with years of historical data, enable incremental refresh:

```
Semantic Model → Select Table → Incremental Refresh → ON

Settings:
  Archive data: 3 years     (keep 3 years total in the model)
  Refresh:      10 days     (only reimport the last 10 days on each refresh)

Requires:
  • Table must have a Date/DateTime column (e.g., order_date)
  • RangeStart and RangeEnd Power Query parameters must be defined
  → Fabric only queries the Delta files for the refresh date range
  → Massive reduction in refresh time for multi-year datasets
```

### 4. Semantic Link — The Bridge between Spark and BI
**Semantic Link** is a feature that allows you to read Power BI Semantic Models directly into a Spark Notebook using Python.
*   **The Move:** Use the `sempy` library to query your Power BI DAX measures inside a Python notebook.
*   **Architect Note:** This is powerful for **Data Quality checks**. You can write a notebook that compares the "Total Sales" in your Gold table vs. the "Total Sales" measure in Power BI to ensure they match before the CEO sees them.

```python
# In a Fabric Notebook:
import sempy.fabric as fabric

# List all measures in a semantic model
measures = fabric.list_measures("SalesSemanticModel")
display(measures)

# Query a DAX measure directly into a Spark DataFrame
df_revenue = fabric.evaluate_dax(
    "SalesSemanticModel",
    "EVALUATE SUMMARIZECOLUMNS('dim_date'[Year], \"Total Revenue\", [Total Revenue])"
)
display(df_revenue)
```

---

## 🏛️ Phase 6: End-to-End Architecture — Fabric + Power BI

```
Complete data flow from source to CEO dashboard:

[Source Systems: SAP ERP, Salesforce, PostgreSQL]
         │
         ▼ (Data Factory - Copy Activity, daily 2:00 AM)
[Fabric Lakehouse - Files/landing/ (Bronze CSVs)]
         │
         ▼ (Fabric Notebook - Bronze → Silver, MERGE pattern)
[Fabric Lakehouse - Tables/silver_orders (Delta)]
         │
         ▼ (Fabric Notebook - Silver → Gold, aggregations)
[Fabric Lakehouse - Tables/gold_daily_revenue (Delta)]
         │
         ▼ (Automatic - Direct Lake, no copy)
[Fabric Semantic Model - "SalesSemanticModel"]
   • Relationships: fact_sales ↔ dim_customer, dim_date
   • Measures: Total Revenue, MoM Growth, Avg Order Value
   • RLS: Regional managers see only their region
         │
         ▼
[Power BI Reports & Dashboards]
   • "Executive Dashboard" → CEO + Board (read-only via Viewer role)
   • "Operations Report"   → Ops Managers (RegionViewer RLS role)
   • "Finance Report"      → Finance team (full data, OLS hides cost columns from others)

[Governance: Microsoft Purview]
   • Sensitivity label: "Internal" on Semantic Model
   • Data Lineage: tracks SAP → Lake → Semantic Model → Report
   • DLP: blocks export of "Confidential" tables to personal devices
```

---

### ✅ Key Takeaways

1. **Direct Lake** = Power BI reads Lakehouse Delta files directly — no import, no staleness, near in-memory speed. This is Fabric's superpower over standalone Power BI Premium.
2. **Semantic Model** = the governed, shared data layer. One model serves many reports. Build it once, reuse everywhere.
3. **DAX** = the formula language for business logic. Keep it simple — heavy computation belongs in Spark, not DAX.
4. **RLS** = one report, personalized per user's identity. Use a user-region mapping table for dynamic, scalable RLS.
5. **OLS** = hide entire columns/tables from specific roles. Complement to RLS.
6. **Deployment Pipelines** = safe DEV → TEST → PROD promotion with visual diffs. Deployment Rules swap data source configs automatically per stage.
7. **Star schema always** = one fact table + multiple dimension tables. Avoid snowflake and many-to-many relationships.
8. **Purview integration** = automatic lineage from source to report. Sensitivity labels flow through the entire stack.

## 🎯 Phase 7: Certification & Interview Drill

### 🛡️ DP-600 (Microsoft Fabric) Drill
*   **Direct Lake Fallback:** What happens if the data in OneLake is too large for the Power BI Capacity? 
    *   **Answer:** Power BI will "fallback" to **DirectQuery** mode. It will still work, but it will be slower. You should monitor this using the **Fabric Capacity Metrics** app.
*   **DAX Optimization:** Should you use `CALCULATE` inside a row iterator like `SUMX`?
    *   **Answer:** Avoid it if possible. `CALCULATE` performs a **Context Transition** which is expensive and can slow down your report if used on millions of rows.

### 🏢 Consultancy Scenario: "The Single Source of Truth"
**Scenario:** A client has 50 different Power BI reports, all with slightly different "Revenue" calculations. They are confused and frustrated.
*   **Architect Answer:** **The Shared Semantic Model.**
*   **The Move:** Build one "Master" **Semantic Model** that contains the official `[Total Revenue]` measure logic. Certify this model. Instruct all report builders to "connect to existing dataset" instead of building their own. This ensures that every report shows the exact same number, every time.

### 🚀 Startup Scenario: "The Self-Service Chaos"
**Scenario:** "Everyone in my startup is building their own reports, but the data is messy and I'm worried they'll leak sensitive info."
*   **Answer:** **Certified Datasets and RLS.**
*   **The Drill:** Use the **Endorsement** feature to mark your governed Semantic Model as **Certified**. This places a ribbon on it, telling users "This is the one to trust." Combine this with **Row-Level Security (RLS)** so that even if a user builds their own report, they can only see the data allowed for their email address.

### 🏛️ FAANG Scenario: "The 100GB Semantic Model"
**Scenario:** "We have a 100GB Semantic Model. How does Fabric handle this in memory when 1,000 users hit it at once?"
*   **Answer:** **Columnar Eviction.**
*   **The Drill:** Fabric's VertiPaq engine doesn't load the whole 100GB at once. It loads data **per column**. If a user only looks at "Year" and "Sales", only those two columns are loaded. If memory gets tight, Fabric uses a **Least Recently Used (LRU)** policy to "evict" (unload) unused columns from memory to make room for new ones.

---

## 🧪 Practice Exercises

### Exercise 1 — Build a Semantic Model on Titanic Gold Data (Beginner)
**Goal:** Create a Direct Lake Semantic Model from the Gold tables built in Lesson 3.

```
Prerequisites: Complete Lesson 3 Exercise 1 so you have:
  • gold_survival_summary (Pclass, Sex, survival_rate_pct, avg_fare, avg_age)
  • silver_titanic (all passenger records)

Steps:
1. Workspace → New → Semantic Model
   Name: "TitanicSemanticModel"
   Source: "LearningLakehouse"
   Include tables:
     ✅ gold_survival_summary
     ✅ silver_titanic
     ✅ (create dim_class below first)

2. Create a simple dimension table in a Notebook:
   spark.sql("""
     CREATE TABLE dim_class USING DELTA AS
     SELECT 1 AS Pclass, 'First Class'  AS class_name, 'High'   AS fare_tier UNION ALL
     SELECT 2,           'Second Class', 'Medium'                             UNION ALL
     SELECT 3,           'Third Class',  'Low'
   """)

3. In the Semantic Model → Add dim_class table
   Create relationship: gold_survival_summary[Pclass] → dim_class[Pclass] (Many-to-One)

4. Add these DAX Measures:
   Total Passengers  = SUM(gold_survival_summary[total_passengers])
   Total Survived    = SUM(gold_survival_summary[total_survived])
   Survival Rate %   = DIVIDE([Total Survived], [Total Passengers]) * 100
   Avg Fare by Class = AVERAGE(silver_titanic[Fare])

5. Create a Power BI Report from this model:
   Page 1: "Survival Overview"
   Visual 1: Clustered bar chart → X: class_name, Y: Survival Rate %, Legend: Sex
   Visual 2: Card → Total Passengers: 891
   Visual 3: Table → class_name, total_passengers, total_survived, avg_fare

   Expected insight: Female 1st class passengers had ~97% survival rate
```

---

### Exercise 2 — Implement RLS with a Mapping Table (Intermediate)
**Goal:** Build dynamic Row-Level Security so each analyst sees only their assigned passenger class.

```
Setup: Create the user-class mapping table in a Notebook:

spark.sql("""
  CREATE TABLE user_class_map USING DELTA AS
  SELECT 'analyst_first@company.com'  AS email, 1 AS allowed_class UNION ALL
  SELECT 'analyst_second@company.com',           2                  UNION ALL
  SELECT 'analyst_third@company.com',            3                  UNION ALL
  SELECT 'manager@company.com',                  1                  UNION ALL
  SELECT 'manager@company.com',                  2                  UNION ALL
  SELECT 'manager@company.com',                  3
""")
-- Note: manager gets all 3 classes — multi-row grants work with IN filter!

Steps in the Semantic Model:
1. Add user_class_map table to the model (no relationships needed)
2. Manage Roles → New Role: "ClassAnalyst"

   DAX Filter on "silver_titanic":
   [Pclass] IN
       CALCULATETABLE(
           VALUES(user_class_map[allowed_class]),
           user_class_map[email] = USERPRINCIPALNAME()
       )

   Apply the SAME filter to "gold_survival_summary"

3. Test the role:
   View As Role → "ClassAnalyst"
   Test email: analyst_first@company.com
   → Verify ONLY Pclass=1 rows appear in all visuals

   Test email: manager@company.com
   → Verify ALL classes appear (multi-row grant works!)

4. Challenge: What happens when an email is NOT in user_class_map?
   → The IN filter returns empty set → user sees NO data
   → Fix: Add a fallback row or create a "ViewAll" role with no filter
```

---

### Exercise 3 — Deployment Pipeline DEV → PROD (Advanced)
**Goal:** Promote your Titanic report from DEV to a PROD workspace safely.

```
Setup required:
1. Create two workspaces:
   "TitanicAnalytics-Dev"  (Contributor role: you)
   "TitanicAnalytics-Prod" (Viewer role: your test "business user")

2. Move all your Titanic items to "TitanicAnalytics-Dev":
   LearningLakehouse → TitanicLakehouse-Dev (create new, re-run notebooks)
   TitanicSemanticModel
   Titanic Power BI Report

3. Workspace → Deployment Pipelines → New Pipeline
   Name: "TitanicAnalyticsPipeline"
   Stage 1 (DEV):  "TitanicAnalytics-Dev"
   Stage 2 (PROD): "TitanicAnalytics-Prod"

4. Deploy DEV → PROD:
   Click "Deploy" → review the diff
   ✅ TitanicSemanticModel (new)
   ✅ Titanic Report (new)
   ⬜ TitanicLakehouse-Dev (stays in DEV — data layer not promoted)

5. Add a Deployment Rule:
   PROD stage → Deployment Rules
   Rule: "TitanicSemanticModel" → Data Source → TitanicLakehouse-Prod
   (Even though both point to same data now, practice setting the rule)

6. Make a small change in DEV (rename a measure or add a new visual):
   Promote again → observe the DIFF view showing only the change

7. Verify in PROD workspace:
   Switch to "TitanicAnalytics-Prod" → open the report
   → RLS should still work in PROD (roles are promoted too)
   → Report should read from TitanicLakehouse-Prod per the deployment rule
```

---

## 💼 Common Interview Questions

**Q1: What is Direct Lake mode and why is it better than Import mode?**
> **Import mode** copies data from the source into Power BI's in-memory engine (VertiPaq). Data is stale until the next scheduled refresh (up to 8 hours). **Direct Lake** is a Fabric-exclusive mode where Power BI reads Delta Lake Parquet files **directly from OneLake** — the same files that Spark notebooks write. No copy, no refresh schedule, data is always current. Performance is near in-memory because VertiPaq uses columnar reads directly from Parquet. The only requirement: the data must be in a Fabric Lakehouse as Delta tables.

**Q2: What is a Semantic Model and why should you have one shared model instead of per-report models?**
> A Semantic Model (formerly "Power BI Dataset") is the governed semantic layer — it defines measures, relationships, hierarchies, and security rules that sit between the raw data and the reports. With a **shared Semantic Model**: (1) 10 reports reuse the same `[Total Revenue]` measure — no inconsistency risk. (2) An RLS rule change propagates to all 10 reports instantly. (3) A DAX fix in one measure fixes it everywhere. Without sharing, each report duplicates logic and diverges over time — "Sales says $1.2M, Finance says $1.5M" — the classic BI alignment problem.

**Q3: Explain Row-Level Security (RLS) and how you'd implement it for 500 regional managers.**
> RLS restricts which rows a user sees when they open a report. For 500 regional managers, hardcoding emails in DAX is impractical. The scalable approach: (1) Create a **`user_region_map` table** in the Lakehouse mapping each manager's email to their authorized regions. (2) Create an RLS role with a DAX filter: `[region] IN CALCULATETABLE(VALUES(user_region_map[region]), user_region_map[email] = USERPRINCIPALNAME())`. (3) Assign the entire "Regional Managers" Azure AD security group to this role — all 500 users get their personalized view from one rule. Adding/removing an employee means only updating the mapping table.

**Q4: What is the difference between RLS and OLS?**
> **Row-Level Security (RLS)** filters which *rows* a user sees: John (Asia manager) sees only Asia rows, Carlos sees only Americas rows — same columns, different rows. **Object-Level Security (OLS)** hides entire *columns or tables* from a role: the "SalesRep" role cannot see the `cost_price` column at all — it doesn't appear in the field list and any DAX measure referencing it returns an error for that role. Use RLS for data partitioning by identity (region, department). Use OLS for classification-based column hiding (financial, PII, cost data).

**Q5: What are Deployment Rules in Fabric Deployment Pipelines and why are they critical?**
> When you promote a Semantic Model from DEV to PROD, you don't want it pointing to the DEV Lakehouse (with test data). Deployment Rules let you define **stage-specific configuration overrides**: "In PROD, this Semantic Model connects to `LakehouseProd` instead of `LakehouseDev`." Rules are also used for pipeline parameters (`env = "prod"`) and connection strings. Without Deployment Rules, every promotion would require manually re-pointing data sources — which is error-prone and defeats the purpose of automated promotion.

