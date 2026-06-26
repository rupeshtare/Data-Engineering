# Lesson 4: Data Quality & Observability (The Master Guide)

> **Goal:** Build trust in your data pipelines. Learn how to catch bad data before it hits production using automated quality checks, unit tests, and real-time observability.

---

## 🏗️ Phase 1: Absolute Foundations (For Beginners)

### 1. What is "Data Quality"?
In the software world, if code is bad, the app crashes. In the data world, if data is bad, the report keeps running but shows **wrong numbers**. This is much more dangerous.

**The Four Pillars of Data Quality:**
1. **Accuracy:** Does the data reflect reality? (e.g., Is negative revenue possible?)
2. **Completeness:** Are there missing values (NULLs) where there shouldn't be?
3. **Consistency:** Does the same user have the same ID across all tables?
4. **Timeliness:** Is the data from today, or is it 3 days old?

### 2. What is "Data Observability"?
Observability is knowing **why** something is broken, not just **that** it's broken.
- **Monitoring:** "The pipeline failed." (The What)
- **Observability:** "The pipeline failed because the upstream API changed its date format from YYYY-MM-DD to DD-MM-YYYY." (The Why)

---

## 🚀 Phase 2: Intermediate (The Developer Level)

### 1. Great Expectations (The Industry Standard)
Great Expectations (GX) is a Python library that lets you define "Expectations" (tests) for your data.

```python
import great_expectations as gx

context = gx.get_context()
datasource = context.sources.add_pandas("my_datasource")
asset = datasource.add_csv_asset("raw_sales", filepath="sales_data.csv")

# Create a suite of tests
suite = context.add_expectation_suite("sales_quality_suite")

# Define expectations
# 1. Column 'order_id' must never be NULL
asset.expect_column_values_to_not_be_null("order_id")

# 2. 'total_amount' must be greater than 0
asset.expect_column_values_to_be_between("total_amount", min_value=0)

# 3. 'region' must be one of these values
asset.expect_column_values_to_be_in_set("region", ["APAC", "EMEA", "NA", "LATAM"])

# Run the validation
checkpoint = context.add_or_update_checkpoint(
    name="my_checkpoint",
    expectation_suite_name="sales_quality_suite",
)
results = checkpoint.run()
```

### 2. Unit Testing Your Spark Logic
You should test your **logic**, not just your **data**. Use `pytest` to verify that your transformation functions work as expected.

```python
# transformations.py
def calculate_net_revenue(df):
    return df.withColumn("net_revenue", col("gross") - col("tax"))

# test_transformations.py
def test_calculate_net_revenue(spark_session):
    input_data = [(100, 10), (200, 20)]
    df = spark_session.createDataFrame(input_data, ["gross", "tax"])
    
    result_df = calculate_net_revenue(df)
    results = result_df.collect()
    
    assert results[0]["net_revenue"] == 90
    assert results[1]["net_revenue"] == 180
```

---

## 🏛️ Phase 3: Architect (The Professional Level)

### 1. Data SLAs, SLOs, and SLIs
Architects define the "contract" between the data team and the business.

| Term | Meaning | Example |
|------|---------|---------|
| **SLI** (Indicator) | What we measure | Pipeline latency / Data freshness |
| **SLO** (Objective) | The target value | "99% of data must be available by 8 AM" |
| **SLA** (Agreement) | The business contract | "If data is late more than twice a month, we meet with the CEO." |

### 2. The Circuit Breaker Pattern
If your DQ checks fail in the **Silver** layer, you should **stop** the pipeline! Don't let bad data flow into the **Gold** layer where it will infect the CEO's dashboard.

```python
# Airflow Logic
def check_data_quality(**context):
    results = run_gx_suite()
    if not results.success:
        raise Exception("DQ Check Failed! Stopping pipeline to prevent data corruption.")

dq_task = PythonOperator(task_id="dq_gatekeeper", python_callable=check_data_quality)
load_gold_task = SparkSubmitOperator(task_id="load_gold")

dq_task >> load_gold_task   # load_gold ONLY runs if dq_task succeeds
```

---

## 🎯 Phase 4: Certification & Interview Drill

### 🛡️ Databricks Associate Drill
*   **DLT Expectations:** In Delta Live Tables, you can define quality constraints directly in SQL/Python.
    ```sql
    CONSTRAINT valid_timestamp EXPECT (timestamp > '2020-01-01') ON VIOLATION DROP ROW
    ```
*   **The Drill:** What happens to rows that fail a `DROP ROW` expectation? 
    *   **Answer:** They are excluded from the target table, but the count of dropped rows is recorded in the DLT event log.

### 🏢 Consultancy Scenario: "The Missing Billions"
**Scenario:** A client's dashboard shows they lost $2 Billion yesterday. It turns out a source system started sending "null" for the price, and your Spark job treated "null" as 0.
*   **Architect Answer:** We failed because we didn't have a **Circuit Breaker**.
*   **The Fix:** Implement a "Fail-Fast" check in the Silver layer that validates no `price` columns are NULL before the aggregation runs.

### 🚀 Startup Scenario: "The Dashboard Trust"
**Scenario:** "Our users stopped using our dashboards because once a month the data is wrong, and they don't trust it anymore."
*   **Answer:** You need **Proactive Alerts**. 
*   **The Move:** Set up a Slack alert that triggers when a DQ check fails, *before* the users even open the dashboard. Tell them: "We are investigating a data delay," rather than letting them find the error.

---

## ⚠️ Common Pitfalls
1. **Testing Everything:** Trying to test 100 columns. You will spend all your time maintaining tests.
    *   **Fix:** Only test "Critical Columns" (IDs, Dates, Financials).
2. **Ignoring Warnings:** Setting DQ checks to "Warning" but never looking at the logs.
    *   **Fix:** Every warning must have an owner who checks it weekly.
3. **Hardcoding Thresholds:** Testing that "Revenue must be > $1M" when revenue naturally fluctuates.
    *   **Fix:** Use relative thresholds (e.g., "Revenue must be within 20% of the 7-day average").

---

## 🧪 Practice Exercises
1. **Write a GX Expectation:** Define an expectation that a `discount_percent` column must be between 0 and 100.
2. **The SQL DQ Audit:** Write a SQL query to find the percentage of NULL values in a `customer_email` column. If it's > 5%, the query should return a "FAIL" status.

---

## 💼 Common Interview Questions
**Q1: What is the difference between Data Testing and Data Observability?**
> Testing is checking if data matches your rules (Great Expectations). Observability is looking at the metadata (lineage, logs, cluster health) to understand the state of the whole system and find the root cause of failures.

**Q2: How do you handle "Bad Data" in a production pipeline?**
> I use the **Circuit Breaker** pattern. In the Silver layer, I run quality checks. If they fail, I either **Quarantine** the bad rows into a "Bad Data" table or **Fail the job** entirely to prevent bad data from reaching the Gold layer.

[Phase 6: Architect Mindset →](../../Phase_6_Architect_Mindset/README.md)
