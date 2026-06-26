# Lesson 3: MLflow & Feature Store (ML Integration for Data Engineers)

> **Goal:** Understand how Data Engineers support the ML lifecycle — from tracking experiments and registering models to building a Feature Store that serves real-time ML predictions.

---

## 🏗️ Phase 1: Foundations — Why Data Engineers Need to Know MLflow

### 1. The Problem Without MLflow

```
❌ Data Scientist A trains a model: accuracy = 92%. "Which data did you use? Which hyperparameters?"
   "...I think it was the April dataset. Let me check my notebook."
   
❌ Data Scientist B deploys a model to production. 3 months later it degrades. 
   "Which version is in production? Did we even log the metrics?"
   
❌ Data Engineer built a feature "avg_spend_last_30_days". Three teams compute it differently.
   → Three different models give three different predictions for the same customer.
```

**MLflow** solves all of this:
-  **Experiment Tracking** — Log every model run: data version, parameters, metrics, artifacts
-  **Model Registry** — Version models, promote through Staging → Production
-  **Feature Store** — One canonical definition of every feature, shared across ALL models

### 2. The Four Components of MLflow

| Component | What It Does | Analogy |
|----------|-------------|---------|
| **Tracking** | Log experiments, parameters, metrics, artifacts | A research lab notebook |
| **Projects** | Package ML code for reproducibility | Docker for ML code |
| **Models** | Standard model format (sklearn, TF, PyTorch all same interface) | USB-C for models |
| **Registry** | Lifecycle management (Dev → Staging → Production) | App Store for models |

---

## 🚀 Phase 2: MLflow Experiment Tracking

### 1. Logging a Training Run

```python
import mlflow
import mlflow.sklearn
from sklearn.ensemble import GradientBoostingClassifier
from sklearn.metrics import roc_auc_score, precision_score, recall_score
from sklearn.model_selection import train_test_split
import pandas as pd

# Set the experiment (creates if doesn't exist)
mlflow.set_experiment("/ML/churn_prediction")

# Load feature data (from the Feature Store or a Gold table)
df = spark.table("gold.customer_features").toPandas()
X = df.drop(columns=["will_churn", "customer_id"])
y = df["will_churn"]
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

# Training inside an MLflow run — everything is auto-logged
with mlflow.start_run(run_name="GBM_v2_April_features"):

    # 1. Log parameters (hyperparameters)
    params = {
        "n_estimators":    200,
        "max_depth":       5,
        "learning_rate":   0.05,
        "subsample":       0.8,
    }
    mlflow.log_params(params)

    # 2. Log data version (critical for reproducibility!)
    mlflow.log_param("training_data_table",   "gold.customer_features")
    mlflow.log_param("training_data_version", 14)   # Delta Lake version!
    mlflow.log_param("feature_count",         X_train.shape[1])
    mlflow.log_param("training_rows",         X_train.shape[0])

    # 3. Train the model
    model = GradientBoostingClassifier(**params)
    model.fit(X_train, y_train)

    # 4. Evaluate
    y_pred_proba = model.predict_proba(X_test)[:, 1]
    y_pred       = model.predict(X_test)

    metrics = {
        "roc_auc":   roc_auc_score(y_test, y_pred_proba),
        "precision": precision_score(y_test, y_pred),
        "recall":    recall_score(y_test, y_pred),
    }
    mlflow.log_metrics(metrics)

    # 5. Log artifacts (feature importance plot, confusion matrix)
    import matplotlib.pyplot as plt
    feat_imp = pd.Series(model.feature_importances_, index=X_train.columns)
    feat_imp.nlargest(20).plot(kind="barh")
    plt.title("Top 20 Feature Importances")
    plt.savefig("/tmp/feature_importance.png")
    mlflow.log_artifact("/tmp/feature_importance.png")

    # 6. Log the model itself (with schema for automatic validation)
    mlflow.sklearn.log_model(
        model,
        "gbm_churn_model",
        input_example=X_train.head(5),      # For schema detection
        registered_model_name="churn_prediction_gbm",   # Auto-register!
    )

    print(f"Run complete! ROC-AUC: {metrics['roc_auc']:.4f}")
```

### 2. Model Registry — Promoting Models to Production

```python
from mlflow.tracking import MlflowClient

client = MlflowClient()

# View all versions of a model
for version in client.search_model_versions("name='churn_prediction_gbm'"):
    print(f"Version: {version.version} | Stage: {version.current_stage} | Run: {version.run_id}")

# Promote version 5 to Staging (after QA review)
client.transition_model_version_stage(
    name="churn_prediction_gbm",
    version=5,
    stage="Staging",
    archive_existing_versions=False
)

# After A/B testing passes → Promote to Production
client.transition_model_version_stage(
    name="churn_prediction_gbm",
    version=5,
    stage="Production",
    archive_existing_versions=True   # Archive the old Production version
)

# Add description for audibility:
client.update_model_version(
    name="churn_prediction_gbm",
    version=5,
    description="GBM v2. ROC-AUC=0.89. Trained on April 2024 data (Delta version 14). Approved by ML team on 2024-04-20."
)
```

### 3. Serving Predictions at Scale from Databricks

```python
# Option 1: Batch Predictions on Gold table (most common for DE work)
import mlflow

# Load the Production model
model_uri = "models:/churn_prediction_gbm/Production"
loaded_model = mlflow.pyfunc.load_model(model_uri)

# Or use the spark_udf for huge-scale predictions:
predict_udf = mlflow.pyfunc.spark_udf(
    spark,
    model_uri=model_uri,
    result_type="double"
)

# Apply predictions to the entire customer base (millions of rows!)
df_customers = spark.table("gold.customer_features")
df_with_predictions = df_customers.withColumn(
    "churn_probability",
    predict_udf(*[F.col(c) for c in feature_columns])
)

# Write back to Gold — ready for the CRM team to use!
df_with_predictions.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("gold.customer_churn_predictions")

print(f"Scored {df_with_predictions.count()} customers.")
```

---

## 🏛️ Phase 3: Databricks Feature Store

### 1. Why Feature Store Matters

```
Without Feature Store:                    With Feature Store:
─────────────────────────────────         ──────────────────────────────────────
Model A computes "avg_spend_30d"         "avg_spend_30d" is defined ONCE
Model B computes "avg_spend_30d"         Both Model A and B read the same value
differently (time zones? nulls?)         → Consistent predictions
→ Different predictions for same customer

Training: uses historical features        Training: reads from feature store using
Serving: recomputes features live         point-in-time lookup (same logic!)
→ Training-serving skew!                  → Zero training-serving skew!
```

### 2. Creating and Using a Feature Store Table

```python
from databricks.feature_store import FeatureStoreClient, FeatureLookup

fs = FeatureStoreClient()

# ========================================
# STEP 1: Define and populate Feature Table
# ========================================
def compute_customer_features(df_orders: DataFrame) -> DataFrame:
    """
    Compute customer-level features from order history.
    This SAME function is used in both training AND serving.
    → No training-serving skew!
    """
    from pyspark.sql import functions as F
    from pyspark.sql.window import Window

    # Define a 30-day lookback window
    w_30d = Window.partitionBy("customer_id").orderBy("order_timestamp") \
                  .rangeBetween(-30 * 86400, 0)   # 30 days in seconds

    return (
        df_orders
        .withColumn("avg_spend_30d",   F.avg("amount").over(w_30d))
        .withColumn("order_count_30d", F.count("order_id").over(w_30d))
        .withColumn("days_since_last", F.datediff(F.current_date(), F.max("order_date").over(w_30d)))
        .groupBy("customer_id")
        .agg(
            F.last("avg_spend_30d").alias("avg_spend_30d"),
            F.last("order_count_30d").alias("order_count_30d"),
            F.last("days_since_last").alias("days_since_last"),
            F.max("order_date").alias("last_order_date")
        )
    )

# Create the Feature Table in Databricks Feature Store:
df_orders = spark.table("silver.orders")
df_features = compute_customer_features(df_orders)

fs.create_table(
    name="prod.customer_features",
    primary_keys=["customer_id"],
    timestamp_keys=["last_order_date"],    # For point-in-time lookup!
    df=df_features,
    schema=df_features.schema,
    description="Customer-level behavioral features from order history. Refreshed daily."
)

# Update the feature table daily:
fs.write_table(name="prod.customer_features", df=df_features, mode="merge")

# ========================================
# STEP 2: Use Features in Model Training
# ========================================
feature_lookups = [
    FeatureLookup(
        table_name="prod.customer_features",
        feature_names=["avg_spend_30d", "order_count_30d", "days_since_last"],
        lookup_key="customer_id",
        timestamp_lookup_key="label_date"   # Point-in-time: get features AS OF label_date
    )
]

# Training dataset automatically joins labels with features:
training_set = fs.create_training_set(
    df=df_labels,                 # Just customer_id + label (will_churn)
    feature_lookups=feature_lookups,
    label="will_churn",
    exclude_columns=["label_date"]
)
df_training = training_set.load_df()

# Train and log with the Feature Store (tracks feature dependencies automatically!):
with mlflow.start_run():
    model = GradientBoostingClassifier()
    model.fit(df_training.drop("will_churn"), df_training["will_churn"])

    fs.log_model(
        model=model,
        artifact_path="churn_model",
        flavor=mlflow.sklearn,
        training_set=training_set,     # Logs feature dependencies!
        registered_model_name="churn_prediction_gbm"
    )
```

---

### 3. Model Serving — Real-Time Inference
**Databricks Model Serving** allows you to expose a registered model as a REST API. It is **Serverless**, meaning you don't manage any servers; it scales based on traffic.
*   **The Move:** Once a model is in the "Production" stage in Unity Catalog, you click "Enable Serving". Databricks provides a URL.
*   **Data Engineer's Role:** Ensure the **Feature Store** can provide real-time lookups (Online Store) so the serving endpoint can get the latest user features in milliseconds.

---

## 🎯 Phase 4: Certification & Interview Drill

### 🛡️ Databricks Machine Learning Professional Drill
*   **MLflow Flavors:** Understand why we use "flavors" (e.g., `mlflow.sklearn`, `mlflow.pytorch`). It allows MLflow to understand the model's internal format while providing a universal `pyfunc` wrapper for deployment.
*   **Feature Store Lookup:** In the exam, remember that `create_training_set` is the function that joins your labels with the feature store. It uses **Point-in-Time Joins** to prevent data leakage.

### 🛡️ DP-600 (Microsoft Fabric) Drill
*   **Fabric ML Workload:** Microsoft Fabric has an integrated MLflow experience. For the exam, know that you can track experiments and register models in Fabric just like in Databricks, as both use the open-source MLflow standard.

### 🏢 Consultancy Scenario: "The Black Box Model"
**Scenario:** A client has a model in production, but nobody knows who trained it or what data was used.
*   **Architect Answer:** **Implement MLflow Tracking and the Model Registry.**
*   **The Move:** Set up a rule that **no model** goes to production unless it has a corresponding MLflow Run ID. This Run ID must link to a specific **Git Commit** (via Repos) and a specific **Delta Table Version**. This creates a perfect audit trail.

### 🚀 Startup Scenario: "The Buy vs. Build ML Platform"
**Scenario:** "Should we use Databricks MLflow or build our own tracking system using a SQL database?"
*   **Answer:** **Use MLflow.** 
*   **The Drill:** Building a tracking system, a model versioning UI, and a deployment engine from scratch takes months. MLflow is included for free in Databricks and is the industry standard. Focus your startup's energy on the **Product**, not the infrastructure.

### 🏛️ FAANG Scenario: "The 1-Year Reproducibility Challenge"
**Scenario:** "A regulator asks us to prove exactly how our model made a decision for a user 1 year ago. We have updated the model 20 times since then."
*   **Answer:** **MLflow + Delta Time Travel.**
*   **The Drill:** Because you logged the **Delta Table Version** in your MLflow run 1 year ago, you can use `RESTORE TABLE fact_sales TO VERSION AS OF X` to get the exact data from that day. Then, you pull the model version from the MLflow Registry. This combination is the "Gold Standard" for compliance.

---

### 🧪 Hands-on Labs
- [mlflow_tracking_lab.py](mlflow_tracking_lab.py) (A full training loop including logging and registry promotion)

---

### ✅ Key Takeaways
1. **MLflow** is the library; **Databricks** is the host. Use it for everything ML.
2. **Experiment Tracking** = Reproducibility. Log your data versions!
3. **Model Registry** = Governance. Manage the lifecycle (Staging/Production).
4. **Feature Store** = Consistency. Defined once, used everywhere.
5. **Serverless Model Serving** = Scalability. REST APIs for your models in one click.
6. **Data Engineers** build the features; **Data Scientists** build the models. Collaboration happens in the Feature Store.

[Next: Lesson 4: Databricks SQL & Photon (BI in the Lakehouse) →](../Lesson_4_Databricks_SQL_and_Photon/README.md)

---

## 🧪 Practice Exercises

### Exercise 1 — Train & Track with MLflow (Beginner)
**Goal:** Train a simple model and log it to MLflow using Autologging.

```python
import mlflow
import mlflow.sklearn
from sklearn.ensemble import RandomForestRegressor
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_squared_error
import pandas as pd

# 1. Enable Autologging (No more manual mlflow.log_param!)
mlflow.sklearn.autolog()

# 2. Prepare dummy data
df = spark.range(1000).selectExpr("id as feature1", "id*2 as target").toPandas()
X = df[['feature1']]
y = df['target']
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2)

# 3. Train inside an MLflow run
with mlflow.start_run(run_name="my_first_experiment"):
    model = RandomForestRegressor(n_estimators=10)
    model.fit(X_train, y_train)
    
    predictions = model.predict(X_test)
    rmse = mean_squared_error(y_test, predictions, squared=False)
    print(f"RMSE: {rmse}")

# 4. GO TO THE UI: Click the 'Experiments' icon in the sidebar to see your run!
```

---

### Exercise 2 — Building a Feature Table (Intermediate)
**Goal:** Create a Feature Table in Unity Catalog and use it for training.

```python
from databricks.feature_engineering import FeatureEngineeringClient
fe = FeatureEngineeringClient()

# 1. Dummy data with a primary key
data = spark.createDataFrame([
    (1, 10.5, "A"), (2, 20.1, "B"), (3, 15.0, "A")
], ["user_id", "avg_spend", "user_segment"])

# 2. Create the feature table
fe.create_table(
    name="main.default.user_features",
    primary_keys=["user_id"],
    df=data,
    schema=data.schema,
    description="Customer behavior features"
)

# 3. Read it back using the Feature Engineering SDK
feature_df = fe.read_table(name="main.default.user_features")
display(feature_df)
```

---

### Exercise 3 — Model Registry Promotion (Architect)
**Goal:** Promote a model from 'None' to 'Challenger' using the Python SDK.

```python
from mlflow.tracking import MlflowClient
client = MlflowClient()

model_name = "sales_forecast_model"
latest_version = client.get_latest_versions(model_name, stages=["None"])[0].version

# Promote to Staging (now called 'Challenger' in Unity Catalog models)
client.transition_model_version_stage(
    name=model_name,
    version=latest_version,
    stage="Staging",
    archive_existing_versions=True
)

print(f"Model {model_name} version {latest_version} promoted to Staging!")
```

---

## 💼 Common Interview Questions

**Q1: What is the benefit of MLflow Autologging?**
> Autologging automatically captures parameters (like `n_estimators`, `learning_rate`), metrics (`accuracy`, `loss`), and even the model artifact itself without you writing `mlflow.log_param` for every line. It ensures your experiments are reproducible and searchable in the UI with zero extra code.

**Q2: Why use Feature Store instead of just joining tables in a notebook?**
> (1) **Consistency**: Ensures the same feature logic is used for training and inference (prevents Training-Serving Skew). (2) **Searchability**: Data Scientists can search for existing features before building new ones. (3) **Lineage**: Databricks tracks which models use which features, making it easy to see the impact of a data change.

**Q3: Explain the 'Model Flavor' concept in MLflow.**
> A 'Flavor' is a standard format for saving models (e.g., `sklearn`, `pytorch`, `tensorflow`). Because MLflow saves models in a standard flavor, tools like Databricks Model Serving can host them as REST APIs regardless of which library was used to train them.

**Q4: How does Unity Catalog change the Model Registry?**
> In the legacy Model Registry, models were workspace-specific. In **Unity Catalog Models**, models are part of the three-tier namespace (`catalog.schema.model`). This means you can share a single model across 10 different workspaces (Dev, Test, Prod) using standard SQL-like permissions.

**Q5: What is 'Time-Travel' in Feature Store?**
> Feature Store allows you to "Point-in-Time" joins. If you are training on data from Jan 1st, the Feature Store will only provide feature values as they existed on Jan 1st, even if the user's spending habits have changed today. This is critical to prevent **Data Leakage** (using future information to predict the past).
