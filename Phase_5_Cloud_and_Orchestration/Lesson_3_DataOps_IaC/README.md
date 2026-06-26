# Lesson 3: DataOps & Infrastructure as Code (The Master Guide)

> **Goal:** Treat your data infrastructure and pipeline code with the same engineering rigor as software — versioned, tested, automated, and reproducible. Stop clicking buttons in cloud consoles.

---

## 🏗️ Phase 1: Absolute Foundations (For Beginners)

### 1. What is "DataOps"?

**DataOps** is the application of DevOps principles (automation, collaboration, testing) to data pipelines and infrastructure. It answers the question: "How do we deploy data pipeline changes safely and reliably?"

**The Problem Without DataOps:**
```
❌ Monday: Engineer A changes a SQL transformation on the production server.
❌ Tuesday: Engineer B overwrites A's change unknowingly.
❌ Wednesday: The pipeline fails. Nobody knows what changed or why.
❌ Thursday: 3 engineers spend the day debugging.
❌ Friday: CTO asks "why did the revenue dashboard show wrong numbers for 3 days?"
```

**With DataOps:**
```
✅ All code is in Git. Every change is a Pull Request with a code review.
✅ Every PR triggers automated tests. If tests fail → blocked from merging.
✅ Merging to main automatically deploys to production via CI/CD.
✅ If something breaks → git blame immediately shows WHO changed WHAT.
✅ Roll back in 2 minutes (git revert → auto-deploy).
```

### 2. What is Git? (The Foundation)

**Git** is a **version control system** — it tracks every change to every file in your codebase.

```bash
# Setting up a new data project with Git
mkdir my_pipeline && cd my_pipeline
git init                           # Start tracking changes

# Create your pipeline files
touch pipeline.py
touch requirements.txt
mkdir tests/

# Stage and commit (save a version)
git add .                          # Stage all files
git commit -m "feat: add daily sales ingestion pipeline"  # Save the version

# Push to GitHub (cloud backup + team collaboration)
git remote add origin https://github.com/team/pipeline.git
git push -u origin main
```

**The Golden Workflow for a Data Team:**

```bash
# ✅ GOOD - The feature branch workflow:
# 1. Create a branch for your change
git checkout -b feature/add-customer-segmentation

# 2. Make changes, write tests
vi pipeline.py
vi tests/test_pipeline.py

# 3. Commit your work
git add .
git commit -m "feat: add RFM customer segmentation to Gold layer"

# 4. Push to GitHub and open a Pull Request
git push origin feature/add-customer-segmentation
# → Open PR on GitHub → Teammate reviews → CI tests run → Merge!

# ❌ BAD - The one-person cowboy workflow:
git add . && git commit -m "fix" && git push main
# No review, no tests, no description of what changed. Dangerous!
```

### 3. What is Infrastructure as Code (IaC)?

IaC means writing **text files that describe your cloud infrastructure** instead of clicking buttons in the AWS/Azure console.

**Without IaC (clicking buttons):**
```
"To create our Databricks cluster, I click the Databricks UI,
then go to Compute → Create Cluster → set 4 workers → set Spark 13.3 → click Save"

Problem: "Can you recreate the exact same cluster in our test environment?"
Answer: "I'll try to click the same buttons... hope I don't miss any settings"
```

**With IaC (Terraform):**
```hcl
# clusters.tf — This IS the cluster definition. Version controlled. Reproducible.
resource "databricks_cluster" "sales_pipeline" {
  cluster_name  = "Sales Pipeline Cluster"
  spark_version = "13.3.x-scala2.12"
  node_type_id  = "i3.xlarge"
  num_workers   = 4

  spark_conf = {
    "spark.sql.shuffle.partitions" = "200"
    "spark.databricks.delta.preview.enabled" = "true"
  }
}
# Run "terraform apply" → cluster is created in 30 seconds, identical every time!
```

---

## 🚀 Phase 2: Intermediate (The Developer Level)

### 1. Git Workflows for Data Engineering Teams

```bash
# ============================================
# Conventional Commits — Standard Git Messages
# ============================================
# Format: <type>: <short description>
# Types:
#   feat     → New feature or pipeline
#   fix      → Bug fix
#   refactor → Code restructure (no behavior change)
#   test     → Adding or updating tests
#   docs     → Documentation changes
#   ci       → CI/CD config changes
#   chore    → Maintenance (dependency upgrades, etc.)

# Good commit messages:
git commit -m "feat: add dim_product SCD Type 2 tracking"
git commit -m "fix: handle null customer_id in silver pipeline"
git commit -m "perf: broadcast join for country_code lookup table"

# Bad commit messages:
git commit -m "fix"
git commit -m "changes"
git commit -m "asdfgh"

# ============================================
# .gitignore — What NOT to version control
# ============================================
# Create a .gitignore file in your project root:
cat > .gitignore << 'EOF'
# Python
__pycache__/
*.pyc
.venv/
.env             # ← NEVER version control your secrets!

# Databricks
.databricks/

# Local test data
data/raw/
data/test/

# Terraform state (contains sensitive data!)
*.tfstate
*.tfstate.backup
.terraform/
EOF
```

### 2. CI/CD for Data Pipelines — GitHub Actions

**CI (Continuous Integration):** Every time someone pushes code, automated tests run.
**CD (Continuous Deployment):** When tests pass on the main branch, code is automatically deployed.

```yaml
# .github/workflows/pipeline_ci.yml

name: Data Pipeline CI/CD

on:
  push:
    branches: [main]
  pull_request:
    branches: [main]

jobs:
  # ========================
  # JOB 1: Run Tests
  # ========================
  test:
    name: Run Test Suite
    runs-on: ubuntu-latest

    steps:
      - name: Checkout code
        uses: actions/checkout@v4

      - name: Set up Python 3.11
        uses: actions/setup-python@v5
        with:
          python-version: "3.11"

      - name: Install dependencies
        run: |
          pip install -r requirements.txt
          pip install pytest pyspark great-expectations

      - name: Run unit tests
        run: pytest tests/ -v --tb=short

      - name: Run data quality tests
        run: great_expectations checkpoint run daily_sales_checkpoint

  # ========================
  # JOB 2: Deploy (only on merge to main)
  # ========================
  deploy:
    name: Deploy to Production
    runs-on: ubuntu-latest
    needs: test                   # Only runs if "test" job passes!
    if: github.ref == 'refs/heads/main'   # Only on the main branch

    steps:
      - name: Checkout code
        uses: actions/checkout@v4

      - name: Deploy notebooks to Databricks
        run: |
          databricks workspace import_dir ./notebooks /Repos/pipeline/notebooks \
            --overwrite \
            --token ${{ secrets.DATABRICKS_TOKEN }} \
            --host ${{ secrets.DATABRICKS_HOST }}

      - name: Trigger Databricks job run (smoke test)
        run: |
          databricks jobs run-now --job-id 12345 \
            --token ${{ secrets.DATABRICKS_TOKEN }}
```

**Writing Testable Pipeline Code:**

```python
# tests/test_silver_pipeline.py
import pytest
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from datetime import date

@pytest.fixture(scope="session")
def spark():
    """Create a local Spark session for testing (no cluster needed!)"""
    return (
        SparkSession.builder
            .master("local[2]")
            .appName("PipelineTests")
            .config("spark.sql.shuffle.partitions", "2")  # Small for tests
            .getOrCreate()
    )

def test_silver_deduplication(spark):
    """Test that duplicate orders are removed in Silver layer."""
    # Arrange: Create test data with duplicates
    raw_data = [
        (1001, "2024-03-19", 500.00),
        (1001, "2024-03-19", 500.00),   # Duplicate!
        (1002, "2024-03-19", 250.00),
    ]
    df_raw = spark.createDataFrame(raw_data, ["order_id", "order_date", "amount"])

    # Act: Apply the deduplication transformation
    from pipeline.silver_orders import deduplicate_orders
    df_clean = deduplicate_orders(df_raw)

    # Assert: Should have 2 rows, not 3
    assert df_clean.count() == 2, f"Expected 2 records after dedup, got {df_clean.count()}"

def test_amount_validation(spark):
    """Test that negative amounts are filtered out."""
    raw_data = [
        (1001, 500.00),
        (1002, -100.00),  # Invalid!
        (1003, 0.00),     # Invalid!
        (1004, 200.00),
    ]
    df = spark.createDataFrame(raw_data, ["order_id", "amount"])

    from pipeline.silver_orders import validate_amounts
    df_valid = validate_amounts(df)

    assert df_valid.count() == 2
    assert df_valid.filter(col("amount") <= 0).count() == 0
```

---

## 🏛️ Phase 3: Architect (The Professional Level)

### 1. Terraform — Full Infrastructure as Code

```hcl
# terraform/main.tf

terraform {
  required_providers {
    databricks = {
      source  = "databricks/databricks"
      version = "~> 1.40"
    }
    azurerm = {
      source  = "hashicorp/azurerm"
      version = "~> 3.90"
    }
  }

  # Store Terraform state in Azure Blob (not locally!)
  # State file = "what currently exists in the cloud"
  backend "azurerm" {
    resource_group_name  = "terraform-state-rg"
    storage_account_name = "tfstatedataeng"
    container_name       = "tfstate"
    key                  = "databricks.tfstate"
  }
}

provider "databricks" {
  host  = var.databricks_host
  token = var.databricks_token
}

# ======================================
# Variables (customizable per environment)
# ======================================
variable "environment" {
  description = "dev | staging | prod"
  type        = string
}

variable "databricks_host" {
  description = "Databricks workspace URL"
  type        = string
  sensitive   = true
}

# ======================================
# Resources
# ======================================

# All-purpose cluster for development
resource "databricks_cluster" "dev_cluster" {
  count          = var.environment == "dev" ? 1 : 0   # Only in dev!
  cluster_name   = "${var.environment}-all-purpose"
  spark_version  = "13.3.x-scala2.12"
  node_type_id   = "Standard_DS3_v2"
  num_workers    = 2
  autotermination_minutes = 30   # Auto-kill after 30 minutes idle → saves $$$

  spark_conf = {
    "spark.databricks.delta.preview.enabled" = "true"
  }
}

# Production job cluster for the daily pipeline
resource "databricks_job" "daily_sales_pipeline" {
  name = "${var.environment}-daily-sales"

  schedule {
    quartz_cron_expression = "0 0 2 * * ?"  # 2 AM every day
    timezone_id            = "UTC"
    pause_status           = var.environment == "prod" ? "UNPAUSED" : "PAUSED"
  }

  new_cluster {
    num_workers   = var.environment == "prod" ? 8 : 2
    spark_version = "13.3.x-scala2.12"
    node_type_id  = "Standard_DS4_v2"
  }

  task {
    task_key = "ingest"
    notebook_task {
      notebook_path = "/Repos/${var.environment}/pipeline/01_bronze_ingest"
    }
  }

  task {
    task_key = "transform"
    depends_on { task_key = "ingest" }
    notebook_task {
      notebook_path = "/Repos/${var.environment}/pipeline/02_silver_transform"
    }
  }

  email_notifications {
    on_failure = ["data-team@company.com"]
  }
}

# Unity Catalog: Create schemas per environment
resource "databricks_schema" "bronze" {
  catalog_name = "main"
  name         = "${var.environment}_bronze"
  comment      = "${var.environment} Bronze layer - raw data"
}

resource "databricks_schema" "silver" {
  catalog_name = "main"
  name         = "${var.environment}_silver"
  comment      = "${var.environment} Silver layer - cleaned data"
}

resource "databricks_schema" "gold" {
  catalog_name = "main"
  name         = "${var.environment}_gold"
  comment      = "${var.environment} Gold layer - business metrics"
}
```

### 2. Terraform Modules — Reusable Blueprints

```hcl
# modules/databricks_pipeline/main.tf
# A reusable module that creates a complete pipeline job

variable "pipeline_name"   { type = string }
variable "notebook_path"   { type = string }
variable "schedule_cron"   { type = string }
variable "num_workers"     { type = number }
variable "environment"     { type = string }

resource "databricks_job" "pipeline" {
  name = "${var.environment}-${var.pipeline_name}"

  schedule {
    quartz_cron_expression = var.schedule_cron
    pause_status = var.environment == "prod" ? "UNPAUSED" : "PAUSED"
  }

  new_cluster {
    num_workers   = var.num_workers
    spark_version = "13.3.x-scala2.12"
    node_type_id  = "Standard_DS4_v2"
  }

  task {
    task_key = "run"
    notebook_task { notebook_path = var.notebook_path }
  }
}

# -----------------------------------------------
# Using the module — create 3 pipelines with 3 lines each!
# -----------------------------------------------
# terraform/pipelines.tf

module "sales_pipeline" {
  source         = "./modules/databricks_pipeline"
  pipeline_name  = "sales"
  notebook_path  = "/Repos/prod/pipeline/sales"
  schedule_cron  = "0 0 2 * * ?"
  num_workers    = 8
  environment    = "prod"
}

module "inventory_pipeline" {
  source         = "./modules/databricks_pipeline"
  pipeline_name  = "inventory"
  notebook_path  = "/Repos/prod/pipeline/inventory"
  schedule_cron  = "0 0 3 * * ?"
  num_workers    = 4
  environment    = "prod"
}

module "hr_pipeline" {
  source         = "./modules/databricks_pipeline"
  pipeline_name  = "hr"
  notebook_path  = "/Repos/prod/pipeline/hr"
  schedule_cron  = "0 0 0 * * MON"
  num_workers    = 2
  environment    = "prod"
}
```

### 3. dbt — SQL Transformations with Version Control, Tests, and Docs

**dbt (data build tool)** manages your SQL transformations like code: version controlled, tested, and self-documenting.

```yaml
# models/silver/silver_orders.sql (a dbt model)
-- dbt automatically creates this as a table or view in your warehouse

WITH source AS (
    SELECT * FROM {{ source('bronze', 'orders_raw') }}
),
cleaned AS (
    SELECT
        CAST(order_id AS INT)                AS order_id,
        CAST(amount AS DECIMAL(12,2))        AS amount,
        TO_DATE(order_date, 'yyyy-MM-dd')    AS order_date,
        UPPER(TRIM(region))                  AS region
    FROM source
    WHERE order_id IS NOT NULL
      AND amount > 0
)
SELECT * FROM cleaned
```

```yaml
# models/silver/schema.yml — Tests and documentation in one file!
models:
  - name: silver_orders
    description: "Cleaned and validated orders from all sources"
    columns:
      - name: order_id
        description: "Unique order identifier"
        tests:
          - unique           # dbt test: all values must be unique
          - not_null         # dbt test: no NULLs allowed

      - name: amount
        description: "Total order amount in INR"
        tests:
          - not_null
          - dbt_expectations.expect_column_values_to_be_between:
              min_value: 1
              max_value: 100000

      - name: region
        tests:
          - accepted_values:
              values: ['NORTH', 'SOUTH', 'EAST', 'WEST', 'CENTRAL']
```

```bash
# dbt commands:
dbt run           # Execute all models (create/replace tables in warehouse)
dbt test          # Run all data quality tests
dbt docs generate # Generate a beautiful HTML documentation site
dbt docs serve    # Open the docs in your browser
dbt build         # run + test in one command

# Deploy only the changed models (great for CI/CD!):
dbt run --select state:modified+  # Run models that changed + their dependents
```

---

### 4. Terraform State Management
The **State File** is the most important part of Terraform. It tracks exactly what is currently deployed.
*   **The Rule:** NEVER store state on your laptop. If your laptop dies, your infra is orphaned.
*   **The Fix:** Use a **Remote Backend** (S3 or Azure Blob) with **State Locking**. This allows multiple engineers to work on the same infra without corrupted states.

---

## 🎯 Phase 4: Certification & Interview Drill

### 🛡️ Terraform Associate Drill
*   **Resource vs. Data Source:**
    *   **Resource:** "I want you to CREATE this thing."
    *   **Data Source:** "I want you to LOOK UP this existing thing." 
*   **Plan vs. Apply:** Always run `terraform plan` first to see what WILL happen before running `terraform apply`.

### 🛡️ DP-600 (Microsoft Fabric) Drill
*   **Git Integration:** Fabric now supports **Git Integration**. You can sync your Fabric Workspace with a GitHub/Azure DevOps repo. 
*   **The Move:** Show the interviewer you understand that even a "SaaS" platform like Fabric must follow DataOps principles of version control and branching.

### 🏢 Consultancy Scenario: "The Missing State"
**Scenario:** A client was using Terraform, but the developer who set it up left, and nobody has the `.tfstate` file.
*   **Architect Answer:** You are in a tough spot. Terraform doesn't know what's in the cloud.
*   **The Solution:** Use `terraform import`. You have to manually map每一项 cloud resource back into your code. It's painful, but it's the only way to "regain" control without deleting everything and starting over.

### 🚀 Startup Scenario: "The $0 CI/CD"
**Scenario:** You have no budget for Jenkins or CircleCI. How do you automate your deployments?
*   **Answer:** **GitHub Actions.** 
*   **The Move:** It's free for public repos and has a generous free tier for private ones. Use it to run `pytest` and `dbt test` on every Pull Request. This ensures your startup's data quality is "Enterprise Grade" from Day 1.

### 🏛️ FAANG Scenario: "The Monorepo Scale"
**Scenario:** "We have 500 Data Engineers. Should they all work in one massive Git repo (Monorepo) or 500 separate ones?"
*   **Answer:** **Monorepo for Shared Infrastructure.**
*   **The Drill:** A Monorepo allows for shared Terraform modules and cross-team code visibility. However, you must use **CODEOWNERS** files so that a Marketing DE can't accidentally merge a change to the Finance pipeline without approval.

---

### 🧪 Hands-on Labs
- [full_infrastructure_lab.tf](full_infrastructure_lab.tf) (Creating a modular S3/ADLS setup with Terraform)

---

### ✅ Key Takeaways
1. **Git is not optional.** Every line of code must be versioned.
2. **Terraform** turns "Magic Clicking" into "Standard Engineering".
3. **Remote State** is the only safe way to use Terraform in a team.
4. **CI/CD** removes human error from the deployment process.
5. **dbt** brings software engineering best practices (tests, docs) to SQL.
6. **Code Reviews (PRs)** are the single best way to catch bugs before they reach production.

[Next Chapter: Phase 6: Architect Mindset (Design and Strategy) →](../../Phase_6_Architect_Mindset/README.md)

---

## ⚠️ Common Pitfalls (Beginner Mistakes)

1.  **Committing Secrets to Git:** Accidentally pushing `AWS_SECRET_KEY` or `DB_PASSWORD` to a public (or even private) GitHub repo.
    *   **The Issue:** Once it's in history, it's there forever. Deleting the file in a new commit doesn't remove it from the old versions.
    *   **Fix:** Use `repo-filter` to purge secrets from history and use `.gitignore` and **Secret Scanning** tools.
2.  **Manually Editing Infrastructure:** Running `terraform apply` but then going into the AWS console to "just tweak" a firewall rule manually.
    *   **The Issue:** **Configuration Drift.** The next time someone runs `terraform apply`, Terraform will see the manual change and try to "fix" it (delete it), potentially breaking production.
    *   **Fix:** If it's in Terraform, ONLY change it in Terraform.
3.  **The "Merge and Pray" Method:** Merging a PR to `main` without having a staging environment or running integration tests.
    *   **The Issue:** Your unit tests might pass, but the code fails when it tries to connect to the actual production database.
    *   **Fix:** Use a **CI/CD pipeline** that deploys to a "QA/Staging" environment first for a smoke test.
4.  **No dbt Tests:** Using dbt as just a "SQL Wrapper" without writing any `.yml` tests for `unique` or `not_null`.
    *   **The Issue:** You are using 10% of dbt's power. Without tests, you are just automating the creation of bad data.
    *   **Fix:** Every dbt model must have at least a `unique` and `not_null` test on its primary key.

---

## 🧪 Practice Exercises

### Exercise 1 — The Git Scramble (Beginner)
**Goal:** Fix a commit error.

**Scenario:** You accidentally committed a 1GB CSV file to your local git history. You haven't pushed it yet.

**Your Task:**
Identify the Git command to **undo** your last commit while keeping your file changes so you can move the CSV out of the folder and add it to `.gitignore`. (Hint: `git reset --soft HEAD~1`).

---

### Exercise 2 — Terraform Resource Logic (Intermediate)
**Goal:** Define an S3 bucket.

**Scenario:** You need to create an S3 bucket for "Bronze" data.
- Requirement 1: Versioning must be enabled.
- Requirement 2: It should be private (Block public access).

**Your Task:**
Write the basic HCL (Terraform) code for these two requirements.

---

### Exercise 3 — The CI/CD Flow (Architect)
**Goal:** Design a deployment pipeline.

**Scenario:** Your team has 3 environments: `Dev`, `UAT`, and `Production`.

**Your Task:**
Describe the "Git Flow" steps from the moment a developer finishes their code to the moment it reaches `Production`. Include when tests run and who must approve.

---

## 💼 Common Interview Questions

**Q1: What is "Configuration Drift" and why is it dangerous?**
> Configuration Drift happens when the actual state of your cloud infrastructure (AWS/Azure) stops matching the state defined in your IaC (Terraform) code. This usually happens when people make manual changes in the cloud console. It is dangerous because future automated deployments might fail or cause unexpected side effects (like deleting the manual change) that break production.

**Q2: Explain the Git "Pull Request" (PR) process and why it is critical for Data Teams.**
> A PR is a request to merge code from a feature branch into the main branch. It is critical because it: 1. Allows for **Code Review** (finding bugs/bad logic), 2. Serves as a **Documentation** of why a change was made, and 3. Triggers **Automated CI Tests** to ensure the code is safe to deploy.

**Q3: What is dbt (data build tool) and what problem does it solve?**
> dbt is a transformation framework that allows Data Engineers to write modular SQL with software engineering best practices. It solves the problem of "Spaghetti SQL" by allowing for code reuse (macros), dependency management (DAGs), automated data testing, and built-in documentation—all version-controlled in Git.

**Q4: How do you handle "Secrets" (API Keys, Passwords) in a CI/CD pipeline?**
> Secrets should **never** be in the code/Git repo. They should be stored in a secure secret manager (GitHub Secrets, AWS Secrets Manager, or Azure Key Vault). During the CI/CD run, the pipeline "injects" these secrets as environment variables so the code can use them without anyone ever seeing the actual values.

**Q5: What is the benefit of using "Immutable Infrastructure"?**
> Immutable infrastructure means that instead of "updating" an existing server or cluster, you destroy the old one and create a brand new one from your IaC code. This prevents the buildup of "junk" settings over time and ensures that your production environment exactly matches your code definition every single time.
