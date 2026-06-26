# workflow_lab.py
# Phase 4, Lesson 4: Workflows & Automation
# Goal: Automate a multi-step ETL pipeline.

# 🏗️ Phase 1: Absolute Foundations (Beginner)
# A Workflow is just a series of Jobs (Notebooks, Scripts)
# Job 1: Ingest Data
# Job 2: Process/Transform
# Job 3: Output to Dashboard

def ingest_step():
    print("--- [WORKFLOW] Job 1: Reading Raw JSON from S3 ---")

def silver_step():
    print("--- [WORKFLOW] Job 2: Cleaning and Deduplicating ---")

def gold_step():
    print("--- [WORKFLOW] Job 3: Aggregating for Business Stakeholders ---")

# 🚀 Phase 2: Intermediate (Developer)
# In Databricks, workflows are defined via JSON or Terraform.
# This script simulates the logic of a 'Task Graph'.
ingest_step()
silver_step()
gold_step()

# 🏛️ Phase 3: Architect (Professional)
# Error Handling & Retries:
# What happens if 'ingest_step' fails? 
# "Job Retry: 3 attempts, 5 minute backoff."
# "Notification: Send Slack alert to #data-alerts."

# 🏛️ Architect's Tip:
# "An orphan job is a useless job. Every piece of code you write 
# should be part of an automated workflow with clear success/failure 
# triggers. A Data Engineer's value is measured by the reliability 
# of their automation."
