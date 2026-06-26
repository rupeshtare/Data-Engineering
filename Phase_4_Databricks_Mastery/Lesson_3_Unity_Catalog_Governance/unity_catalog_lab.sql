-- unity_catalog_lab.sql
-- Phase 4, Lesson 3: Unity Catalog & Governance
-- Goal: Understand Data Governance, Security, and Lineage.

-- 🏗️ Phase 1: Absolute Foundations (Beginner)
-- The 3-Level Namespace: catalog.schema.table
CREATE CATALOG IF NOT EXISTS prod_catalog;
CREATE SCHEMA IF NOT EXISTS prod_catalog.sales_reporting;

CREATE TABLE prod_catalog.sales_reporting.gold_metrics (
    month_name STRING,
    total_sales DECIMAL(15,2)
);

-- 🚀 Phase 2: Intermediate (Developer)
-- Granting Permissions (The RBAC Model)
GRANT USAGE ON CATALOG prod_catalog TO `analysts_group`;
GRANT SELECT ON SCHEMA prod_catalog.sales_reporting TO `analysts_group`;

-- Masking Sensitive Data (Dynamic Data Masking)
-- CREATE FUNCTION mask_email(email STRING) ...
-- ALTER TABLE customers ALTER COLUMN email SET MASK mask_email;

-- 🏛️ Phase 3: Architect (Professional)
-- Unity Catalog provides 'End-to-End Lineage'. 
-- You can see which Dashboard used which SQL Table, and which 
-- Python job created that table. This is the heart of auditing.

-- 🏛️ Architect's Tip:
-- "Governance is not about 'locking people out', it's about making 
-- data discoverable and trustworthy. Without Unity Catalog, your 
-- Lakehouse is just a 'Data Swamp' where nobody knows who owns what."
