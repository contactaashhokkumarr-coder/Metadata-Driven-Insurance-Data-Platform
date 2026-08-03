-- Databricks notebook source
-- MAGIC %md
-- MAGIC **insurance_metadata schema (2 tables)**

-- COMMAND ----------

SHOW CREATE TABLE dbw_insurance.insurance_metadata.bronze_config;

-- COMMAND ----------

SHOW CREATE TABLE dbw_insurance.insurance_metadata.pipeline_watermark;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **insurance_audit schema**

-- COMMAND ----------

SELECT *
FROM dbw_insurance.insurance_audit.pipeline_audit
ORDER BY end_time DESC LIMIT 20 ;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **insurance_bronze schema (5 tables)**

-- COMMAND ----------

SHOW CREATE TABLE dbw_insurance.insurance_bronze.agent;

-- COMMAND ----------

SHOW CREATE TABLE dbw_insurance.insurance_bronze.branch;


-- COMMAND ----------

SHOW CREATE TABLE dbw_insurance.insurance_bronze.claim;


-- COMMAND ----------

SHOW CREATE TABLE dbw_insurance.insurance_bronze.customer;

-- COMMAND ----------

SHOW CREATE TABLE dbw_insurance.insurance_bronze.policy;