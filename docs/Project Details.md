# Project Overview
This project demonstrates an end‑to‑end Azure Data Engineering pipeline for the banking customer domain.  
It leverages **Azure Data Factory (ADF)** for ingestion, **Azure Databricks** for transformations, and **Azure Data Lake Storage (ADLS)** for storage in a **Bronze‑Silver‑Gold** architecture.

# Workflow

## Ingestion (Bronze Layer)
- Synthetic banking dataset ingested from SQL Server into ADLS using ADF pipelines.  
- Raw data stored in Bronze layer for traceability.

## Transformation (Silver Layer)
- Databricks notebooks clean, normalize, and join datasets.  
- Business rules applied (e.g., customer segmentation, transaction categorization).

## Enrichment (Gold Layer)
- Aggregated and curated datasets prepared for analytics.  
- Ready for reporting and downstream consumption.

# Banking Data Engineering Architecture

<img width="1244" height="571" alt="image" src="https://github.com/user-attachments/assets/7511e979-ff31-499b-a5ce-a93d2500b342" />

# Objectives
- **Data Ingestion** → Extract raw data from SQL Server and load it into the ADLS Bronze layer using ADF pipelines.  

- **Data Transformation** →  
  - Perform minor transformations in Bronze (duplicate removal, column renaming, deletion of unwanted columns).  
  - Apply major transformations in Silver (aggregations, joins, window functions) using Databricks notebooks.  

- **Data Enrichment** → Store the final transformed data in the Gold layer, ready for analytics or downstream consumption.

