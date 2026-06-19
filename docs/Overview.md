#Project Overview
This project demonstrates an end‑to‑end Azure Data Engineering pipeline for the banking customer domain.
It leverages Azure Data Factory (ADF) for ingestion, Azure Databricks for transformations, and Azure Data Lake Storage (ADLS) for storage in a Bronze‑Silver‑Gold architecture.

## Objectives

-**Data Ingestion** → Extract raw data from SQL Server and load it into the ADLS Bronze layer using ADF pipelines.

-**Data Transformation** →

  1. Perform minor transformations in Bronze (duplicate removal, column renaming, deletion of unwanted columns).

  2. Apply major transformations in Silver (aggregations, joins, window functions) using Databricks notebooks.

-**Data Enrichment** → Store the final transformed data in the Gold layer, ready for analytics or downstream consumption.
