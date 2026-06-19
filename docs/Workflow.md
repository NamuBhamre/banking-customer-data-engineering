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
