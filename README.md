# Banking Customer Data Engineering Project

## Overview
This project demonstrates an end-to-end **Azure Data Engineering workflow** in the **banking customer domain**.  
Using a **self-created synthetic dataset** of ~10 CSV files, the pipeline migrates data from on-premises SQL Server to Azure, transforms it with Databricks, and organizes it into a structured **bronze-silver-gold architecture**.

## Domain
- **Industry**: Banking & Financial Services  
- **Dataset**: Synthetic banking customer dataset (demographics, transactions, account details).  
- **Use Case**: Customer segmentation, dormant account detection, and risk profiling.

## Tools & Technologies
- **Azure Data Factory (ADF)** → Pipelines with Lookup, ForEach, and Copy Data activities  
- **Azure Data Lake Storage Gen2** → Bronze, Silver, Gold layers for data organization  
- **Azure Databricks (PySpark)** → Data cleaning, transformations, window functions, aggregations  
- **SQL Server (On-Premises)** → Source system for initial dataset migration  

## Project Structure

banking-data-engineering/
- README.md                # Project overview
- notebooks/               # Databricks notebooks (bronze, silver, gold transformations)
- docs/                    # Detailed explanation + screenshots
- data/                    # Synthetic banking dataset (CSV files)
- src/                     # Helper Python scripts (ETL functions)
- configs/                 # Config templates (with placeholders, no credentials)

## Workflow
1. **Data Migration** → CSV files migrated from on-prem SQL Server to ADF.  
2. **ADF Pipeline** → Lookup, ForEach, and Copy Data activities move files into ADLS Gen2.  
3. **Bronze Layer** → Raw CSV files stored in ADLS.  
4. **Databricks Notebook 1** → Connects to ADLS, reads bronze data.  
5. **Databricks Notebook 2** → Cleans and transforms data, outputs to Silver layer.  
6. **Databricks Notebook 3** → Applies window functions & aggregations, outputs to Gold layer.  
7. **ADF Notebook Activity** → Orchestrates Databricks notebooks execution.  

## Sample Insights
- Customer distribution by demographics  
- Transaction volume trends  
- Dormant account identification  
- Aggregated customer risk scores  

## Documentation
Detailed project explanation with **screenshots and scenario description** is available in the [`/docs`](./docs) folder.

## How to Run
1. Clone this repository.  
2. Import notebooks from `/notebooks` into **Databricks Community Edition** (free signup).  
3. Upload the synthetic banking dataset from `/data` into your workspace.  
4. Run notebooks step by step to reproduce the pipeline.  

> Note: This project was originally built on a free Azure account that is no longer active.  
> All required notebooks and dataset are included in this repository for reproducibility.

---
**Author**: Namrata Ratilal Bhamre
<br>
© 2026 Namrata Ratilal Bhamre. All rights reserved.







