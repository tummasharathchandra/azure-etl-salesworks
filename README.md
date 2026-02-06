# azure-etl-salesworks
End-to-end Azure ETL pipeline using ADF, ADLS Gen2, Databricks (Unity Catalog)
# Azure ETL Pipeline: Medallion Architecture Project

## Overview

This project demonstrates an **end-to-end Azure ETL pipeline** using a modern Medallion Architecture (Bronze → Silver → Gold) on **Azure Data Lake Storage Gen2**, **Azure SQL**, **Azure Data Factory**, **Databricks Unity Catalog**, and **Power BI**.

The pipeline covers:  
- Data ingestion from **on-premise SQL Server** to **Azure SQL Database**  
- **Data movement and transformation** using Azure Data Factory and PySpark  
- Implementation of **Bronze, Silver, Gold layers** for structured ETL  
- **Delta tables** creation and governance using Unity Catalog  
- Data visualization in **Power BI**  
- Metadata tracking for **row counts and schema auditing**

---

## Project Architecture

On-Prem SQL Server
│
▼
Azure SQL DB
│
▼
ADLS Gen2 (Bronze Layer) ──> Databricks (Silver Layer: Cleaning & Standardization)
│ │
▼ ▼
ADLS Gen2 (Silver Layer) ───────────────> Databricks (Gold Layer: Business Transformations)
│ │
▼ ▼
ADLS Gen2 (Gold Layer) ───────────────> Unity Catalog Delta Tables
│
▼
Power BI Dashboards


---

## Components Used

| Component | Purpose |
|-----------|---------|
| **Azure SQL Database** | Centralized cloud relational database for staging on-prem data |
| **Azure Data Factory (ADF)** | Orchestrates ETL workflows and moves data between sources |
| **Azure Data Lake Storage Gen2 (ADLS)** | Stores raw, cleaned, and curated datasets |
| **Databricks (PySpark + Unity Catalog)** | Performs data cleaning, transformations, and Delta table management |
| **Delta Tables & Unity Catalog** | Provides managed tables with versioning and governance |
| **Power BI** | Visualizes curated business-ready datasets |
| **Metastore / Metadata Tracking** | Captures table schemas and row counts for auditing |

---

## Medallion Architecture Layers

### Bronze Layer
- Raw ingestion from Azure SQL or CSV files in ADLS
- Tables: `Product`, `Region`, `Reseller`, `Sales`, `Salesperson`, `SalespersonRegion`, `Targets`
- Purpose: **Data ingestion and raw storage**

### Silver Layer
- Data cleaning and standardization
- Dropping nulls, renaming columns, fixing date types
- Tables: `Product_Silver`, `Region_Silver`, `Reseller_Silver`, etc.
- Purpose: **Cleaned and conformed data for transformations**

### Gold Layer
- Business logic transformations:
  - Splitting Product into Brand / Additional Info  
  - Standardizing Region / Country  
  - Calculating Profit/Loss  
  - Creating FullAddress for Reseller  
  - Extracting Email Domain for Salesperson  
- Tables: `product_gold`, `region_gold`, `reseller_gold`, `sales_gold`, etc.
- Purpose: **Business-ready curated data for reporting and analytics**

---

## Key Features

- **End-to-end ETL pipeline** connecting on-premise to cloud
- **PySpark transformations** with modular notebooks (`bronze_ingestion.py`, `silver_cleaning.py`, `gold_transformation.py`)
- **Medallion architecture** implemented for data reliability
- **Unity Catalog governance**:
  - Credentials
  - External locations
  - Catalogs and schemas
- **Delta table storage** with ACID compliance
- **Metadata management**: schema info & row counts for auditing
- **Power BI integration** for visual analytics

---

## Folder Structure



databricks/
├── bronze_ingestion.py # Ingest raw data to Bronze layer
├── silver_cleaning.py # Clean and standardize data in Silver layer
└── gold_transformation.py # Business transformations & Gold layer tables

sql/
├── credentials.sql # Storage credentials setup
├── external_locations.sql # External locations setup
└── catalogs_schemas_tables.sql # Unity Catalog: catalogs, schemas, and tables


---

## How to Run

1. Setup **Azure resources**:
   - Resource group, ADLS Gen2 account, containers (Bronze/Silver/Gold), Azure SQL DB
2. Create **Databricks workspace** and **Unity Catalog metastore**
3. Create **credentials** and **external locations** using `credentials.sql` and `external_locations.sql`
4. Register **catalogs, schemas, and tables** using `catalogs_schemas_tables.sql`
5. Run notebooks in order:
   1. `bronze_ingestion.py`
   2. `silver_cleaning.py`
   3. `gold_transformation.py`
6. Connect **Power BI** to Unity Catalog Gold tables for dashboards

---

## Author

**Monty Sharath Chandra**  
Data Engineer | Azure | PySpark | SQL | Databricks | ETL | Data Lakehouse  

---

## Notes

- Replace hardcoded storage/account names with your own in `.py` scripts  
- Delta tables are **versioned** — you can query historical data if needed  
- Metadata tracking ensures **auditable and reliable pipelines**

---

