-- =========================================================
-- Metastore: Salesworks Project
-- Purpose: Create a Unity Catalog metastore for ETL pipeline
-- =========================================================

-- Create Metastore
CREATE METASTORE IF NOT EXISTS salesworks_metastore
COMMENT 'Metastore for Salesworks Azure ETL Project'
LOCATION 'abfss://metastoredata@adlspro1.dfs.core.windows.net/'
WITH
  STORAGE_CREDENTIAL = adls_cred,   -- Reference to the credential defined in credentials.sql
  DELTA_SHARING_SCOPE = 'ACCOUNT',  -- Optional, for Delta Sharing
  PRIVILEGE_MODEL_VERSION = '1.0';  -- Unity Catalog privilege model version

-- Optional: Grant ownership to your user / admin

-- =========================================================
-- Notes:
-- 1. 'LOCATION' is your ADLS Gen2 container for storing metastore data.
-- 2. 'STORAGE_CREDENTIAL' must be defined in credentials.sql.
-- 3. Adjust DELTA_SHARING_SCOPE if you plan to share data externally.
-- 4. Privilege model version '1.0' is standard.
-- =========================================================
