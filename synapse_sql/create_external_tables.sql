-- =============================================================================
-- Synapse Serverless SQL — External tables over Gold Delta files
-- =============================================================================
-- Prereqs:
--   * A dedicated database (default: pipelinex_gold)
--   * A database-scoped credential using Managed Identity or SAS
--   * External data source pointing at gold container
-- Cost: Serverless SQL is billed on data processed (first 1 TB/month free).
-- =============================================================================

IF DB_ID('pipelinex_gold') IS NULL
    EXEC('CREATE DATABASE pipelinex_gold;');
GO
USE pipelinex_gold;
GO

-- Master key + credential (Managed Identity)
IF NOT EXISTS (SELECT 1 FROM sys.symmetric_keys WHERE name = '##MS_DatabaseMasterKey##')
    CREATE MASTER KEY ENCRYPTION BY PASSWORD = 'ChangeMeStrongP@ss123!';

IF NOT EXISTS (SELECT 1 FROM sys.database_scoped_credentials WHERE name = 'cred_lake_mi')
    CREATE DATABASE SCOPED CREDENTIAL cred_lake_mi
        WITH IDENTITY = 'Managed Identity';

-- External data source: swap <storage> for your ADLS Gen2 account name.
IF NOT EXISTS (SELECT 1 FROM sys.external_data_sources WHERE name = 'ds_gold')
    CREATE EXTERNAL DATA SOURCE ds_gold WITH (
        LOCATION   = 'abfss://gold@<storage>.dfs.core.windows.net',
        CREDENTIAL = cred_lake_mi
    );

-- File format for Delta
IF NOT EXISTS (SELECT 1 FROM sys.external_file_formats WHERE name = 'ff_delta')
    CREATE EXTERNAL FILE FORMAT ff_delta WITH (FORMAT_TYPE = DELTA);

-- =============================================================================
-- Fact + dims
-- =============================================================================

CREATE OR ALTER VIEW v_fact_sales AS
SELECT *
FROM OPENROWSET(
    BULK 'olist/fact_sales/',
    DATA_SOURCE = 'ds_gold',
    FORMAT = 'DELTA'
) AS r;
GO

CREATE OR ALTER VIEW v_dim_product AS
SELECT *
FROM OPENROWSET(
    BULK 'olist/dim_product/',
    DATA_SOURCE = 'ds_gold',
    FORMAT = 'DELTA'
) AS r;
GO

CREATE OR ALTER VIEW v_dim_customer AS
SELECT *
FROM OPENROWSET(
    BULK 'olist/dim_customer/',
    DATA_SOURCE = 'ds_gold',
    FORMAT = 'DELTA'
) AS r;
GO

CREATE OR ALTER VIEW v_dim_seller AS
SELECT *
FROM OPENROWSET(
    BULK 'olist/dim_seller/',
    DATA_SOURCE = 'ds_gold',
    FORMAT = 'DELTA'
) AS r;
GO

CREATE OR ALTER VIEW v_dim_date AS
SELECT *
FROM OPENROWSET(
    BULK 'olist/dim_date/',
    DATA_SOURCE = 'ds_gold',
    FORMAT = 'DELTA'
) AS r;
GO
