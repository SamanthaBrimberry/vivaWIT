-- Databricks Marketing Demo - Unity Catalog setup
-- Purpose: Create schema and a Volume for raw landing files
-- Adjust the catalog/schema below if needed.

-- Choose your catalog and schema here
USE CATALOG main;
CREATE SCHEMA IF NOT EXISTS marketing_demo;
USE SCHEMA marketing_demo;

-- Create a managed Volume for raw files (Auto Loader / DLT source)
CREATE VOLUME IF NOT EXISTS raw COMMENT 'Raw landing zone for marketing demo';

-- Optional helper objects
CREATE OR REPLACE VIEW uc_context AS
SELECT current_catalog() AS catalog_name, current_schema() AS schema_name;

-- Show what we created
SHOW VOLUMES;
SHOW TABLES;


