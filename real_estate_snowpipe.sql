DROP DATABASE IF EXISTS redfin_database_1;
CREATE DATABASE redfin_database_1;
-- CREATE WAREHOUSE redfin_warehouse;
CREATE SCHEMA redfin_schema;

// Create Table

TRUNCATE TABLE redfin_database_1.redfin_schema.redfin_table;
CREATE OR REPLACE TABLE redfin_database_1.redfin_schema.redfin_table (
period_duration INT,
city STRING,
state STRING,
property_type STRING,
median_sale_price FLOAT,
median_ppsf FLOAT,
homes_sold FLOAT,
inventory FLOAT,
months_of_supply FLOAT,
median_dom FLOAT,
sold_above_list FLOAT,
period_end_yr INT,
period_end_month STRING
);

SELECT *
FROM redfin_database_1.redfin_schema.redfin_table LIMIT 50;

SELECT COUNT(*) FROM redfin_database_1.redfin_schema.redfin_table;
-- DESC TABLE redfin_database.redfin_schema.redfin_table;

// Create file format object
CREATE SCHEMA file_format_schema;
CREATE OR REPLACE file format redfin_database_1.file_format_schema.format_parquet
type = 'PARQUET'
compression = 'SNAPPY';

// Create staging schema
CREATE SCHEMA external_stage_schema;
// Create staging
-- DROP STAGE redfin_database_1.external_stage_schema.redfin_ext_stage_yml;
CREATE OR REPLACE STAGE redfin_database_1.external_stage_schema.redfin_ext_stage_yml
url="s3://redfin-data-yml/redfin-transform-zone-yml/redfin_data.parquet/"
credentials=(aws_key_id='xxxxxxxx'
aws_secret_key='xxxxxx')
FILE_FORMAT = redfin_database_1.file_format_schema.format_parquet;

list @redfin_database_1.external_stage_schema.redfin_ext_stage_yml;


// Create schema for snowpipe
-- DROP SCHEMA redfin_database.snowpipe_schema;
CREATE OR REPLACE SCHEMA redfin_database_1.snowpipe_schema;

// Create Pipe

CREATE OR REPLACE PIPE redfin_database_1.snowpipe_schema.redfin_snowpipe
auto_ingest = TRUE
AS
COPY INTO redfin_database_1.redfin_schema.redfin_table
FROM @redfin_database_1.external_stage_schema.redfin_ext_stage_yml
FILE_FORMAT = (TYPE = 'PARQUET')
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE;


DESC PIPE redfin_database_1.snowpipe_schema.redfin_snowpipe;