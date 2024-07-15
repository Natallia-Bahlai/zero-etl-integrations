-- Redshift Federated Query
CREATE EXTERNAL SCHEMA IF NOT EXISTS ext_schema_rds
FROM MYSQL DATABASE 'nba' SCHEMA 'public'
URI 'db-name.cluster.us-east-1.rds.amazonaws.com' PORT  -- this is a cluster endpoint
IAM_ROLE 'iam-role-arn'
SECRET_ARN 'secret-arn';

SELECT * FROM ext_schema_rds.feedback;
-- Query Plan ->  Remote MySQL Seq Scan ext_schema_rds.feedback

-- An incrementally maintained materialized view and auto-refresh are not supported on external tables
-- The materialized view created will be recomputed from scratch for every REFRESH
CREATE MATERIALIZED VIEW mv_fq_feedback AUTO REFRESH NO AS
SELECT * FROM ext_schema_rds.feedback;

REFRESH MATERIALIZED VIEW mv_fq_feedback; -- Full refresh/recompute
SELECT * FROM mv_fq_feedback;
-- end of Redshift Federated Query

-- Zero-ETL with Aurora MySQL
-- Database nbazetl is the destination db name created via Zero-ETL config or created via CREATE DATABASE destination_db FROM INTEGRATION 'integration_id';
-- https://docs.aws.amazon.com/redshift/latest/mgmt/zero-etl-using.creating-db.html
-- Schema nba is copied from the source MySQL database
SELECT * FROM nbazetl.nba.feedback;

-- An incrementally maintained materialized view and auto-refresh are not supported on the tables replicated via Zero-ETL
CREATE MATERIALIZED VIEW mv_zeroetl_feedback AUTO REFRESH NO AS
SELECT * FROM nbazetl.nba.feedback;

REFRESH MATERIALIZED VIEW mv_zeroetl_feedback; -- Full refresh/recompute
SELECT * FROM mv_zeroetl_feedback;
-- end of Zero-ETL with Aurora MySQL

-- Redshift Streaming e.g. data flow: DynamoDB -> Kinesis Data Streams -> Redshift
CREATE EXTERNAL SCHEMA ext_schema_kds
FROM KINESIS
IAM_ROLE 'iam-role-arn'

CREATE MATERIALIZED VIEW mv_kds_feedback_cdc_raw AUTO REFRESH YES AS
SELECT * FROM ext_schema_kds.innovate_feedback;

SELECT * FROM mv_kds_feedback_cdc_raw;

CREATE MATERIALIZED VIEW mv_kds_feedback_cdc AUTO REFRESH YES AS
SELECT approximate_arrival_timestamp,
partition_key,
shard_id,
sequence_number,
refresh_time,
JSON_PARSE(kinesis_data) as data
FROM ext_schema_kds.innovate_feedback
WHERE CAN_JSON_PARSE(kinesis_data);

SELECT * FROM mv_kds_feedback_cdc order by approximate_arrival_timestamp; --38 records

REFRESH MATERIALIZED VIEW mv_kds_feedback_cdc;
-- end of Redshift Streaming

-- Data Firehose
-- ensure to update COPY command in Data Firehose with FORMAT JSON 's3://s3-bucket-name/kinesis-schema.json'
-- {
--     "jsonpaths": [
--         "$.awsRegion",
--         "$.eventID",
--         "$.eventName",
--         "$.dynamodb",
--         "$.dynamodb.Keys.pk.S",
--         "$.dynamodb.NewImage.feedback.S"
--     ]
-- }
CREATE  TABLE feedback (
awsRegion char(100),
eventID char(100),
eventName char(100),
data SUPER,
id integer,
feedback char(100))

SELECT * FROM feedback;

CREATE MATERIALIZED VIEW mv_firehose_feedback AUTO REFRESH YES AS
SELECT * FROM feedback;

SELECT * FROM mv_firehose_feedback;

REFRESH MATERIALIZED VIEW mv_firehose_feedback;
-- end of Data Firehose

-- Redshift Spectrum to query S3 data lake
create external schema ext_schema_spectrum
from data catalog 
database 'myspectrum_db' 
IAM_ROLE 'iam-role-arn'
create external database if not exists;

create external table ext_schema_spectrum.feedback(
id integer,
feedback char(100))
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n\l'
stored as textfile
location 's3://s3-bucket-name/'
table properties ('skip.header.line.count'='1');

SELECT * FROM ext_schema_spectrum.feedback;

-- An incrementally maintained materialized view and auto-refresh are not supported with Redshift Spectrum external tables
-- The materialized view created will be recomputed from scratch for every REFRESH
CREATE MATERIALIZED VIEW mv_spectrum_feedback AUTO REFRESH NO AS
SELECT * FROM ext_schema_spectrum.feedback;

SELECT * FROM mv_spectrum_feedback;

REFRESH MATERIALIZED VIEW mv_spectrum_feedback;
-- end of Redshift Spectrum

-- Auto-mounted Data Catalog awsdatacatalog
-- Materialized views are not supported
-- This results in error: ERROR: Trying to make internal datasharing request for auto mounted catalog.
CREATE MATERIALIZED VIEW mv_datacatalog_view AS
SELECT     * FROM     "awsdatacatalog"."default"."dmscoffeedata"; 
SHOW data_catalog_auto_mount;

-- Additional information
select * from SVV_EXTERNAL_SCHEMAS;
select * from SVV_TABLE_INFO;
