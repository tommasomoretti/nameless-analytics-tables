![Na logo beta](https://github.com/tommasomoretti/nameless-analytics/assets/29273232/7d4ded5e-4b79-46a2-b089-03997724fd10)

---

# Nameless Analytics tables
The Nameless Analytics Tables is a set of tables in BigQuery where [Nameless_Analytics Server-side client tag](https://github.com/tommasomoretti/nameless-analytics-server-side-client-tag) or [Nameless_Analytics Data Loader](https://github.com/tommasomoretti/nameless-analytics-data-loader) inserts event data.

For an overview of how Nameless Analytics works [start from here](https://github.com/tommasomoretti/nameless-analytics).

Start from here:
- [Main table schema](#main-table-schema)
- [Dates table schema](#dates-table-schema)
- [Data Loader logs table](#data-loader-logs-table)
- [Create main tables](#create-main-tables)



## Main table schema

This is the schema of the raw data main table. Create it manually before starting stream events. 

It's a partitioned table by event_date, clustered by client_id, session_id and event_name.  

| name                    | mode     | type    | description                             |
|-------------------------|----------|---------|-----------------------------------------|
| event_date              | NULLABLE | DATE    | Date of the request                     |
| client_id               | NULLABLE | STRING  | Client id of the user                   |
| session_id              | NULLABLE | STRING  | Session id of the user                  |
| event_name              | NULLABLE | STRING  | Event name of the request               |
| event_timestamp         | NULLABLE | INTEGER | Insert timestamp of the event           |
| event_data              | REPEATED | RECORD  | Event data                              |
| event_data.name         | NULLABLE | STRING  | Event data parameter name               |
| event_data.value        | NULLABLE | RECORD  | Event data parameter value name         |
| event_data.value.string | NULLABLE | STRING  | Event data parameter string value       |
| event_data.value.int    | NULLABLE | INTEGER | Event data parameter int number value   |
| event_data.value.float  | NULLABLE | FLOAT   | Event data parameter float number value |
| event_data.value.json   | NULLABLE | JSON    | Event data parameter JSON value         |
| consent_data            | REPEATED | RECORD  | Consent data                            |
| consent_data.name       | NULLABLE | STRING  | Consent data parameter name             |
| consent_data.value      | NULLABLE | BOOLEAN | Consent data parameter boolean value    |

To create the main table and the default views, see [Create main tables](#create-main-tables)



## Dates table schema



## Data Loader logs table



## Create main tables

```sql
# NAMELESS ANALYTICS

declare project_name string default 'tom-moretti';  -- Change this
declare dataset_name string default 'nameless_analytics'; -- Change this
declare dataset_location string default 'EU'; -- Change this

# Tables
declare main_table_name string default 'events'; -- Change this
declare dates_table_name string default 'dates';
declare data_loader_logs_table_name string default 'data_loader_logs';

declare main_dataset_path string default CONCAT('`', project_name, '.', dataset_name, '`');
declare main_table_path string default CONCAT('`', project_name, '.', dataset_name, '.', main_table_name,'`');
declare dates_table_path string default CONCAT('`', project_name, '.', dataset_name, '.', dates_table_name,'`');
declare data_loader_logs_table_path string default CONCAT('`', project_name, '.', dataset_name, '.', data_loader_logs_table_name,'`');


# -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------


# Main dataset
declare main_dataset_sql string default format(
  """
    create schema if not exists %s
    options (
      -- default_kms_key_name = 'KMS_KEY_NAME',
      -- default_partition_expiration_days = PARTITION_EXPIRATION,
      -- default_table_expiration_days = TABLE_EXPIRATION,
      -- max_time_travel_hours = HOURS, // default 168 hours => 7 days 
      -- storage_billing_model = BILLING_MODEL // Phytical or logical (default)  
      description = 'Nameless Analytics',
      location = '%s'
    );
  """
, main_dataset_path, dataset_location);


# Main table
declare main_table_sql string default format(
  """
    create table if not exists %s (
      event_date DATE NOT NULL OPTIONS (description = 'Date of the request'),
      event_datetime DATETIME OPTIONS (description = 'Datetime of the request'),
      event_timestamp INT64 NOT NULL OPTIONS (description = 'Insert timestamp of the event'),
      processing_event_timestamp INT64 OPTIONS (description = ' Nameless Analytics Server-side Client Tag received event timestamp when hits are sent from a website or a Measurement Protocol request. Script start execution timestamp if hits are imported by Nameless Analytics Data Loader.'),
      event_origin STRING NOT NULL OPTIONS (description = '"Measurement Protocol" if the hit comes from measurement protocol, "Website" if the hit comes from browser, "Batch" if the hit comes from data_loader script'),
      job_id STRING OPTIONS (description = 'Job id for Measurement Protocol hits or Batch imports'),
      content_length INT64 OPTIONS (description = 'Size of the message body, in bytes'),
      client_id STRING NOT NULL OPTIONS (description = 'Client id of the user'),
      user_id STRING OPTIONS (description = 'User id of the user'),
      session_id STRING NOT NULL OPTIONS (description = 'Session id of the user'),
      event_name STRING NOT NULL OPTIONS (description = 'Event name of the request'),
      event_data ARRAY<
        STRUCT<
          name STRING OPTIONS (description = 'Event data parameter name'),
          value STRUCT<
            string STRING OPTIONS (description = 'Event data parameter string value'),
            int INT64 OPTIONS (description = 'Event data parameter int number value'),
            float FLOAT64 OPTIONS (description = 'Event data parameter float number value'),
            json JSON OPTIONS (description = 'Event data parameter JSON value')
          > OPTIONS (description = 'Event data parameter value name')
        >
      > OPTIONS (description = 'Event data'),
      consent_data ARRAY<
        STRUCT<
          name STRING OPTIONS (description = 'Consent data parameter name'),
          value STRUCT<
            string STRING OPTIONS (description = 'Consent data parameter string value')
          > OPTIONS (description = 'Consent data parameter value name')
        >
      > OPTIONS (description = 'Consent data'),
    datalayer JSON OPTIONS (description = 'Current dataLayer value')
    )
    PARTITION BY event_date
    CLUSTER BY client_id, session_id, event_name
    OPTIONS (description = 'Nameless Analytics | Main table');
  """
, main_table_path);


# Dates table
declare dates_table_sql string default FORMAT(
  """
    create table if not exists %s (
      date DATE NOT NULL OPTIONS(description = "The date value"),
      year INT64 OPTIONS(description = "Year extracted from the date"),
      quarter INT64 OPTIONS(description = "Quarter of the year (1-4) extracted from the date"),
      month_number INT64 OPTIONS(description = "Month number of the year (1-12) extracted from the date"),
      month_name STRING OPTIONS(description = "Full name of the month (e.g., January) extracted from the date"),
      week_number_sunday INT64 OPTIONS(description = "Week number of the year, starting on Sunday"),
      week_number_monday INT64 OPTIONS(description = "Week number of the year, starting on Monday"),  
      day_number INT64 OPTIONS(description = "Day number of the month (1-31)"),
      day_name STRING OPTIONS(description = "Full name of the day of the week (e.g., Monday)"),
      day_of_week_number INT64 OPTIONS(description = "Day of the week number (1 for Monday, 7 for Sunday)"),
      is_weekend BOOL OPTIONS(description = "True if the day is Saturday or Sunday")
    ) PARTITION BY DATE_TRUNC(date, year)
      CLUSTER BY month_name, day_name
      OPTIONS (description = 'Nameless Analytics | Dates utility table')
      AS
    (
      SELECT 
        date,
        EXTRACT(YEAR FROM date) AS year,
        EXTRACT(QUARTER FROM date) AS quarter,
        EXTRACT(MONTH FROM date) AS month_number,
        FORMAT_DATE('%%B', date) AS month_name, -- Nome completo del mese
        EXTRACT(WEEK(SUNDAY) FROM date) AS week_number_sunday, -- Numero settimana (domenica inizio)
        EXTRACT(WEEK(MONDAY) FROM date) AS week_number_monday, -- Numero settimana (lunedì inizio)
        EXTRACT(DAY FROM date) AS day_number, -- Giorno del mese
        FORMAT_DATE('%%A', date) AS day_name, -- Nome completo del giorno
        EXTRACT(DAYOFWEEK FROM date) AS day_of_week_number, -- Numero giorno della settimana (1 = domenica)
        IF(EXTRACT(DAYOFWEEK FROM date) IN (1, 7), TRUE, FALSE) AS is_weekend -- Sabato o domenica
      FROM UNNEST(GENERATE_DATE_ARRAY('1970-01-01', '2050-12-31', INTERVAL 1 DAY)) AS date
    );
  """
, dates_table_path);


# Data loader logs table
declare data_loader_logs_table_sql string default format(
  """
    create table if not exists %s (
      date DATE NOT NULL OPTIONS (description = 'Date of the batch import'),
      datetime DATETIME NOT NULL OPTIONS (description = 'Datetime of the batch import'),
      timestamp INT64 NOT NULL OPTIONS (description = 'Timestamp of the batch import'),
      job_id STRING NOT NULL OPTIONS (description = 'Data loader script execution job id'),
      status STRING NOT NULL OPTIONS (description = 'Data loader script execution status'),
      message STRING NOT NULL OPTIONS (description = 'Data loader script execution result'),
      execution_time_micros INT64 OPTIONS (description = 'Data loader script execution time'),
      rows_inserted INT64 OPTIONS (description = 'Number of rows inserted')
    ) 

    PARTITION BY date
    CLUSTER BY status
    OPTIONS (description = 'Nameless Analytics | Data loader table');
  """
, data_loader_logs_table_path);


# -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------


# Create tables 
execute immediate main_dataset_sql;
execute immediate main_table_sql;
execute immediate dates_table_sql;
execute immediate data_loader_logs_table_sql;
```
