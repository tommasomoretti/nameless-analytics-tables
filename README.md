<picture>
  <source srcset="https://github.com/user-attachments/assets/6af1ff70-3abe-4890-a952-900a18589590" media="(prefers-color-scheme: dark)">
  <img src="https://github.com/user-attachments/assets/9d9a4e42-cd46-452e-9ea8-2c03e0289006">
</picture>

---

# Tables
The Nameless Analytics tables is a set of tables in BigQuery where users, sessions and events data are stored.

For an overview of how Nameless Analytics works [start from here](https://github.com/tommasomoretti/nameless-analytics/).

Table of contents:
- Tables
  - [Main table](#main-table)
  - [User and sessions](#user-and-sessions)
  - [Batch data loader logs](#batch-data-loader-logs)
  - [Dates table](#dates-table)
- Table functions
  - [Users raw latest](#users-raw-latest)
  - [Users](#users)
  - [Sessions](#sessions)
  - [Pages](#pages)
  - [Transactions](#transactions)
  - [Products](#products)
  - [Shopping stages open funnel](#shopping-stages-open-funnel)
  - [Shopping stages closed funnel](#shopping-stages-closed-funnel)
  - [GTM performances](#gtm-performances)
  - [Consents](#consents)
- [Create tables](#create-tables)
- [Create table functions](#create-table-functions)



## Tables
### Main table
This is the schema of the raw data main table. It's a partitioned table by event_date, clustered by client_id, session_id and event_name.


### Main table
Lorem ipsum


### User and sessions
Lorem ipsum 


### Batch data loader logs
Lorem ipsum


### Dates table
Lorem ipsum 



## Table functions
### Users raw latest
Lorem ipsum

| Colonna                       | Descrizione                              |
|-------------------------------|----------------------------------------- |
| user_date                     | Data associata all’utente                |
| user_id                       | Identificativo utente                    |
| client_id                     | Identificativo client                    |
| user_first_session_timestamp  | Timestamp della prima sessione utente    |
| user_last_session_timestamp   | Timestamp dell’ultima sessione utente    |
| days_from_first_to_last_visit | Giorni tra prima e ultima visita         |
| days_from_first_visit         | Giorni dalla prima visita a oggi         |
| days_from_last_visit          | Giorni dall’ultima visita a oggi         |
| user_channel_grouping         | Raggruppamento canale utente             |
| user_source                   | Fonte utente                             |
| user_campaign                 | Campagna utente                          |
| user_campaign_id              | ID campagna utente                       |
| user_device_type              | Tipo dispositivo utente                  |
| user_country                  | Paese utente                             |
| user_language                 | Lingua utente                            |
| session_date                  | Data della sessione                      |
| session_id                    | ID sessione                              |
| session_number                | Numero sessione                          |
| cross_domain_session          | Flag sessione cross-domain               |
| session_start_timestamp       | Timestamp inizio sessione                |
| session_end_timestamp         | Timestamp fine sessione                  |
| session_duration_sec          | Durata sessione in secondi               |
| session_channel_grouping      | Raggruppamento canale sessione           |
| session_source                | Fonte sessione                           |
| session_campaign              | Campagna sessione                        |
| session_campaign_id           | ID campagna sessione                     |
| session_device_type           | Tipo dispositivo sessione                |
| session_country               | Paese sessione                           |
| session_language              | Lingua sessione                          |
| session_hostname              | Hostname sessione                        |
| session_browser_name          | Nome browser sessione                    |
| session_landing_page_category | Categoria pagina di atterraggio sessione |
| session_landing_page_location | URL pagina di atterraggio sessione       |
| session_landing_page_title    | Titolo pagina di atterraggio sessione    |
| session_exit_page_category    | Categoria pagina di uscita sessione      |
| session_exit_page_location    | URL pagina di uscita sessione            |
| session_exit_page_title       | Titolo pagina di uscita sessione         |


### Users
Lorem ipsum


### Sessions
Lorem ipsum


### Pages
Lorem ipsum


### Transactions
Lorem ipsum


### Products
Lorem ipsum


### Shopping stages open funnel
Lorem ipsum


### Shopping stages closed funnel
Lorem ipsum


### GTM performances
Lorem ipsum


### Consents
Lorem ipsum



## Create tables
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
      
      client_id STRING NOT NULL OPTIONS (description = 'Client ID'),
      user_data ARRAY<
        STRUCT<
          name STRING OPTIONS (description = 'User data parameter name'),
          value STRUCT<
            string STRING OPTIONS (description = 'User data parameter string value'),
            int INT64 OPTIONS (description = 'User data parameter int number value'),
            float FLOAT64 OPTIONS (description = 'User data parameter float number value'),
            json JSON OPTIONS (description = 'User data parameter JSON value')
          > OPTIONS (description = 'User data parameter value name')
        >
      > OPTIONS (description = 'User data'),

      session_id STRING NOT NULL OPTIONS (description = 'Session ID'),
      session_data ARRAY<
        STRUCT<
          name STRING OPTIONS (description = 'Session data parameter name'),
          value STRUCT<
            string STRING OPTIONS (description = 'Session data parameter string value'),
            int INT64 OPTIONS (description = 'Session data parameter int number value'),
            float FLOAT64 OPTIONS (description = 'Session data parameter float number value'),
            json JSON OPTIONS (description = 'Session data parameter JSON value')
          > OPTIONS (description = 'Session data parameter value name')
        >
      > OPTIONS (description = 'Session data'),
      
      event_id STRING NOT NULL OPTIONS (description = 'Event ID'),
      event_name STRING NOT NULL OPTIONS (description = 'Event name'),

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
      ecommerce JSON OPTIONS (description = 'Ecommerce object'),
      datalayer JSON OPTIONS (description = 'Current dataLayer value'),

      consent_data ARRAY<
        STRUCT<
          name STRING OPTIONS (description = 'Consent data parameter name'),
          value STRUCT<
            string STRING OPTIONS (description = 'Consent data parameter string value')
          > OPTIONS (description = 'Consent data parameter value name')
        >
      > OPTIONS (description = 'Consent data')
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

## Create table functions


---

Reach me at: [Email](mailto:hello@tommasomoretti.com) | [Website](https://tommasomoretti.com/?utm_source=github.com&utm_medium=referral&utm_campaign=nameless_analytics) | [Twitter](https://twitter.com/tommoretti88) | [Linkedin](https://www.linkedin.com/in/tommasomoretti/)
