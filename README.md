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
  - [Events raw table](#events-raw-table)
  - [Users raw changelog table](#users-raw-changelog-table)
  - [Batch data loader logs table](#batch-data-loader-logs-table)
  - [Dates table](#dates-table)
- Table functions
  - [Events](#events)
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
### Events raw table
This is the schema of the raw data main table. It's a partitioned table by event_date, clustered by client_id, session_id and event_name.

| Column                     | Type     | Description                                                                                                | 
|----------------------------|----------|------------------------------------------------------------------------------------------------------------| 
| event_date                 | DATE     | Date of the request                                                                                        | 
| event_datetime             | DATETIME | Datetime of the request                                                                                    | 
| event_timestamp            | INT64    | Insert timestamp of the event                                                                              | 
| processing_event_timestamp | INT64    | Nameless Analytics Server-side Client Tag received event timestamp or script execution timestamp           | 
| event_origin               | STRING   | "Streaming Protocol" if hit from measurement protocol, "Website" if from browser, "Batch" if data loader   |
| job_id                     | STRING   | Job id for Measurement Protocol hits or Batch imports                                                      |
| content_length             | INT64    | Size of the message body in bytes                                                                          |
| client_id                  | STRING   | Client ID                                                                                                  |
| session_id                 | STRING   | Session ID                                                                                                 |
| event_id                   | STRING   | Event ID                                                                                                   |
| event_name                 | STRING   | Event name                                                                                                 |
| ecommerce                  | JSON     | Ecommerce object                                                                                           |
| datalayer                  | JSON     | Current dataLayer value                                                                                    |
| consent_data               | ARRAY    | Consent data parameters                                                                                    |


### Users raw changelog table
The Users raw changelog table is the export of the Google Firestore users collection. Learn more about [stream Firestore data to BigQuery](https://extensions.dev/extensions/firebase/firestore-bigquery-export).

The users_raw_latest view is useless and can be safely deleted.


### Batch data loader logs table
This is the schema of the Batch data loader logs table. It's a partitioned table by date, clustered by status.

| Column                | Type     | Description                                       |
|-----------------------|----------|---------------------------------------------------|
| date                  | DATE     | Date of the batch import                          |
| datetime              | DATETIME | Datetime of the batch import                      |
| timestamp             | INT64    | Timestamp of the batch import                     |
| job_id                | STRING   | Data loader script execution job id               |
| status                | STRING   | Data loader script execution status               |
| message               | STRING   | Data loader script execution result               |
| execution_time_micros | INT64    | Data loader script execution time in microseconds |
| rows_inserted         | INT64    | Number of rows inserted                           |
 

### Dates table
This is the schema of the Dates table. It's a partitioned table by date, clustered by month_name and day_name.

| Column             | Type   | Description                                                    |
|--------------------|--------|----------------------------------------------------------------|
| date               | DATE   | The date value                                                 |
| year               | INT64  | Year extracted from the date                                   |
| quarter            | INT64  | Quarter of the year (1-4) extracted from the date              |
| month_number       | INT64  | Month number of the year (1-12) extracted from the date        |
| month_name         | STRING | Full name of the month (e.g., January) extracted from the date |
| week_number_sunday | INT64  | Week number of the year, starting on Sunday                    |
| week_number_monday | INT64  | Week number of the year, starting on Monday                    |
| day_number         | INT64  | Day number of the month (1-31)                                 |
| day_name           | STRING | Full name of the day of the week (e.g., Monday)                |
| day_of_week_number | INT64  | Day of the week number (1 for Sunday, 7 for Saturday)          |
| is_weekend         | BOOL   | True if the day is Saturday or Sunday                          |



## Table functions
### Users raw latest
This is the schema of the Users raw latest table function.


### Users
This is the schema of the Users table function.


### Sessions
This is the schema of the Sessions table function.


### Pages
This is the schema of the Pages table function.


### Transactions
This is the schema of the Transaction table function.


### Products
This is the schema of the Products table function.


### Shopping stages open funnel
This is the schema of the Shopping stages open funnel table function.


### Shopping stages closed funnel
This is the schema of the Shopping stages closed funnel table function.


### GTM performances
This is the schema of the GTM performances table function.


### Consents
This is the schema of the Consents table function.



## Create tables



## Create table functions


---

Reach me at: [Email](mailto:hello@tommasomoretti.com) | [Website](https://tommasomoretti.com/?utm_source=github.com&utm_medium=referral&utm_campaign=nameless_analytics) | [Twitter](https://twitter.com/tommoretti88) | [Linkedin](https://www.linkedin.com/in/tommasomoretti/)
