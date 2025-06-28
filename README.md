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
  - [Events raw table](#events-raw)
  - [Users raw changelog table and Users raw latest view](#users-raw-changelog-and-users-raw-latest)
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
### Events raw
This is the schema of the raw data Main table. It's a partitioned table by event_date, clustered by client_id, session_id and event_name.

| Column                     | Type            | Description                                                                                                | 
|----------------------------|-----------------|------------------------------------------------------------------------------------------------------------| 
| event_date                 | DATE NOT NULL   | Date of the request                                                                                        | 
| event_datetime             | DATETIME        | Datetime of the request                                                                                    | 
| event_timestamp            | INT64 NOT NULL  | Insert timestamp of the event                                                                              | 
| processing_event_timestamp | INT64           | Nameless Analytics Server-side Client Tag received event timestamp or script execution timestamp           | 
| event_origin               | STRING NOT NULL | "Measurement Protocol" if hit from measurement protocol, "Website" if from browser, "Batch" if data loader |
| job_id                     | STRING          | Job id for Measurement Protocol hits or Batch imports                                                      |
| content_length             | INT64           | Size of the message body in bytes                                                                          |
| client_id                  | STRING NOT NULL | Client ID                                                                                                  |
| session_id                 | STRING NOT NULL | Session ID                                                                                                 |
| event_id                   | STRING NOT NULL | Event ID                                                                                                   |
| event_name                 | STRING NOT NULL | Event name                                                                                                 |
| ecommerce                  | JSON            | Ecommerce object                                                                                           |
| datalayer                  | JSON            | Current dataLayer value                                                                                    |
| consent_data               | ARRAY           | Consent data parameters                                                                                    |

### Users raw changelog table
The Users raw changelog table is the export of the Google Firestore users collection. Learn more about [stream Firestore data to BigQuery](https://extensions.dev/extensions/firebase/firestore-bigquery-export).

The users_raw_latest view is useless and can be safely deleted.


### Batch data loader logs
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

| Column                        | Type      | Description                            |
|-------------------------------|-----------|----------------------------------------|
| user_date                     | DATE      | Date associated with the user          |
| user_id                       | STRING    | User identifier                        |
| client_id                     | STRING    | Client identifier                      |
| user_first_session_timestamp  | TIMESTAMP | Timestamp of user's first session      |
| user_last_session_timestamp   | TIMESTAMP | Timestamp of user's last session       |
| days_from_first_to_last_visit | INT64     | Days between first and last user visit |
| days_from_first_visit         | INT64     | Days since first visit                 |
| days_from_last_visit          | INT64     | Days since last visit                  |
| user_channel_grouping         | STRING    | User channel grouping                  |
| user_source                   | STRING    | User source                            |
| user_campaign                 | STRING    | User campaign                          |
| user_campaign_id              | STRING    | Campaign ID associated with user       |
| user_device_type              | STRING    | User device type                       |
| user_country                  | STRING    | User country                           |
| user_language                 | STRING    | User language                          |
| session_date                  | DATE      | Date of the session                    |
| session_id                    | STRING    | Session identifier                     |
| session_number                | INT64     | Session number within user sessions    |
| cross_domain_session          | STRING    | Flag for cross-domain session          |
| session_start_timestamp       | TIMESTAMP | Session start timestamp                |
| session_end_timestamp         | TIMESTAMP | Session end timestamp                  |
| session_duration_sec          | INT64     | Session duration in seconds            |
| session_channel_grouping      | STRING    | Session channel grouping               |
| session_source                | STRING    | Session source                         |
| session_campaign              | STRING    | Session campaign                       |
| session_campaign_id           | STRING    | Session campaign ID                    |
| session_device_type           | STRING    | Device type used in session            |
| session_country               | STRING    | Country of the session                 |
| session_language              | STRING    | Language setting during the session    |
| session_hostname              | STRING    | Hostname during the session            |
| session_browser_name          | STRING    | Browser name used during the session   |
| session_landing_page_category | STRING    | Category of landing page               |
| session_landing_page_location | STRING    | URL of landing page                    |
| session_landing_page_title    | STRING    | Title of landing page                  |
| session_exit_page_category    | STRING    | Category of exit page                  |
| session_exit_page_location    | STRING    | URL of exit page                       |
| session_exit_page_title       | STRING    | Title of exit page                     |


### Users
This is the schema of the Users table function.

| Column                        | Type      | Description                                                                 |
|-------------------------------|-----------|-----------------------------------------------------------------------------|
| user_date                     | DATE      | Date associated with the user                                               |
| client_id                     | STRING    | Client identifier                                                           |
| user_id                       | STRING    | User identifier                                                             |
| user_channel_grouping         | STRING    | User channel grouping                                                       |
| user_source                   | STRING    | User source (top-level domain extracted)                                    |
| original_user_source          | STRING    | Original full user source                                                   |
| user_campaign                 | STRING    | User campaign                                                               |
| user_country                  | STRING    | User country                                                                |
| user_last_session_timestamp.  | TIMESTAMP | Timestamp of last session                                                   |
| user_first_session_timestamp  | TIMESTAMP | Timestamp of first session                                                  |
| user_type                     | STRING    | User type: `new_user` or `returning_user`                                   |
| new_user                      | STRING    | Flag for new user (client_id or null)                                       |
| returning_user                | STRING    | Flag for returning user (client_id or null)                                 |
| days_from_first_visit         | INT64     | Days since first visit                                                      |
| days_from_last_visit          | INT64     | Days since last visit                                                       |
| session_date                  | DATE      | Date of the session                                                         |
| session_id                    | STRING    | Session identifier                                                          |
| sessions                      | INT64     | Number of sessions                                                          |
| session_duration_sec          | FLOAT64   | Average session duration (seconds)                                          |
| first_session                 | INT64     | Flag indicating first session (1=yes, 0=no)                                 |
| engaged_session               | INT64     | Flag indicating engaged session (1=yes, 0=no)                               |
| page_view                     | INT64     | Count of page views                                                         |
| session_source                | STRING    | Session source (top-level domain extracted)                                 |
| original_session_source       | STRING    | Original full session source                                                |
| session_campaign              | STRING    | Session campaign                                                            |
| session_device_type           | STRING    | Device type of the session                                                  |
| session_country               | STRING    | Country of the session                                                      |
| session_browser_name          | STRING    | Browser name during session                                                 |
| session_language              | STRING    | Language setting during session                                             |
| cross_domain_session          | STRING    | Flag for cross-domain session                                               |
| session_landing_page_category | STRING    | Landing page category                                                       |
| session_landing_page_location | STRING    | Landing page URL                                                            |
| session_landing_page_title    | STRING    | Landing page title                                                          |
| session_exit_page_category    | STRING    | Exit page category                                                          |
| session_exit_page_location    | STRING    | Exit page URL                                                               |
| session_exit_page_title       | STRING    | Exit page title                                                             |
| session_hostname              | STRING    | Hostname during session                                                     |
| purchase                      | INT64     | Count of purchase events                                                    |
| refund                        | INT64     | Count of refund events                                                      |
| purchase_revenue              | FLOAT64   | Sum of purchase revenue                                                     |
| purchase_shipping             | FLOAT64   | Sum of purchase shipping costs                                              |
| purchase_tax                  | FLOAT64   | Sum of purchase taxes                                                       |
| refund_revenue                | FLOAT64   | Sum of refund revenue                                                       |
| refund_shipping               | FLOAT64   | Sum of refund shipping costs                                                |
| refund_tax                    | FLOAT64   | Sum of refund taxes                                                         |
| purchase_net_refund           | INT64     | Net count of purchases minus refunds                                        |
| revenue_net_refund            | FLOAT64   | Net revenue (purchase revenue minus refund revenue)                         |
| shipping_net_refund           | FLOAT64   | Net shipping cost (purchase shipping plus refund shipping)                  |
| tax_net_refund                | FLOAT64   | Net tax (purchase tax plus refund tax)                                      |
| item_quantity_purchased       | INT64     | Total quantity of items purchased                                           |
| item_quantity_refunded        | INT64     | Total quantity of items refunded                                            |
| purchase_id                   | STRING    | Purchase transaction ID                                                     |
| refund_id                     | STRING    | Refund transaction ID                                                       |
| page_view                     | INT64     | Number of page views                                                        |
| first_purchase_timestamp      | TIMESTAMP | Timestamp of first purchase                                                 |
| last_purchase_timestamp       | TIMESTAMP | Timestamp of last purchase                                                  |
| avg_purchase_value            | FLOAT64   | Average purchase value                                                      |
| avg_refund_value              | FLOAT64   | Average refund value                                                        |
| recency_score                 | INT64     | R score (1-5), recent activity                                              |
| frequency_score               | INT64     | F score (1-5), purchase frequency                                           |
| monetary_score                | INT64     | M score (1-5), purchase monetary value                                      |
| rfm_segment                   | STRING    | Combined RFM segment (concatenation of scores)                              |
| rfm_cluster                   | STRING    | RFM cluster label (e.g., High Valuable, Mid Valuable, Low Valuable, Others) |


### Sessions
This is the schema of the Sessions table function.

| Column                          | Type       | Description                                                  |
|-----------------------------    |------------|--------------------------------------------------------      |
| user_date                       | DATE       | Date associated with the user                                |
| client_id                       | STRING     | Client identifier                                            |
| user_id                         | STRING     | User identifier                                              |
| user_channel_grouping           | STRING     | User channel grouping                                        |
| user_source                     | STRING     | User source (top-level domain extracted)                     |
| original_user_source            | STRING     | Original full user source                                    |
| user_campaign                   | STRING     | User campaign                                                |
| user_device_type                | STRING     | User device type                                             |
| user_country                    | STRING     | User country                                                 |
| user_language                   | STRING     | User language                                                |
| user_type                       | STRING     | User type (`new_user` or `returning_user`)                   |
| new_user                        | STRING     | Flag for new user (client_id or null)                        |
| returning_user                  | STRING     | Flag for returning user (client_id or null)                  |
| session_date                    | DATE       | Date of the session                                          |
| session_number                  | INT64      | Number of the session                                        |
| session_id                      | STRING     | Session identifier                                           |
| session_start                   | TIMESTAMP  | Session start timestamp                                      |
| session_duration_sec            | FLOAT64    | Average session duration in seconds                          |
| first_session                   | INT64      | Flag (1 if first session, else 0)                            |
| engaged_session                 | INT64      | Flag (1 if session engaged, else 0)                          |
| session_channel_grouping        | STRING     | Session channel grouping                                     |
| session_source                  | STRING     | Session source (top-level domain extracted)                  |
| original_session_source         | STRING     | Original full session source                                 |
| session_campaign                | STRING     | Session campaign                                             |
| session_device_type             | STRING     | Device type used in session                                  |
| session_country                 | STRING     | Country of the session                                       |
| session_browser_name            | STRING     | Browser name used in session                                 |
| session_language                | STRING     | Session language                                             |
| cross_domain_session            | STRING     | Flag for cross-domain session                                |
| session_landing_page_category   | STRING     | Landing page category                                        |
| session_landing_page_location   | STRING     | Landing page URL                                             |
| session_landing_page_title      | STRING     | Landing page title                                           |
| session_exit_page_category      | STRING     | Exit page category                                           |
| session_exit_page_location      | STRING     | Exit page URL                                                |
| session_exit_page_title         | STRING     | Exit page title                                              |
| session_hostname                | STRING     | Hostname during session                                      |
| page_view                       | INT64      | Count of page views                                          |
| view_item_list                  | INT64      | Count of view item list events                               |
| select_item                     | INT64      | Count of select item events                                  |
| view_item                       | INT64      | Count of view item events                                    |
| add_to_wishlist                 | INT64      | Count of add to wishlist events                              |
| add_to_cart                     | INT64      | Count of add to cart events                                  |
| remove_from_cart                | INT64      | Count of remove from cart events                             |
| view_cart                       | INT64      | Count of view cart events                                    |
| begin_checkout                  | INT64      | Count of begin checkout events                               |
| add_shipping_info               | INT64      | Count of add shipping info events                            |
| add_payment_info                | INT64      | Count of add payment info events                             |
| purchase                        | INT64      | Count of purchase events                                     |
| refund                          | INT64      | Count of refund events                                       |
| purchase_revenue                | FLOAT64    | Sum of purchase revenue                                      |
| purchase_shipping               | FLOAT64    | Sum of purchase shipping costs                               |
| purchase_tax                    | FLOAT64    | Sum of purchase taxes                                        |
| refund_revenue                  | FLOAT64    | Sum of refund revenue                                        |
| refund_shipping                 | FLOAT64    | Sum of refund shipping costs                                 |
| refund_tax                      | FLOAT64    | Sum of refund taxes                                          |
| purchase_net_refund             | INT64      | Net count of purchases minus refunds                         |
| revenue_net_refund              | FLOAT64    | Net revenue (purchase revenue minus refund revenue)          |
| shipping_net_refund             | FLOAT64    | Net shipping cost (purchase shipping plus refund shipping)   |
| tax_net_refund                  | FLOAT64    | Net tax (purchase tax plus refund tax)                       |
| session_ad_user_data            | INT64      | Flag (1 if ad user data consent granted, else 0)             |
| session_ad_personalization      | INT64      | Flag (1 if ad personalization consent granted, else 0)       |
| session_ad_storage              | INT64      | Flag (1 if ad storage consent granted, else 0)               |
| session_analytics_storage       | INT64      | Flag (1 if analytics storage consent granted, else 0)        |
| session_functionality_storage   | INT64      | Flag (1 if functionality storage consent granted, else 0)    |
| session_personalization_storage | INT64      | Flag (1 if personalization storage consent granted, else 0)  |
| session_security_storage        | INT64      | Flag (1 if security storage consent granted, else 0)         |
| consent_timestamp               | TIMESTAMP  | Timestamp of the consent event                               |
| consent_expressed               | STRING     | 'Yes' if consent was expressed during the session, else 'No' |


### Pages
This is the schema of the Pages table function.

| Column                        | Type      | Description                                      |
|-------------------------------|-----------|--------------------------------------------------|
| user_date                     | DATE      | Date associated with the user                    |
| client_id                     | STRING    | Client identifier                                |
| user_id                       | STRING    | User identifier                                  |
| user_type                     | STRING    | User type (e.g. new_user, returning_user)        |
| new_user                      | STRING    | Flag indicating if user is new                   |
| returning_user                | STRING    | Flag indicating if user is returning             |
| session_date                  | DATE      | Date of the session                              |
| session_number                | INT64     | Number of the session                            |
| session_id                    | STRING    | Session identifier                               |
| session_start_timestamp       | TIMESTAMP | Session start timestamp                          |
| session_channel_grouping      | STRING    | Session channel grouping                         |
| session_source                | STRING    | Session source                                   |
| original_session_source       | STRING    | Original source of the session                   |
| session_campaign              | STRING    | Session campaign                                 |
| session_landing_page_category | STRING    | Category of landing page for the session         |
| session_landing_page_location | STRING    | URL location of landing page                     |
| session_landing_page_title    | STRING    | Title of landing page                            |
| session_exit_page_category    | STRING    | Category of exit page                            |
| session_exit_page_location    | STRING    | URL location of exit page                        |
| session_exit_page_title       | STRING    | Title of exit page                               |
| session_hostname              | STRING    | Hostname during the session                      |
| session_device_type           | STRING    | Device type used during session                  |
| session_country               | STRING    | Country of session                               |
| session_language              | STRING    | Language setting during session                  |
| session_browser_name          | STRING    | Browser name used in the session                 |
| page_view_number              | INT64     | Number of the page view within the session       |
| page_id                       | STRING    | Unique page identifier                           |
| page_location                 | STRING    | URL location of the page                         |
| page_hostname                 | STRING    | Hostname of the page                             |
| page_title                    | STRING    | Title of the page                                |
| page_category                 | STRING    | Category of the page                             |
| page_load_timestamp           | TIMESTAMP | Timestamp when the page started loading          |
| page_unload_timestamp         | TIMESTAMP | Timestamp when the page was unloaded             |
| time_on_page_sec              | FLOAT64   | Average time spent on page in seconds            |
| time_to_dom_interactive       | FLOAT64   | Average time to DOM interactive in seconds       |
| page_render_time              | FLOAT64   | Average page render time in seconds              |
| time_to_dom_complete          | FLOAT64   | Average time to DOM complete in seconds          |
| page_load_time                | FLOAT64   | Average total page load time in seconds          |
| page_status_code              | INT64     | Maximum HTTP status code encountered on the page |
| total_events                  | INT64     | Total events counted on the page                 |
| page_view                     | INT64     | Total number of page views                       |


### Transactions
This is the schema of the Transaction table function.

| Column                   | Type      | Description                                                 |
|--------------------------|-----------|-------------------------------------------------------------|
| user_date                | DATE      | Date associated with the user                               |
| client_id                | STRING    | Client identifier                                           |
| user_id                  | STRING    | User identifier                                             |
| user_channel_grouping    | STRING    | User channel grouping                                       |
| user_source              | STRING    | User source (top-level domain extracted)                    |
| original_user_source     | STRING    | Original full user source                                   |
| user_campaign            | STRING    | User campaign                                               |
| user_device_type         | STRING    | User device type                                            |
| user_country             | STRING    | User country                                                |
| user_language            | STRING    | User language                                               |
| user_type                | STRING    | User type (`new_user` or `returning_user`)                  |
| new_user                 | STRING    | Flag for new user (client_id or null)                       |
| returning_user           | STRING    | Flag for returning user (client_id or null)                 |
| session_number           | INT64     | Number of the session                                       |
| session_id               | STRING    | Session identifier                                          |
| session_start_timestamp  | TIMESTAMP | Session start timestamp                                     |
| session_channel_grouping | STRING    | Session channel grouping                                    |
| session_source           | STRING    | Session source (top-level domain extracted)                 |
| original_session_source  | STRING    | Original full session source                                |
| session_campaign         | STRING    | Session campaign                                            |
| session_device_type      | STRING    | Session device type                                         |
| session_country          | STRING    | Session country                                             |
| session_browser_name     | STRING    | Browser name used in session                                |
| session_language         | STRING    | Session language                                            |
| event_date               | DATE      | Event date                                                  |
| event_name               | STRING    | Name of the event                                           |
| event_timestamp          | TIMESTAMP | Event timestamp                                             |
| transaction_id           | STRING    | Transaction identifier                                      |
| purchase                 | INT64     | Count of purchase events                                    |
| refund                   | INT64     | Count of refund events                                      |
| transaction_currency     | STRING    | Transaction currency code                                   |
| transaction_coupon       | STRING    | Coupon code used in transaction                             |
| purchase_revenue         | FLOAT64   | Total purchase revenue                                      |
| purchase_shipping        | FLOAT64   | Total shipping costs for purchases                          |
| purchase_tax             | FLOAT64   | Total tax for purchases                                     |
| refund_revenue           | FLOAT64   | Total refund revenue                                        |
| refund_shipping          | FLOAT64   | Total shipping costs for refunds                            |
| refund_tax               | FLOAT64   | Total tax for refunds                                       |
| purchase_net_refund      | INT64     | Net count of purchases minus refunds                        |
| revenue_net_refund       | FLOAT64   | Net revenue (purchase revenue minus refund revenue)         |
| shipping_net_refund      | FLOAT64   | Net shipping costs (purchase shipping plus refund shipping) |
| tax_net_refund           | FLOAT64   | Net tax (purchase tax plus refund tax)                      |


### Products
This is the schema of the Products table function.

| Column                          | Type      | Description                                          |
|---------------------------------|-----------|------------------------------------------------------|
| user_date                       | DATE      | Date associated with the user                        |
| client_id                       | STRING    | Client identifier                                    |
| user_id                         | STRING    | User identifier                                      |
| user_channel_grouping           | STRING    | User channel grouping                                |
| user_source                     | STRING    | User source (top-level domain extracted)             |
| original_user_source            | STRING    | Original full user source                            |
| user_campaign                   | STRING    | User campaign                                        |
| user_device_type                | STRING    | User device type                                     |
| user_country                    | STRING    | User country                                         |
| user_language                   | STRING    | User language                                        |
| user_type                       | STRING    | User type (`new_user` or `returning_user`)           |
| new_user                        | STRING    | Flag for new user (client_id or null)                |
| returning_user                  | STRING    | Flag for returning user (client_id or null)          |
| session_number                  | INT64     | Number of the session                                |
| session_id                      | STRING    | Session identifier                                   |
| session_start_timestamp         | TIMESTAMP | Session start timestamp                              |
| session_channel_grouping        | STRING    | Session channel grouping                             |
| session_source                  | STRING    | Session source (top-level domain extracted)          |
| original_session_source         | STRING    | Original full session source                         |
| session_campaign                | STRING    | Session campaign                                     |
| session_device_type             | STRING    | Session device type                                  |
| session_country                 | STRING    | Session country                                      |
| session_browser_name            | STRING    | Browser name used in session                         |
| session_language                | STRING    | Session language                                     |
| event_date                      | DATE      | Event date                                           |
| event_name                      | STRING    | Name of the event                                    |
| event_timestamp                 | TIMESTAMP | Event timestamp                                      |
| transaction_id                  | STRING    | Transaction identifier                               |
| list_name                       | STRING    | Name of the item list                                |
| item_list_id                    | STRING    | ID of the item list                                  |
| item_list_name                  | STRING    | Name of the item list                                |
| item_affiliation                | STRING    | Item affiliation                                     |
| item_coupon                     | STRING    | Coupon applied to item                               |
| item_discount                   | FLOAT64   | Discount applied to item                             |
| creative_name                   | STRING    | Creative name (promotion/advertisement)              |
| creative_slot                   | STRING    | Creative slot                                        |
| promotion_id                    | STRING    | Promotion ID                                         |
| promotion_name                  | STRING    | Promotion name                                       |
| item_brand                      | STRING    | Brand of the item                                    |
| item_id                         | STRING    | Item identifier                                      |
| item_name                       | STRING    | Item name                                            |
| item_variant                    | STRING    | Item variant                                         |
| item_category                   | STRING    | Item primary category                                |
| item_category_2                 | STRING    | Item secondary category                              |
| item_category_3                 | STRING    | Item tertiary category                               |
| item_category_4                 | STRING    | Item quaternary category                             |
| item_category_5                 | STRING    | Item quinary category                                |
| view_promotion                  | INT64     | Count of 'view_promotion' events                     |
| select_promotion                | INT64     | Count of 'select_promotion' events                   |
| view_item_list                  | INT64     | Count of 'view_item_list' events                     |
| select_item                     | INT64     | Count of 'select_item' events                        |
| view_item                       | INT64     | Count of 'view_item' events                          |
| add_to_wishlist                 | INT64     | Count of 'add_to_wishlist' events                    |
| add_to_cart                     | INT64     | Count of 'add_to_cart' events                        |
| remove_from_cart                | INT64     | Count of 'remove_from_cart' events                   |
| view_cart                       | INT64     | Count of 'view_cart' events                          |
| begin_checkout                  | INT64     | Count of 'begin_checkout' events                     |
| add_shipping_info               | INT64     | Count of 'add_shipping_info' events                  |
| add_payment_info                | INT64     | Count of 'add_payment_info' events                   |
| item_quantity_purchased         | INT64     | Quantity of items purchased                          |
| item_quantity_refunded          | INT64     | Quantity of items refunded                           |
| item_quantity_added_to_cart.    | INT64     | Quantity of items added to cart                      |
| item_quantity_removed_from_cart | INT64     | Quantity of items removed from cart                  |
| item_purchase_revenue           | FLOAT64   | Total revenue from purchased items                   |
| item_refund_revenue             | FLOAT64   | Total revenue refunded for items                     |
| item_unique_purchases           | INT64     | Count of unique purchased items                      |
| item_revenue_net_refund         | FLOAT64   | Net item revenue (purchase revenue + refund revenue) |


### Shopping stages open funnel
This is the schema of the Shopping stages open funnel table function.

| Column                    | Type   | Description                                                                                 |
|---------------------------|--------|---------------------------------------------------------------------------------------------|
| event_date                | DATE   | Date of the event                                                                           |
| client_id                 | STRING | Client identifier                                                                           |
| user_id                   | STRING | User identifier                                                                             |
| session_id                | STRING | Session identifier                                                                          |
| session_channel_grouping  | STRING | Session channel grouping                                                                    |
| original_session_source   | STRING | Original full session source                                                                |
| session_source            | STRING | Session source (top-level domain extracted from original source)                            |
| session_campaign          | STRING | Campaign associated with the session                                                        |
| session_device_type       | STRING | Device type used during the session                                                         |
| session_country           | STRING | Country of the session                                                                      |
| session_language          | STRING | Language setting during the session                                                         |
| step_name                 | STRING | Name of the shopping funnel step (e.g. "1 - View item", "6 - Purchase")                     |
| step_index                | INT64  | Numeric index of the step in the funnel                                                     |
| step_index_next_step_real | INT64  | Real next step index (null if current step is final purchase step)                          |
| step_index_next_step      | INT64  | Next step index from lead window function                                                   |
| status                    | STRING | Status of session regarding funnel (e.g. "New funnel entries", "Continuing funnel entries") |
| client_id_next_step       | STRING | Client ID for the next step session (null if none or final step)                            |
| session_id_next_step      | STRING | Session ID for the next step session (null if none or final step)                           |


### Shopping stages closed funnel
This is the schema of the Shopping stages closed funnel table function.

| Column                   | Type   | Description                                                               |
|--------------------------|--------|---------------------------------------------------------------------------|
| event_date               | DATE   | Date of the event                                                         |
| client_id                | STRING | Client identifier                                                         |
| user_id                  | STRING | User identifier                                                           |
| session_id               | STRING | Session identifier                                                        |
| session_channel_grouping | STRING | Session channel grouping                                                  |
| original_session_source. | STRING | Original full session source URL or domain                                |
| session_source           | STRING | Session source (top-level domain extracted from original source)          |
| session_campaign         | STRING | Campaign associated with the session                                      |
| session_device_type      | STRING | Device type used during the session                                       |
| session_country          | STRING | Country of the session                                                    |
| session_language         | STRING | Language setting during the session                                       |
| step_name                | STRING | Name of the funnel step (e.g. "0 - All", "1 - View item", "6 - Purchase") |
| client_id_next_step      | STRING | Client ID in the next funnel step (null if none or last step)             |
| user_id_next_step        | STRING | User ID in the next funnel step (null if none or last step)               |
| session_id_next_step     | STRING | Session ID in the next funnel step (null if none or last step)            |


### GTM performances
This is the schema of the GTM performances table function.

| Column                        | Type      | Description                                                  |
|-------------------------------|-----------|--------------------------------------------------------------|
| user_date                     | DATE      | Date associated with the user                                |
| user_id                       | STRING    | User identifier                                              |
| client_id                     | STRING    | Client identifier                                            |
| user_first_session_timestamp  | TIMESTAMP | Timestamp of the user's first session                        |
| user_last_session_timestamp   | TIMESTAMP | Timestamp of the user's last session                         |
| days_from_first_to_last_visit | INT64     | Days between first and last visit                            |
| days_from_first_visit         | INT64     | Days since first visit                                       |
| days_from_last_visit          | INT64     | Days since last visit                                        |
| user_channel_grouping         | STRING    | User channel grouping                                        |
| user_source                   | STRING    | User source                                                  |
| user_campaign                 | STRING    | User campaign                                                |
| user_device_type              | STRING    | User device type                                             |
| user_country                  | STRING    | User country                                                 |
| user_language                 | STRING    | User language                                                |
| session_date                  | DATE      | Date of the session                                          |
| session_id                    | STRING    | Session identifier                                           |
| session_number                | INT64     | Number of the session                                        |
| cross_domain_session          | STRING    | Cross-domain session flag (e.g. "Yes"/"No")                  |
| session_start_timestamp       | TIMESTAMP | Session start timestamp                                      |
| session_end_timestamp         | TIMESTAMP | Session end timestamp                                        |
| session_duration_sec          | INT64     | Session duration in seconds                                  |
| session_channel_grouping      | STRING    | Session channel grouping                                     |
| session_source                | STRING    | Session source                                               |
| session_campaign              | STRING    | Session campaign                                             |
| session_hostname              | STRING    | Hostname during session                                      |
| session_device_type           | STRING    | Device type used in session                                  |
| session_country               | STRING    | Country of session                                           |
| session_language              | STRING    | Language setting during session                              |
| session_browser_name          | STRING    | Browser name used in session                                 |
| session_landing_page_category | STRING    | Category of landing page                                     |
| session_landing_page_location | STRING    | URL of landing page                                          |
| session_landing_page_title    | STRING    | Title of landing page                                        |
| session_exit_page_category    | STRING    | Category of exit page                                        |
| session_exit_page_location    | STRING    | URL of exit page                                             |
| session_exit_page_title       | STRING    | Title of exit page                                           |
| event_date                    | DATE      | Event date                                                   |
| event_datetime                | STRING    | Event date and time in ISO format                            |
| event_timestamp               | INT64     | Event timestamp in milliseconds                              |
| processing_event_timestamp    | INT64     | Timestamp when event was processed (milliseconds)            |
| delay_in_milliseconds         | INT64     | Processing delay in milliseconds (processing - event time)   |
| delay_in_seconds              | FLOAT64   | Processing delay in seconds                                  |
| event_origin                  | STRING    | Origin of the event                                          |
| content_length                | INT64     | Content length of the event payload                          |
| cs_hostname                   | STRING    | Client-side hostname from event_data                         |
| ss_hostname                   | STRING    | Server-side hostname from event_data                         |
| cs_container_id               | STRING    | Client-side container ID from event_data                     |
| ss_container_id               | STRING    | Server-side container ID from event_data                     |
| hit_number                    | INT64     | Sequential number of hits per client_id and session_id       |
| event_name                    | STRING    | Name of the event                                            |
| event_id                      | STRING    | Unique identifier of the event                               |
| event_data                    | ARRAY<STRUCT> | Array of event data fields with string, int, float, JSON |
| ecommerce                     | JSON | Ecommerce data serialized as JSON string                          |
| dataLayer                     | JSON | dataLayer payload serialized as JSON string                       |


### Consents
This is the schema of the Consents table function.

| Column                           | Type   | Description                                                   |
|----------------------------------|--------|---------------------------------------------------------------|
| session_date                     | DATE   | Date of the session                                           |
| session_id                       | STRING | Session identifier                                            |
| session_channel_grouping         | STRING | Session channel grouping                                      |
| original_session_source          | STRING | Original full session source                                  |
| session_source                   | STRING | Session source (top-level domain extracted)                   |
| session_campaign                 | STRING | Campaign associated with the session                          |
| session_device_type              | STRING | Device type used during the session                           |
| session_country                  | STRING | Country of the session                                        |
| session_language                 | STRING | Language setting during the session                           |
| consent_state                    | STRING | 'Consent expressed' or 'Consent not expressed'                |
| session_id_consent_expressed     | STRING | Session ID if consent was expressed, else null                |
| session_id_consent_not_expressed | STRING | Session ID if consent was NOT expressed, else null            |
| consent_name                     | STRING | Name of the consent type (e.g. session_ad_user_data)          |
| consent_value_string             | STRING | 'Granted' or 'Denied' based on consent acceptance             |
| consent_value_int_accepted       | INT64  | Integer flag (1 = consent accepted, 0 = consent denied)       |
| consent_value_int_denied         | INT64  | Integer flag (1 = consent denied for specified types, else 0) |



## Create tables



## Create table functions


---

Reach me at: [Email](mailto:hello@tommasomoretti.com) | [Website](https://tommasomoretti.com/?utm_source=github.com&utm_medium=referral&utm_campaign=nameless_analytics) | [Twitter](https://twitter.com/tommoretti88) | [Linkedin](https://www.linkedin.com/in/tommasomoretti/)
