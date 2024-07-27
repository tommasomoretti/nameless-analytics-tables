# Nameless Analytics: Reporting queries

## Main table

This is the schema of the raw data main table. You have to create it manually before starting stream events. 

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

To create the main table and the default views, see [Create main table and wiews]()



## Views schema
This is the schema for the default views:
- Users
- Sessions
- Pages
- Ecommerce: Transactions
- Ecommerce: Products
- Ecommerce: Shopping stages - Closed funnel
- Ecommerce: Shopping stages - Open funnel


### Users view

| name                       | mode     | type      | description |
|----------------------------|----------|-----------|-------------|
| client_id                  | NULLABLE | STRING    |             |
| min_user_timestamp         | NULLABLE | TIMESTAMP |             |
| max_user_timestamp         | NULLABLE | TIMESTAMP |             |
| max_days_since_first_visit | NULLABLE | INTEGER   |             |
| max_days_from_last_visit   | NULLABLE | INTEGER   |             |
| user_channel_grouping      | NULLABLE | STRING    |             |
| user_source                | NULLABLE | STRING    |             |
| user_campaign              | NULLABLE | STRING    |             |
| new_user_id                | NULLABLE | STRING    |             |
| returning_user_id          | NULLABLE | STRING    |             |
| is_customer                | NULLABLE | STRING    |             |
| customer_type              | NULLABLE | STRING    |             |
| not_customers              | NULLABLE | INTEGER   |             |
| customers                  | NULLABLE | INTEGER   |             |
| new_customers              | NULLABLE | INTEGER   |             |
| returning_customers        | NULLABLE | INTEGER   |             |
| sessions                   | NULLABLE | INTEGER   |             |
| page_view                  | NULLABLE | INTEGER   |             |
| purchase                   | NULLABLE | INTEGER   |             |
| refund                     | NULLABLE | INTEGER   |             |
| item_quantity_purchased    | NULLABLE | INTEGER   |             |
| item_quantity_refunded     | NULLABLE | INTEGER   |             |
| purchase_revenue           | NULLABLE | FLOAT     |             |
| refund_revenue             | NULLABLE | FLOAT     |             |
| revenue_net_refund         | NULLABLE | FLOAT     |             |


### Session

| name                          | mode     | type    | description |
|-------------------------------|----------|---------|-------------|
| event_date                    | NULLABLE | DATE    |             |
| client_id                     | NULLABLE | STRING  |             |
| user_type                     | NULLABLE | STRING  |             |
| new_user                      | NULLABLE | STRING  |             |
| returning_user                | NULLABLE | STRING  |             |
| session_number                | NULLABLE | INTEGER |             |
| session_id                    | NULLABLE | STRING  |             |
| min_session_timestamp         | NULLABLE | INTEGER |             |
| new_session                   | NULLABLE | INTEGER |             |
| engaged_session               | NULLABLE | INTEGER |             |
| session_channel_grouping      | NULLABLE | STRING  |             |
| session_source                | NULLABLE | STRING  |             |
| session_campaign              | NULLABLE | STRING  |             |
| session_device_type           | NULLABLE | STRING  |             |
| session_country               | NULLABLE | STRING  |             |
| session_browser_name          | NULLABLE | STRING  |             |
| session_browser_language      | NULLABLE | STRING  |             |
| session_landing_page_location | NULLABLE | STRING  |             |
| session_landing_page_title    | NULLABLE | STRING  |             |
| session_hostname              | NULLABLE | STRING  |             |
| session_duration_sec          | NULLABLE | FLOAT   |             |
| page_view                     | NULLABLE | INTEGER |             |
| click_contact_button          | NULLABLE | INTEGER |             |
| view_item_list                | NULLABLE | INTEGER |             |
| select_item                   | NULLABLE | INTEGER |             |
| view_item                     | NULLABLE | INTEGER |             |
| add_to_wishlist               | NULLABLE | INTEGER |             |
| add_to_cart                   | NULLABLE | INTEGER |             |
| remove_from_cart              | NULLABLE | INTEGER |             |
| view_cart                     | NULLABLE | INTEGER |             |
| begin_checkout                | NULLABLE | INTEGER |             |
| add_shipping_info             | NULLABLE | INTEGER |             |
| add_payment_info              | NULLABLE | INTEGER |             |
| purchase                      | NULLABLE | INTEGER |             |
| refund                        | NULLABLE | INTEGER |             |
| purchase_revenue              | NULLABLE | FLOAT   |             |
| purchase_shipping             | NULLABLE | FLOAT   |             |
| purchase_tax                  | NULLABLE | FLOAT   |             |
| refund_revenue                | NULLABLE | FLOAT   |             |
| refund_shipping               | NULLABLE | FLOAT   |             |
| refund_tax                    | NULLABLE | FLOAT   |             |
| revenue_net_refund            | NULLABLE | FLOAT   |             |
| shipping_net_refund           | NULLABLE | FLOAT   |             |
| tax_net_refund                | NULLABLE | FLOAT   |             |


### Pages

| name                          | mode     | type    | description |
|-------------------------------|----------|---------|-------------|
| event_date                    | NULLABLE | DATE    |             |
| client_id                     | NULLABLE | STRING  |             |
| user_type                     | NULLABLE | STRING  |             |
| new_user                      | NULLABLE | STRING  |             |
| returning_user                | NULLABLE | STRING  |             |
| session_number                | NULLABLE | INTEGER |             |
| session_id                    | NULLABLE | STRING  |             |
| min_session_timestamp         | NULLABLE | INTEGER |             |
| session_channel_grouping      | NULLABLE | STRING  |             |
| session_source                | NULLABLE | STRING  |             |
| session_campaign              | NULLABLE | STRING  |             |
| session_device_type           | NULLABLE | STRING  |             |
| session_country               | NULLABLE | STRING  |             |
| session_browser_name          | NULLABLE | STRING  |             |
| session_browser_language      | NULLABLE | STRING  |             |
| session_landing_page_location | NULLABLE | STRING  |             |
| session_landing_page_title    | NULLABLE | STRING  |             |
| session_hostname              | NULLABLE | STRING  |             |
| page_view_number              | NULLABLE | INTEGER |             |
| page_id                       | NULLABLE | STRING  |             |
| page_location                 | NULLABLE | STRING  |             |
| page_hostname                 | NULLABLE | STRING  |             |
| page_title                    | NULLABLE | STRING  |             |
| content_group                 | NULLABLE | STRING  |             |
| time_on_page_sec              | NULLABLE | FLOAT   |             |
| page_view                     | NULLABLE | INTEGER |             |


### Ecommerce: Transactions

| name                          | mode     | type    | description |
|-------------------------------|----------|---------|-------------|
| event_date                    | NULLABLE | DATE    |             |
| client_id                     | NULLABLE | STRING  |             |
| user_type                     | NULLABLE | STRING  |             |
| new_user                      | NULLABLE | STRING  |             |
| returning_user                | NULLABLE | STRING  |             |
| session_number                | NULLABLE | INTEGER |             |
| session_id                    | NULLABLE | STRING  |             |
| min_session_timestamp         | NULLABLE | INTEGER |             |
| session_channel_grouping      | NULLABLE | STRING  |             |
| session_source                | NULLABLE | STRING  |             |
| session_campaign              | NULLABLE | STRING  |             |
| session_landing_page_location | NULLABLE | STRING  |             |
| session_landing_page_title    | NULLABLE | STRING  |             |
| session_device_type           | NULLABLE | STRING  |             |
| session_country               | NULLABLE | STRING  |             |
| session_browser_name          | NULLABLE | STRING  |             |
| session_browser_language      | NULLABLE | STRING  |             |
| purchase                      | NULLABLE | INTEGER |             |
| refund                        | NULLABLE | INTEGER |             |
| transaction_id                | NULLABLE | STRING  |             |
| transaction_currency          | NULLABLE | STRING  |             |
| transaction_coupon            | NULLABLE | STRING  |             |
| purchase_revenue              | NULLABLE | FLOAT   |             |
| purchase_shipping             | NULLABLE | FLOAT   |             |
| purchase_tax                  | NULLABLE | FLOAT   |             |
| refund_revenue                | NULLABLE | FLOAT   |             |
| refund_shipping               | NULLABLE | FLOAT   |             |
| refund_tax                    | NULLABLE | FLOAT   |             |
| purchase_net_refund           | NULLABLE | INTEGER |             |
| revenue_net_refund            | NULLABLE | FLOAT   |             |
| shipping_net_refund           | NULLABLE | FLOAT   |             |
| tax_net_refund                | NULLABLE | FLOAT   |             |


### Ecommerce: Products

| name                            | mode     | type    | description |
|---------------------------------|----------|---------|-------------|
| event_date                      | NULLABLE | DATE    |             |
| client_id                       | NULLABLE | STRING  |             |
| user_type                       | NULLABLE | STRING  |             |
| new_user                        | NULLABLE | STRING  |             |
| returning_user                  | NULLABLE | STRING  |             |
| session_number                  | NULLABLE | INTEGER |             |
| session_id                      | NULLABLE | STRING  |             |
| min_session_timestamp           | NULLABLE | INTEGER |             |
| session_channel_grouping        | NULLABLE | STRING  |             |
| session_source                  | NULLABLE | STRING  |             |
| session_campaign                | NULLABLE | STRING  |             |
| session_landing_page_location   | NULLABLE | STRING  |             |
| session_landing_page_title      | NULLABLE | STRING  |             |
| session_device_type             | NULLABLE | STRING  |             |
| session_country                 | NULLABLE | STRING  |             |
| session_browser_name            | NULLABLE | STRING  |             |
| session_browser_language        | NULLABLE | STRING  |             |
| purchase                        | NULLABLE | INTEGER |             |
| refund                          | NULLABLE | INTEGER |             |
| transaction_id                  | NULLABLE | STRING  |             |
| view_promotion                  | NULLABLE | INTEGER |             |
| select_promotion                | NULLABLE | INTEGER |             |
| view_item_list                  | NULLABLE | INTEGER |             |
| select_item                     | NULLABLE | INTEGER |             |
| view_item                       | NULLABLE | INTEGER |             |
| add_to_wishlist                 | NULLABLE | INTEGER |             |
| add_to_cart                     | NULLABLE | INTEGER |             |
| remove_from_cart                | NULLABLE | INTEGER |             |
| view_cart                       | NULLABLE | INTEGER |             |
| begin_checkout                  | NULLABLE | INTEGER |             |
| add_shipping_info               | NULLABLE | INTEGER |             |
| add_payment_info                | NULLABLE | INTEGER |             |
| list_id                         | NULLABLE | STRING  |             |
| list_name                       | NULLABLE | STRING  |             |
| creative_name                   | NULLABLE | STRING  |             |
| creative_slot                   | NULLABLE | STRING  |             |
| promotion_id                    | NULLABLE | STRING  |             |
| promotion_name                  | NULLABLE | STRING  |             |
| item_list_id                    | NULLABLE | STRING  |             |
| item_list_name                  | NULLABLE | STRING  |             |
| item_affiliation                | NULLABLE | STRING  |             |
| item_coupon                     | NULLABLE | STRING  |             |
| item_discount                   | NULLABLE | FLOAT   |             |
| item_brand                      | NULLABLE | STRING  |             |
| item_id                         | NULLABLE | STRING  |             |
| item_name                       | NULLABLE | STRING  |             |
| item_variant                    | NULLABLE | STRING  |             |
| item_category                   | NULLABLE | STRING  |             |
| item_category_2                 | NULLABLE | STRING  |             |
| item_category_3                 | NULLABLE | STRING  |             |
| item_category_4                 | NULLABLE | STRING  |             |
| item_category_5                 | NULLABLE | STRING  |             |
| item_price                      | NULLABLE | FLOAT   |             |
| item_quantity_purchased         | NULLABLE | INTEGER |             |
| item_quantity_refunded          | NULLABLE | INTEGER |             |
| item_quantity_added_to_cart     | NULLABLE | INTEGER |             |
| item_quantity_removed_from_cart | NULLABLE | INTEGER |             |
| item_purchase_revenue           | NULLABLE | FLOAT   |             |
| item_refund_revenue             | NULLABLE | FLOAT   |             |
| item_unique_purchases           | NULLABLE | INTEGER |             |
| purchase_net_refund             | NULLABLE | INTEGER |             |
| item_revenue_net_refund         | NULLABLE | FLOAT   |             |


### Ecommerce: Shopping stages - Closed funnel

| name                     | mode     | type   | description |
|--------------------------|----------|--------|-------------|
| event_date               | NULLABLE | DATE   |             |
| client_id                | NULLABLE | STRING |             |
| session_id               | NULLABLE | STRING |             |
| session_channel_grouping | NULLABLE | STRING |             |
| session_source           | NULLABLE | STRING |             |
| session_campaign         | NULLABLE | STRING |             |
| session_device_type      | NULLABLE | STRING |             |
| session_country          | NULLABLE | STRING |             |
| session_browser_language | NULLABLE | STRING |             |
| step_name                | NULLABLE | STRING |             |
| client_id_next_step      | NULLABLE | STRING |             |
| session_id_next_step     | NULLABLE | STRING |             |


### Ecommerce: Shopping stages - Open funnel

| name                      | mode     | type    | description |
|---------------------------|----------|---------|-------------|
| event_date                | NULLABLE | DATE    |             |
| client_id                 | NULLABLE | STRING  |             |
| session_id                | NULLABLE | STRING  |             |
| session_channel_grouping  | NULLABLE | STRING  |             |
| session_source            | NULLABLE | STRING  |             |
| session_campaign          | NULLABLE | STRING  |             |
| session_device_type       | NULLABLE | STRING  |             |
| session_country           | NULLABLE | STRING  |             |
| session_browser_language  | NULLABLE | STRING  |             |
| step_name                 | NULLABLE | STRING  |             |
| step_index                | NULLABLE | INTEGER |             |
| step_index_next_step_real | NULLABLE | INTEGER |             |
| step_index_next_step      | NULLABLE | INTEGER |             |
| status                    | NULLABLE | STRING  |             |
| client_id_next_step       | NULLABLE | STRING  |             |
| session_id_next_step      | NULLABLE | STRING  |             |



## Create main table and wiews
Run this query to create the views and the main table.

``` sql
# NAMELESS ANALYTICS 
declare project_name string default 'tom-moretti';  -- Change this according to your project name
declare dataset_name string default 'nameless_analytics'; -- Change this according to your dataset name

declare main_table_path string;
declare users_path string;
declare sessions_path string;
declare pages_path string;
declare ec_transactions_path string;
declare ec_products_path string;
declare ec_shopping_stages_closed_funnel_path string;
declare ec_shopping_stages_open_funnel_path string;

declare main_table_sql string;
declare users_sql string;
declare sessions_sql string;
declare pages_sql string;
declare ec_transactions_sql string;
declare ec_products_sql string;
declare ec_shopping_stages_closed_funnel_sql string;
declare ec_shopping_stages_open_funnel_sql string;

# ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------

set main_table_path = CONCAT('`', project_name, '.', dataset_name, '.hits`'); -- Change this if you want modify the default raw data table name
set users_path = CONCAT('`', project_name, '.', dataset_name, '.users`');
set sessions_path = CONCAT('`', project_name, '.', dataset_name, '.sessions`');
set pages_path = CONCAT('`', project_name, '.', dataset_name, '.pages`');
set ec_transactions_path = CONCAT('`', project_name, '.', dataset_name, '.ec_transactions`');
set ec_products_path = CONCAT('`', project_name, '.', dataset_name, '.ec_products`');
set ec_shopping_stages_closed_funnel_path = CONCAT('`', project_name, '.', dataset_name, '.ec_shopping_stages_closed_funnel`');
set ec_shopping_stages_open_funnel_path = CONCAT('`', project_name, '.', dataset_name, '.ec_shopping_stages_open_funnel`');


# Main table
set main_table_sql = format(
  """
    create table if not exists %s (
      event_date DATE OPTIONS (description = 'Date of the request'),
      client_id STRING OPTIONS (description = 'Client id of the user'),
      session_id STRING OPTIONS (description = 'Session id of the user'),
      event_name STRING OPTIONS (description = 'Event name of the request'),
      event_timestamp INT64 OPTIONS (description = 'Insert timestamp of the event'),
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
          value BOOL OPTIONS (description = 'Consent data parameter boolean value')
        >
      > OPTIONS (description = 'Consent data')
    )
    PARTITION BY event_date
    CLUSTER BY client_id, session_id, event_name
    OPTIONS (description = 'Nameless Analytics | Main table')
  """
, main_table_path);


# Users view
set users_sql = format(
  """
    create view if not exists %s as (
      with user_data_raw as ( 
        select   
          -- USER DATA
          client_id,
          --- (select value.string from unnest (user_data) where name = 'user_id') as user_id,
          --- (select value.string from unnest (user_data) where name = 'customer_id') as customer_id,

          first_value(event_timestamp) over (partition by client_id order by event_timestamp asc) as min_user_timestamp,
          first_value(event_timestamp) over (partition by client_id order by event_timestamp desc) as max_user_timestamp,
          first_value((select value.string from unnest (event_data) where name = 'channel_grouping')) over (partition by client_id order by event_timestamp asc) as user_channel_grouping,
          first_value((select value.string from unnest (event_data) where name = 'source')) over (partition by client_id order by event_timestamp asc) as user_source,
          first_value((select value.string from unnest (event_data) where name = 'campaign')) over (partition by client_id order by event_timestamp asc) as user_campaign,

          -- SESSION DATA
          session_id, 
          first_value(event_timestamp) over (partition by session_id order by event_timestamp asc) as min_session_timestamp,
          first_value(event_timestamp) over (partition by session_id order by event_timestamp desc) as max_session_timestamp,

          -- EVENT DATA
          event_name,
          event_timestamp,

          -- ECOMMERCE DATA
          (select value.json from unnest(event_data) where name = 'ecommerce') as transaction_data,
          json_extract_array((select value.json from unnest(event_data) where name = 'ecommerce'), '$.items') as items_data
        from %s
      ),

      user_data_product_def as (
        select
          client_id,
          -- user_id,
          -- customer_id,
          user_channel_grouping,
          case
            when net.reg_domain(user_source) is not null then net.reg_domain(user_source)
            else user_source
          end as user_source,
          user_campaign,
          timestamp_millis(min_user_timestamp) as min_user_timestamp,
          timestamp_millis(max_user_timestamp) as max_user_timestamp,
          session_id,
          timestamp_millis(min_session_timestamp) as min_session_timestamp,
          timestamp_millis(max_session_timestamp) as max_session_timestamp,
          event_name,
          event_timestamp,
          case
            when event_name = 'purchase' then json_value(transaction_data.transaction_id)
            else null
          end as purchase,
          case
            when event_name = 'refund' then json_value(transaction_data.transaction_id)
            else null
          end as refund,
          case 
            when event_name = 'purchase' then cast(json_value(items, '$.price') as float64) * cast(json_value(items, '$.quantity') as int64)
            else 0
          end as item_revenue_purchased,
          case 
            when event_name = 'refund' then -cast(json_value(items, '$.price') as float64) * cast(json_value(items, '$.quantity') as int64)
            else 0
          end as item_revenue_refunded,
          case when 
            event_name = 'purchase' then cast(json_value(items, '$.quantity') as int64)
            else 0
          end as item_quantity_purchased,
          case when 
            event_name ='refund' then cast(json_value(items, '$.quantity') as int64)
            else 0
          end as item_quantity_refunded,
        from user_data_raw
          left join unnest(items_data) as items
      ),
      
      user_data_session_def as (
        select 
          client_id,
          -- user_id,
          -- customer_id,
          user_channel_grouping,
          user_source,
          user_campaign,
          max_user_timestamp,
          min_user_timestamp,
          session_id,
          dense_rank() over (partition by client_id order by min_session_timestamp asc) as session_number,
          min_session_timestamp,
          max_session_timestamp,
          TIMESTAMP_DIFF(max_session_timestamp, min_session_timestamp, MILLISECOND) / 1000 as session_duration_sec,
          countif(event_name = 'page_view') as page_view,
          count(distinct purchase) as purchase,
          count(distinct refund) as refund,
          sum(item_revenue_purchased) as item_revenue_purchased,
          sum(item_revenue_refunded) as item_revenue_refunded,
          sum(item_quantity_purchased) as item_quantity_purchased,
          sum(item_quantity_refunded) as item_quantity_refunded,
        from user_data_product_def
        group by all
      ),

      user_data_def as(
        select 
          client_id,
          -- user_id,
          -- customer_id,
          case 
            when session_number = 1 then client_id
            else null
          end as new_user,
          case 
            when session_number > 1 then client_id
            else null
          end as returning_user,
          user_channel_grouping,
          user_source,
          user_campaign,
          min_user_timestamp,
          max_user_timestamp,
          date_diff(CURRENT_DATE(), DATE(min_user_timestamp), day) as days_since_first_visit,
          date_diff(CURRENT_DATE(), date(max_user_timestamp), day) as days_from_last_visit,
          session_id,
          session_number,
          case 
            when sum(page_view) >= 2 and (avg(session_duration_sec) >= 10 or sum(purchase) >= 1) then 1
            else 0
          end as engaged_session,
          case 
          when sum(page_view) >= 2 and (avg(session_duration_sec) >= 10 or sum(purchase) >= 1) then 1
          else 0
          end as engaged_session,
          sum(page_view) as page_view,
          sum(purchase) as purchase,
          sum(refund) as refund,
          sum(item_quantity_purchased) as item_quantity_purchased,
          sum(item_quantity_refunded) as item_quantity_refunded,
          sum(item_revenue_purchased) as purchase_revenue,
          sum(item_revenue_refunded) as refund_revenue,
          sum(item_revenue_purchased) + sum(item_revenue_refunded) as revenue_net_refund,
        from user_data_session_def
        group by all
      ),

      def as (
        select 
          client_id,
          -- user_id,
          -- customer_id,
          user_channel_grouping,
          user_source,
          user_campaign,
          new_user,
          max(new_user) over (partition by client_id) as new_user_id,
          returning_user,
          max(returning_user) over (partition by client_id) as returning_user_id,
          case 
            when sum(purchase) = 0 then 'Not customer'
            when sum(purchase) >= 1 then 'Customer'
          end as is_customer,
          case 
            when sum(purchase) = 0 then 1
            else null
          end as not_customers,
          case 
            when sum(purchase) >= 1 then 1
            else null
          end as customers,
          case 
            when sum(purchase) = 1 then 1
            else null
          end as new_customers,
          case 
            when sum(purchase) > 1 then 1
            else null
          end as returning_customers,
          min_user_timestamp,
          max(min_user_timestamp) over (partition by client_id) as max_min_user_timestamp,
          max_user_timestamp,
          max(max_user_timestamp) over (partition by client_id) as max_max_user_timestamp,

          days_since_first_visit,
          max(days_since_first_visit) over (partition by client_id) as max_days_since_first_visit,

          days_from_last_visit,
          max(days_from_last_visit) over (partition by client_id) as max_days_from_last_visit,
          count(distinct session_id) as sessions,
          sum(page_view) as page_view,
          sum(purchase) as purchase,
          sum(refund) as refund,
          sum(item_quantity_purchased) as item_quantity_purchased,
          sum(item_quantity_refunded) as item_quantity_refunded,
          sum(purchase_revenue) as purchase_revenue,
          sum(refund_revenue) as refund_revenue,
          sum(revenue_net_refund) as revenue_net_refund
        from user_data_def
        group by all
      )

      select 
        client_id,
        -- user_id,
        -- customer_id,
        min_user_timestamp,
        max_user_timestamp,
        max(max_days_since_first_visit) as max_days_since_first_visit,
        max(max_days_from_last_visit) as max_days_from_last_visit,
        user_channel_grouping,
        user_source,
        user_campaign,
        max(new_user_id) as new_user_id,
        max(returning_user_id) as returning_user_id,
        is_customer,
        case 
          when sum(purchase) = 1 then 'New customer'
          when sum(purchase) > 1 then 'Returning customer'
          else 'Not customer'
        end as customer_type,
        max(not_customers) as not_customers,
        max(customers) as customers,
        max(new_customers) as new_customers,
        max(returning_customers) as returning_customers,
        sum(sessions) as sessions,
        sum(page_view) as page_view,
        sum(purchase) as purchase,
        sum(refund) as refund,
        sum(item_quantity_purchased) as item_quantity_purchased,
        sum(item_quantity_refunded) as item_quantity_refunded,
        sum(purchase_revenue) as purchase_revenue,
        sum(refund_revenue) as refund_revenue,
        sum(revenue_net_refund) as revenue_net_refund
        -- RFM da fare
      from def
      group by all
    );
  """
, users_path, main_table_path);


# Sessions view
set sessions_sql = format(
  """
    create view if not exists %s as (
      with session_data_raw as ( 
        select
          -- USER DATA
          client_id,   
          --- (select value.string from unnest (event_data) where name = 'user_id') as user_id,
          --- (select value.string from unnest (user_data) where name = 'customer_id') as customer_id,

          -- SESSION DATA
          session_id, 
          first_value(event_timestamp) over (partition by session_id order by event_timestamp asc) as min_session_timestamp,
          first_value(event_timestamp) over (partition by session_id order by event_timestamp desc) as max_session_timestamp,
          first_value((select value.string from unnest (event_data) where name = 'channel_grouping')) over (partition by session_id order by event_timestamp) as session_channel_grouping,
          first_value((select value.string from unnest (event_data) where name = 'source')) over (partition by session_id order by event_timestamp) as session_source,
          first_value((select value.string from unnest (event_data) where name = 'campaign')) over (partition by session_id order by event_timestamp) as session_campaign,
          first_value((select value.string from unnest (event_data) where name = 'page_location')) over (partition by session_id order by event_timestamp) as session_landing_page_location,
          first_value((select value.string from unnest (event_data) where name = 'page_title')) over (partition by session_id order by event_timestamp) as session_landing_page_title,
          first_value((select value.string from unnest (event_data) where name = 'page_hostname')) over (partition by session_id order by event_timestamp) as session_hostname,
          first_value((select value.string from unnest (event_data) where name = 'device_type')) over (partition by session_id order by event_timestamp) as session_device_type,
          first_value((select value.string from unnest (event_data) where name = 'country')) over (partition by session_id order by event_timestamp) as session_country,
          first_value((select value.string from unnest (event_data) where name = 'browser_name')) over (partition by session_id order by event_timestamp) as session_browser_name,
          first_value((select value.string from unnest (event_data) where name = 'browser_language')) over (partition by session_id order by event_timestamp) as session_browser_language,

          -- EVENT DATA
          event_name,
          event_date,
          event_timestamp,

          -- ECOMMERCE DATA
          (select value.json from unnest(event_data) where name = 'ecommerce') as transaction_data,
        from %s
      ),

      session_data_def as (
        select 
          event_date,
          client_id,
          -- user_id,
          -- customer_id,
          dense_rank() over (partition by client_id order by min_session_timestamp asc) as session_number,
          session_id,
          min_session_timestamp,
          max_session_timestamp,
          session_channel_grouping,
          case
            when net.reg_domain(session_source) is not null then net.reg_domain(session_source)
            else session_source
          end as session_source,
          session_campaign,
          session_landing_page_location,
          session_landing_page_title,
          session_hostname,
          session_device_type,
          session_country,
          session_browser_name,
          session_browser_language,
          event_name,
          event_timestamp,
          case
            when event_name = 'purchase' then ifnull(cast(json_value(transaction_data.value) as float64), 0.0)
            else 0.0
          end as transaction_value,
          case
            when event_name = 'purchase' then ifnull(cast(json_value(transaction_data.shipping) as float64), 0.0)
            else 0.0
          end as transaction_shipping,
          case
            when event_name = 'purchase' then ifnull(cast(json_value(transaction_data.tax) as float64), 0.0)
            else 0.0
          end as transaction_tax,
          case
            when event_name = 'refund' then -ifnull(cast(json_value(transaction_data.value) as float64), 0.0)
            else 0.0
          end as refund_value,
          case
            when event_name = 'refund' then -ifnull(cast(json_value(transaction_data.shipping) as float64), 0.0)
            else 0.0
          end as refund_shipping,
          case
            when event_name = 'refund' then -ifnull(cast(json_value(transaction_data.tax) as float64), 0.0)
            else 0.0
          end as refund_tax,
        from session_data_raw
      ),

      session_data as (
        select
          event_date,
          client_id,
          -- user_id,
          -- customer_id,
          case 
            when session_number = 1 then 'new_user'
            when session_number > 1 then 'returning_user'
          end as user_type,
          case 
            when session_number = 1 then client_id
            else null
          end as new_user,
          case 
            when session_number > 1 then client_id
            else null
          end as returning_user,
          session_number,
          session_id,
          min_session_timestamp,
          session_channel_grouping,
          session_source,
          session_campaign,
          session_device_type,
          session_country,
          session_browser_name,
          session_browser_language,
          session_landing_page_location,
          session_landing_page_title,
          session_hostname,
          (max_session_timestamp - min_session_timestamp) / 1000 as session_duration_sec,
          countif(event_name = 'page_view') as page_view,
          countif(event_name = 'click_contact_button') as click_contact_button, 
          countif(event_name = 'view_item_list') as view_item_list,
          countif(event_name = 'select_item') as select_item,
          countif(event_name = 'view_item') as view_item,
          countif(event_name = 'add_to_wishlist') as add_to_wishlist,
          countif(event_name = 'add_to_cart') as add_to_cart,
          countif(event_name = 'remove_from_cart') as remove_from_cart,
          countif(event_name = 'view_cart') as view_cart,
          countif(event_name = 'begin_checkout') as begin_checkout,
          countif(event_name = 'add_shipping_info') as add_shipping_info,
          countif(event_name = 'add_payment_info') as add_payment_info,
          countif(event_name = 'purchase') as purchase,
          countif(event_name = 'refund') as refund,
          sum(transaction_value) as purchase_revenue,
          sum(transaction_shipping) as purchase_shipping,
          sum(transaction_tax) as purchase_tax,
          sum(refund_value) as refund_revenue,
          sum(refund_shipping) as refund_shipping,
          sum(refund_tax) as refund_tax,
        from session_data_def
        group by all
      )

      select 
        event_date,
        client_id,
        -- user_id,
        -- customer_id,
        user_type,
        new_user,
        returning_user,
        session_number,
        session_id,
        min_session_timestamp,
        case 
          when session_number = 1 then 1
          else 0
        end as new_session,
        case 
          when page_view >= 2 and (session_duration_sec >= 10 or purchase >= 1) then 1
          else 0
        end as engaged_session,
        session_channel_grouping,
        session_source,
        session_campaign,
        session_device_type,
        session_country,
        session_browser_name,
        session_browser_language,
        session_landing_page_location,
        session_landing_page_title,
        session_hostname,
        session_duration_sec,
        page_view,
        click_contact_button, 
        view_item_list,
        select_item,
        view_item,
        add_to_wishlist,
        add_to_cart,
        remove_from_cart,
        view_cart,
        begin_checkout,
        add_shipping_info,
        add_payment_info,
        purchase,
        refund,
        purchase_revenue,
        purchase_shipping,
        purchase_tax,
        refund_revenue,
        refund_shipping,
        refund_tax,
        purchase_revenue + refund_revenue as revenue_net_refund,
        purchase_shipping + refund_shipping as shipping_net_refund,
        purchase_tax + refund_tax as tax_net_refund,
      from session_data
    );
  """
, sessions_path, main_table_path);


# Pages view
set pages_sql = format(
  """
    create view if not exists %s as (
      with page_data_raw as ( 
        select
          -- USER DATA
          client_id,
          --- (select value.string from unnest (user_data) where name = 'user_id') as user_id,
          --- (select value.string from unnest (user_data) where name = 'customer_id') as customer_id,

          -- SESSION DATA
          session_id, 
          first_value(event_timestamp) over (partition by session_id order by event_timestamp asc) as min_session_timestamp,
          first_value(event_timestamp) over (partition by session_id order by event_timestamp desc) as max_session_timestamp,
          first_value((select value.string from unnest (event_data) where name = 'channel_grouping')) over (partition by session_id order by event_timestamp) as session_channel_grouping,
          first_value((select value.string from unnest (event_data) where name = 'source')) over (partition by session_id order by event_timestamp) as session_source,
          first_value((select value.string from unnest (event_data) where name = 'campaign')) over (partition by session_id order by event_timestamp) as session_campaign,
          first_value((select value.string from unnest (event_data) where name = 'page_location')) over (partition by session_id order by event_timestamp) as session_landing_page_location,
          first_value((select value.string from unnest (event_data) where name = 'page_title')) over (partition by session_id order by event_timestamp) as session_landing_page_title,
          first_value((select value.string from unnest (event_data) where name = 'page_hostname')) over (partition by session_id order by event_timestamp) as session_hostname,
          first_value((select value.string from unnest (event_data) where name = 'device_type')) over (partition by session_id order by event_timestamp) as session_device_type,
          first_value((select value.string from unnest (event_data) where name = 'country')) over (partition by session_id order by event_timestamp) as session_country,
          first_value((select value.string from unnest (event_data) where name = 'browser_name')) over (partition by session_id order by event_timestamp) as session_browser_name,
          first_value((select value.string from unnest (event_data) where name = 'browser_language')) over (partition by session_id order by event_timestamp) as session_browser_language,

          -- EVENT DATA
          event_name,
          event_date,
          event_timestamp,
          first_value(event_timestamp) over (partition by (select value.string from unnest (event_data) where name = 'page_id') order by event_timestamp asc) as min_page_timestamp,
          first_value(event_timestamp) over (partition by (select value.string from unnest (event_data) where name = 'page_id') order by event_timestamp desc) as max_page_timestamp,
          (select value.string from unnest (event_data) where name = 'page_id') as page_id,
          (select value.string from unnest (event_data) where name = 'page_location') as page_location,
          (select value.string from unnest (event_data) where name = 'page_hostname') as page_hostname,
          (select value.string from unnest (event_data) where name = 'page_title') as page_title,
          (select value.string from unnest (event_data) where name = 'content_group') as content_group,
        from %s 
      ),

      page_data_def as(
        select 
          event_date,
          client_id,
          -- user_id,
          -- customer_id,
          dense_rank() over (partition by client_id order by min_session_timestamp asc) as session_number,
          session_id,
          min_session_timestamp,
          session_channel_grouping,
          case
            when net.reg_domain(session_source) is not null then net.reg_domain(session_source)
            else session_source
          end as session_source,
          session_campaign,
          session_landing_page_location,
          session_landing_page_title,
          session_hostname,
          session_device_type,
          session_country,
          session_browser_name,
          session_browser_language,
          event_name,
          event_timestamp,
          dense_rank() over (partition by session_id order by event_timestamp asc) as page_view_number,
          page_id,
          page_location,
          page_hostname,
          page_title,
          content_group,
          min_page_timestamp,
          max_page_timestamp,
        from page_data_raw
      )

      select 
        event_date,
        client_id,
        -- user_id,
        -- customer_id,
        case 
          when session_number = 1 then 'new_user'
          when session_number > 1 then 'returning_user'
        end as user_type,
        case 
          when session_number = 1 then client_id
          else null
        end as new_user,
        case 
          when session_number > 1 then client_id
          else null
        end as returning_user,
        session_number,
        session_id,
        min_session_timestamp,
        session_channel_grouping,
        session_source,  
        session_campaign,
        session_device_type,
        session_country,
        session_browser_name,
        session_browser_language,
        session_landing_page_location,
        session_landing_page_title,
        session_hostname,
        page_view_number,
        page_id,
        page_location,
        page_hostname,
        page_title,
        content_group,
        (max_page_timestamp - min_page_timestamp) / 1000 as time_on_page_sec,
        countif(event_name = 'page_view') as page_view
      from page_data_def
      group by all
    );
  """
, pages_path, main_table_path);


# Ecommerce - Transactions
set ec_transactions_sql = format(
  """
    create view if not exists %s as (
      with ecommerce_data_raw as ( 
        select
          -- USER DATA
          client_id,
          --- (select value.string from unnest (user_data) where name = 'user_id') as user_id,
          --- (select value.string from unnest (user_data) where name = 'customer_id') as customer_id,

          -- SESSION DATA
          session_id, 
          first_value(event_timestamp) over (partition by session_id order by event_timestamp asc) as min_session_timestamp,
          first_value((select value.string from unnest (event_data) where name = 'channel_grouping')) over (partition by session_id order by event_timestamp) as session_channel_grouping,
          first_value((select value.string from unnest (event_data) where name = 'source')) over (partition by session_id order by event_timestamp) as session_source,
          first_value((select value.string from unnest (event_data) where name = 'campaign')) over (partition by session_id order by event_timestamp) as session_campaign,
          first_value((select value.string from unnest (event_data) where name = 'page_location')) over (partition by session_id order by event_timestamp) as session_landing_page_location,
          first_value((select value.string from unnest (event_data) where name = 'page_title')) over (partition by session_id order by event_timestamp) as session_landing_page_title,
          first_value((select value.string from unnest (event_data) where name = 'device_type')) over (partition by session_id order by event_timestamp) as session_device_type,
          first_value((select value.string from unnest (event_data) where name = 'country')) over (partition by session_id order by event_timestamp) as session_country,
          first_value((select value.string from unnest (event_data) where name = 'browser_name')) over (partition by session_id order by event_timestamp) as session_browser_name,
          first_value((select value.string from unnest (event_data) where name = 'browser_language')) over (partition by session_id order by event_timestamp) as session_browser_language,

          -- EVENT DATA
          event_name,
          event_date,
          event_timestamp,

          -- ECOMMERCE DATA
          (select value.json from unnest(event_data) where name = 'ecommerce') as transaction_data,
        from %s
      ),

      ecommerce_data_def as (
        select 
          event_date,
          client_id, 
          -- user_id,
          -- customer_id,
          dense_rank() over (partition by client_id order by min_session_timestamp asc) as session_number,
          session_id,
          min_session_timestamp,
          session_channel_grouping,
          case
            when net.reg_domain(session_source) is not null then net.reg_domain(session_source)
            else session_source
          end as session_source,
          session_campaign,
          session_landing_page_location,
          session_landing_page_title,
          session_device_type,
          session_country,
          session_browser_name,
          session_browser_language,
          event_name,
          event_timestamp,
          json_value(transaction_data.transaction_id) as transaction_id,
          json_value(transaction_data.currency) as transaction_currency,
          json_value(transaction_data.coupon) as transaction_coupon,
          case
            when event_name = 'purchase' then ifnull(cast(json_value(transaction_data.value) as float64), 0.0)
            else 0.0
          end as purchase_revenue,
          case
            when event_name = 'purchase' then ifnull(cast(json_value(transaction_data.shipping) as float64), 0.0)
            else 0.0
          end as purchase_shipping,
          case
            when event_name = 'purchase' then ifnull(cast(json_value(transaction_data.tax) as float64), 0.0)
            else 0.0
          end as purchase_tax,
          case
            when event_name = 'refund' then -ifnull(cast(json_value(transaction_data.value) as float64), 0.0)
            else 0.0
          end as refund_revenue,
          case
            when event_name = 'refund' then -ifnull(cast(json_value(transaction_data.shipping) as float64), 0.0)
            else 0.0
          end as refund_shipping,
          case
            when event_name = 'refund' then -ifnull(cast(json_value(transaction_data.tax) as float64), 0.0)
            else 0.0
          end as refund_tax,
        from ecommerce_data_raw
      ),

      ecommerce_data as (
        select 
          event_date,
          client_id, 
          -- user_id,
          -- customer_id,
          case 
            when session_number = 1 then 'new_user'
            when session_number > 1 then 'returning_user'
          end as user_type,
          case 
            when session_number = 1 then client_id
            else null
          end as new_user,
          case 
            when session_number > 1 then client_id
            else null
          end as returning_user,
          session_number,
          session_id,
          min_session_timestamp,
          session_channel_grouping,
          session_source,
          session_campaign,
          session_landing_page_location,
          session_landing_page_title,
          session_device_type,
          session_country,
          session_browser_name,
          session_browser_language,
          event_timestamp,
          event_name,
          countif(event_name = 'purchase') as purchase,
          countif(event_name = 'refund') as refund,
          transaction_id,
          transaction_currency,
          transaction_coupon,
          sum(purchase_revenue) as purchase_revenue,
          sum(purchase_shipping) as purchase_shipping,
          sum(purchase_tax) as purchase_tax,
          sum(refund_revenue) as refund_revenue,
          sum(refund_shipping) as refund_shipping,
          sum(refund_tax) as refund_tax,
        from ecommerce_data_def
        where true
          and regexp_contains(event_name, 'purchase|refund')
        group by all
      )

      select 
        event_date,
        client_id,
        -- user_id,
        -- customer_id,
        user_type,
        new_user,
        returning_user,
        session_number,
        session_id,
        min_session_timestamp,
        session_channel_grouping,
        session_source,
        session_campaign,
        session_landing_page_location,
        session_landing_page_title,
        session_device_type,
        session_country,
        session_browser_name,
        session_browser_language,
        purchase,
        refund,
        transaction_id, 
        transaction_currency,
        transaction_coupon,
        purchase_revenue,
        purchase_shipping,
        purchase_tax,
        refund_revenue,
        refund_shipping,
        refund_tax,
        purchase - refund as purchase_net_refund,
        purchase_revenue + refund_revenue as revenue_net_refund,
        purchase_shipping + refund_shipping as shipping_net_refund,
        purchase_tax + refund_tax as tax_net_refund,
      from ecommerce_data
    );
  """
, ec_transactions_path, main_table_path);


# Ecommerce - Products
set ec_products_sql = format(
  """
    create view if not exists %s as (
      with ecommerce_data_raw as ( 
        select
          -- USER DATA
          client_id,
          --- (select value.string from unnest (user_data) where name = 'user_id') as user_id,
          --- (select value.string from unnest (user_data) where name = 'customer_id') as customer_id,

          -- SESSION DATA
          session_id, 
          first_value(event_timestamp) over (partition by session_id order by event_timestamp asc) as min_session_timestamp,
          first_value((select value.string from unnest (event_data) where name = 'channel_grouping')) over (partition by session_id order by event_timestamp) as session_channel_grouping,
          first_value((select value.string from unnest (event_data) where name = 'source')) over (partition by session_id order by event_timestamp) as session_source,
          first_value((select value.string from unnest (event_data) where name = 'campaign')) over (partition by session_id order by event_timestamp) as session_campaign,
          first_value((select value.string from unnest (event_data) where name = 'page_location')) over (partition by session_id order by event_timestamp) as session_landing_page_location,
          first_value((select value.string from unnest (event_data) where name = 'page_title')) over (partition by session_id order by event_timestamp) as session_landing_page_title,
          first_value((select value.string from unnest (event_data) where name = 'device_type')) over (partition by session_id order by event_timestamp) as session_device_type,
          first_value((select value.string from unnest (event_data) where name = 'country')) over (partition by session_id order by event_timestamp) as session_country,
          first_value((select value.string from unnest (event_data) where name = 'browser_name')) over (partition by session_id order by event_timestamp) as session_browser_name,
          first_value((select value.string from unnest (event_data) where name = 'browser_language')) over (partition by session_id order by event_timestamp) as session_browser_language,

          -- EVENT DATA
          event_name,
          event_date,
          event_timestamp,

          -- ECOMMERCE DATA
          (select value.json from unnest(event_data) where name = 'ecommerce') as transaction_data,
          json_extract_array((select value.json from unnest(event_data) where name = 'ecommerce'), '$.items') as items_data
        from %s
      ),

      ecommerce_data_def as (
        select 
          event_date,
          client_id, 
          -- user_id,
          dense_rank() over (partition by client_id order by min_session_timestamp asc) as session_number,
          session_id,
          min_session_timestamp,
          session_channel_grouping,
          case
            when net.reg_domain(session_source) is not null then net.reg_domain(session_source)
            else session_source
          end as session_source,
          session_campaign,
          session_landing_page_location,
          session_landing_page_title,
          session_device_type,
          session_country,
          session_browser_name,
          session_browser_language,
          event_name,
          event_timestamp,
          json_value(transaction_data.transaction_id) as transaction_id,
          json_value(transaction_data.item_list_id) as list_id,
          json_value(transaction_data.item_list_name) as list_name,
          json_value(transaction_data.creative_name) as creative_name,
          json_value(transaction_data.creative_slot) as creative_slot,
          json_value(transaction_data.promotion_id) as promotion_id,
          json_value(transaction_data.promotion_name) as promotion_name,
          json_value(items, '$.item_list_id') as item_list_id,
          json_value(items, '$.item_list_name') as item_list_name,
          json_value(items, '$.affiliation') as item_affiliation,
          json_value(items, '$.coupon') as item_coupon,
          cast(json_value(items, '$.discount') as float64) as item_discount,
          json_value(items, '$.item_brand') as item_brand,
          json_value(items, '$.item_id') as item_id,
          json_value(items, '$.item_name') as item_name,
          json_value(items, '$.item_variant') as item_variant,
          json_value(items, '$.item_category') as item_category,
          json_value(items, '$.item_category2') as item_category_2,
          json_value(items, '$.item_category3') as item_category_3,
          json_value(items, '$.item_category4') as item_category_4,
          json_value(items, '$.item_category5') as item_category_5,
          cast(json_value(items, '$.price') as float64) as item_price,
          case when 
            event_name = 'purchase' then cast(json_value(items, '$.quantity') as int64)
            else 0
          end as item_quantity_purchased,
          case when 
            event_name ='refund' then cast(json_value(items, '$.quantity') as int64)
            else 0
          end as item_quantity_refunded,
          case when 
            event_name = 'add_to_cart' then cast(json_value(items, '$.quantity') as int64)
            else 0
          end as item_quantity_added_to_cart,
          case when 
            event_name = 'remove_from_cart' then cast(json_value(items, '$.quantity') as int64)
            else 0
          end as item_quantity_removed_from_cart,
          case 
            when event_name = 'purchase' then cast(json_value(items, '$.price') as float64) * cast(json_value(items, '$.quantity') as int64)
            else 0
          end as item_revenue_purchased,
          case 
            when event_name = 'refund' then -cast(json_value(items, '$.price') as float64) * cast(json_value(items, '$.quantity') as int64)
            else 0
          end as item_revenue_refunded,
          case 
            when event_name = 'purchase' then count(distinct json_value(items, '$.item_name'))
            else 0 
          end as item_unique_purchases
        from ecommerce_data_raw
          left join unnest(items_data) as items
        group by all
      ),

      ecommerce_data as (
      select 
        event_date,
        client_id, 
        -- user_id,
        case 
          when session_number = 1 then 'new_user'
          when session_number > 1 then 'returning_user'
        end as user_type,
        case 
          when session_number = 1 then client_id
          else null
        end as new_user,
        case 
          when session_number > 1 then client_id
          else null
        end as returning_user,
        session_number,
        session_id,
        min_session_timestamp,
        session_channel_grouping,
        session_source,
        session_campaign,
        session_landing_page_location,
        session_landing_page_title,
        session_device_type,
        session_country,
        session_browser_name,
        session_browser_language,
        event_timestamp,
        event_name,
        countif(event_name = "view_promotion") as view_promotion,
        countif(event_name = "select_promotion") as select_promotion,
        countif(event_name = "view_item_list") as view_item_list,
        countif(event_name = "select_item") as select_item,
        countif(event_name = "view_item") as view_item,
        countif(event_name = "add_to_wishlist") as add_to_wishlist,
        countif(event_name = "add_to_cart") as add_to_cart,
        countif(event_name = "remove_from_cart") as remove_from_cart,
        countif(event_name = "view_cart") as view_cart,
        countif(event_name = "begin_checkout") as begin_checkout,
        countif(event_name = "add_shipping_info") as add_shipping_info,
        countif(event_name = "add_payment_info") as add_payment_info,
        countif(event_name = 'purchase') as purchase,
        countif(event_name = 'refund') as refund,
        transaction_id,
        list_id,
        list_name,
        creative_name,
        creative_slot,
        promotion_id,
        promotion_name,
        item_list_id,
        item_list_name,
        item_affiliation,
        item_coupon,
        item_discount,
        item_brand,
        item_id,
        item_name,
        item_variant,
        item_category,
        item_category_2,
        item_category_3,
        item_category_4,
        item_category_5,
        sum(item_price) as item_price,
        sum(item_quantity_purchased) as item_quantity_purchased,
        sum(item_quantity_refunded) as item_quantity_refunded,
        sum(item_quantity_added_to_cart) as item_quantity_added_to_cart,
        sum(item_quantity_removed_from_cart) as item_quantity_removed_from_cart,
        sum(item_revenue_purchased) as item_purchase_revenue,
        sum(item_revenue_refunded) as item_refund_revenue,
        sum(item_unique_purchases) as item_unique_purchases
      from ecommerce_data_def
      where true
        and regexp_contains(event_name, 'view_promotion|select_promotion|view_item_list|select_item|view_item|add_to_wishlist|add_to_cart|remove_from_cart|view_cart|begin_checkout|add_shipping_info|add_payment_info|purchase|refund')
        group by all
      )

      select 
        event_date,
        client_id,
        -- user_id,
        user_type,
        new_user,
        returning_user,
        session_number,
        session_id,
        min_session_timestamp,
        session_channel_grouping,
        session_source,
        session_campaign,
        session_landing_page_location,
        session_landing_page_title,
        session_device_type,
        session_country,
        session_browser_name,
        session_browser_language,
        purchase,
        refund,
        transaction_id, 
        view_promotion,
        select_promotion,
        view_item_list,
        select_item,
        view_item,
        add_to_wishlist,
        add_to_cart,
        remove_from_cart,
        view_cart,
        begin_checkout,
        add_shipping_info,
        add_payment_info,
        list_id,
        list_name,
        creative_name,
        creative_slot,
        promotion_id,
        promotion_name,
        item_list_id,
        item_list_name,
        item_affiliation,
        item_coupon,
        item_discount,
        item_brand,
        item_id,
        item_name,
        item_variant,
        item_category,
        item_category_2,
        item_category_3,
        item_category_4,
        item_category_5,
        item_price,
        item_quantity_purchased,
        item_quantity_refunded,
        item_quantity_added_to_cart,
        item_quantity_removed_from_cart,
        item_purchase_revenue,
        item_refund_revenue,
        item_unique_purchases,
        purchase - refund as purchase_net_refund,
        item_purchase_revenue + item_refund_revenue as item_revenue_net_refund,
      from ecommerce_data
    );
  """
, ec_products_path, main_table_path);


# Ecommerce: Shopping Stages - Closed funnel
set ec_shopping_stages_closed_funnel_sql = format(
  """
    create view if not exists %s as (
      with shopping_stage_data_raw as ( 
        select
          -- USER DATA
          client_id,
          --- (select value.string from unnest (user_data) where name = 'user_id') as user_id,
          --- (select value.string from unnest (user_data) where name = 'customer_id') as customer_id,

          -- SESSION DATA
          session_id, 
          first_value(event_timestamp) over (partition by session_id order by event_timestamp asc) as min_session_timestamp,
          first_value((select value.string from unnest (event_data) where name = 'channel_grouping')) over (partition by session_id order by event_timestamp) as session_channel_grouping,
          first_value((select value.string from unnest (event_data) where name = 'source')) over (partition by session_id order by event_timestamp) as session_source,
          first_value((select value.string from unnest (event_data) where name = 'campaign')) over (partition by session_id order by event_timestamp) as session_campaign,
          first_value((select value.string from unnest (event_data) where name = 'page_location')) over (partition by session_id order by event_timestamp) as session_landing_page_location,
          first_value((select value.string from unnest (event_data) where name = 'device_type')) over (partition by session_id order by event_timestamp) as session_device_type,
          first_value((select value.string from unnest (event_data) where name = 'country')) over (partition by session_id order by event_timestamp) as session_country,
          first_value((select value.string from unnest (event_data) where name = 'browser_name')) over (partition by session_id order by event_timestamp) as session_browser_name,
          first_value((select value.string from unnest (event_data) where name = 'browser_language')) over (partition by session_id order by event_timestamp) as session_browser_language,

          -- EVENT DATA
          event_name,
          event_date,
          -- event_timestamp,

          -- ECOMMERCE DATA
          -- (select value.json from unnest(event_data) where name = 'ecommerce') as transaction_data,
        from %s
      ),

      all_sessions as (
        select 
          event_date,
          client_id,
          session_id,
          session_channel_grouping,
          case
            when net.reg_domain(session_source) is not null then net.reg_domain(session_source)
            else session_source
          end as session_source,
          session_campaign,
          session_device_type,
          session_country,
          session_browser_language,
        from shopping_stage_data_raw
        group by all
      ),

      view_item as (
        select 
          event_date,
          client_id,
          session_id,
          session_channel_grouping,
          case
            when net.reg_domain(session_source) is not null then net.reg_domain(session_source)
            else session_source
          end as session_source,
          session_campaign,
          session_device_type,
          session_country,
          session_browser_language,
        from shopping_stage_data_raw
        where event_name = 'view_item'
        group by all
      ),

      add_to_cart as (
        select 
          event_date,
          client_id,
          session_id,
          session_channel_grouping,
          case
            when net.reg_domain(session_source) is not null then net.reg_domain(session_source)
            else session_source
          end as session_source,
          session_campaign,
          session_device_type,
          session_country,
          session_browser_language,
        from shopping_stage_data_raw
        where event_name = 'add_to_cart'
        group by all
      ),

      begin_checkout as (
        select 
          event_date,
          client_id,
          session_id,
          session_channel_grouping,
          case
            when net.reg_domain(session_source) is not null then net.reg_domain(session_source)
            else session_source
          end as session_source,
          session_campaign,
          session_device_type,
          session_country,
          session_browser_language,
        from shopping_stage_data_raw
        where event_name = 'begin_checkout'
        group by all
      ),

      add_payment_info as (
        select 
          event_date,
          client_id,
          session_id,
          session_channel_grouping,
          case
            when net.reg_domain(session_source) is not null then net.reg_domain(session_source)
            else session_source
          end as session_source,
          session_campaign,
          session_device_type,
          session_country,
          session_browser_language,
        from shopping_stage_data_raw
        where event_name = 'add_payment_info'
        group by all
      ),

      add_shipping_info as (
        select 
          event_date,
          client_id,
          session_id,
          session_channel_grouping,
          case
            when net.reg_domain(session_source) is not null then net.reg_domain(session_source)
            else session_source
          end as session_source,
          session_campaign,
          session_device_type,
          session_country,
          session_browser_language,
        from shopping_stage_data_raw
        where event_name = 'add_shipping_info'
        group by all
      ),

      purchase as (
        select 
          event_date,
          client_id,
          session_id,
          session_channel_grouping,
          case
            when net.reg_domain(session_source) is not null then net.reg_domain(session_source)
            else session_source
          end as session_source,
          session_campaign,
          session_device_type,
          session_country,
          session_browser_language,
        from shopping_stage_data_raw
        where event_name = 'purchase'
        group by all
      ),

      join_steps as (
        select 
          all_sessions.event_date,
          all_sessions.client_id,
          all_sessions.session_id,
          all_sessions.session_channel_grouping,
          all_sessions.session_source,
          all_sessions.session_campaign,
          all_sessions.session_device_type,
          all_sessions.session_country,
          all_sessions.session_browser_language,

          all_sessions.client_id as all_sessions_users,
          view_item.client_id as view_item_users,
          add_to_cart.client_id as add_to_cart_users,
          begin_checkout.client_id as begin_checkout_users,
          add_shipping_info.client_id as add_shipping_info_users,
          add_payment_info.client_id as add_payment_info_users,
          purchase.client_id as purchase_users,

          all_sessions.session_id as all_sessions_sessions,
          view_item.session_id as view_item_sessions,
          add_to_cart.session_id as add_to_cart_sessions,
          begin_checkout.session_id as begin_checkout_sessions,
          add_shipping_info.session_id as add_shipping_info_sessions,
          add_payment_info.session_id as add_payment_info_sessions,
          purchase.session_id as purchase_sessions

        from all_sessions
          left join view_item
            on all_sessions.session_id = view_item.session_id
          left join add_to_cart
            on view_item.session_id = add_to_cart.session_id
          left join begin_checkout
            on add_to_cart.session_id = begin_checkout.session_id
          left join add_shipping_info
            on begin_checkout.session_id = add_shipping_info.session_id
          left join add_payment_info
            on add_shipping_info.session_id = add_payment_info.session_id
          left join purchase
            on add_payment_info.session_id = purchase.session_id
      ),

      steps_pivot as (
        select 
          *
        from join_steps
          unpivot((client_id, session_id) for step_name in (
            (all_sessions_users, all_sessions_sessions) as "0 - All sessions",
            (view_item_users, view_item_sessions) as "1 - View item",
            (add_to_cart_users, add_to_cart_sessions) as "2 - Add to cart",
            (begin_checkout_users, begin_checkout_sessions) as "3 - Begin checkout",
            (add_shipping_info_users, add_shipping_info_sessions) as "4 - Add shipping info",
            (add_payment_info_users, add_payment_info_sessions) as "5 - Add payment info",
            (purchase_users, purchase_sessions) as "6 - Purchase"
          ))
      )

      select
        event_date,
        client_id,
        session_id,
        session_channel_grouping,
        session_source,
        session_campaign,
        session_device_type,
        session_country,
        session_browser_language,
        step_name,
        lead(client_id, 1) over (
          partition by client_id, session_id, session_device_type, session_country, session_browser_language, session_channel_grouping, session_source, session_campaign
          order by event_date, client_id, session_id, session_device_type, session_country, session_browser_language, session_channel_grouping, session_source, session_campaign, step_name
        ) as client_id_next_step,
        lead(session_id, 1) over (
          partition by client_id, session_id, session_device_type, session_country, session_browser_language, session_channel_grouping, session_source, session_campaign
          order by event_date, client_id, session_id, session_device_type, session_country, session_browser_language, session_channel_grouping, session_source, session_campaign, step_name
        ) as session_id_next_step
      from steps_pivot
      where true
      group by all
    );
  """
, ec_shopping_stages_closed_funnel_path, main_table_path);


# Ecommerce: Shopping Stages - Open funnel
set ec_shopping_stages_open_funnel_sql = format(
  """
    create view if not exists %s as (
      with shopping_stage_data_raw as ( 
        select
          -- USER DATA
          client_id,
          --- (select value.string from unnest (user_data) where name = 'user_id') as user_id,
          --- (select value.string from unnest (user_data) where name = 'customer_id') as customer_id,

          -- SESSION DATA
          session_id, 
          first_value(event_timestamp) over (partition by session_id order by event_timestamp asc) as min_session_timestamp,
          first_value((select value.string from unnest (event_data) where name = 'channel_grouping')) over (partition by session_id order by event_timestamp) as session_channel_grouping,
          first_value((select value.string from unnest (event_data) where name = 'source')) over (partition by session_id order by event_timestamp) as session_source,
          first_value((select value.string from unnest (event_data) where name = 'campaign')) over (partition by session_id order by event_timestamp) as session_campaign,
          first_value((select value.string from unnest (event_data) where name = 'page_location')) over (partition by session_id order by event_timestamp) as session_landing_page_location,
          first_value((select value.string from unnest (event_data) where name = 'device_type')) over (partition by session_id order by event_timestamp) as session_device_type,
          first_value((select value.string from unnest (event_data) where name = 'country')) over (partition by session_id order by event_timestamp) as session_country,
          first_value((select value.string from unnest (event_data) where name = 'browser_name')) over (partition by session_id order by event_timestamp) as session_browser_name,
          first_value((select value.string from unnest (event_data) where name = 'browser_language')) over (partition by session_id order by event_timestamp) as session_browser_language,

          -- EVENT DATA
          event_name,
          event_date,
          -- event_timestamp,

          -- ECOMMERCE DATA
          -- (select value.json from unnest(event_data) where name = 'ecommerce') as transaction_data,
        from %s
      ),

      all_sessions as (
        select 
          event_date,
          client_id,
          session_id,
          session_channel_grouping,
          case
            when net.reg_domain(session_source) is not null then net.reg_domain(session_source)
            else session_source
          end as session_source,
          session_campaign,
          session_device_type,
          session_country,
          session_browser_language,
        from shopping_stage_data_raw
        group by all
      ),

      view_item as (
        select 
          event_date,
          client_id,
          session_id,
          session_channel_grouping,
          case
            when net.reg_domain(session_source) is not null then net.reg_domain(session_source)
            else session_source
          end as session_source,
          session_campaign,
          session_device_type,
          session_country,
          session_browser_language,
        from shopping_stage_data_raw
        where event_name = 'view_item'
        group by all
      ),

      add_to_cart as (
        select 
          event_date,
          client_id,
          session_id,
          session_channel_grouping,
          case
            when net.reg_domain(session_source) is not null then net.reg_domain(session_source)
            else session_source
          end as session_source,
          session_campaign,
          session_device_type,
          session_country,
          session_browser_language,
        from shopping_stage_data_raw
        where event_name = 'add_to_cart'
        group by all
      ),

      begin_checkout as (
        select 
          event_date,
          client_id,
          session_id,
          session_channel_grouping,
          case
            when net.reg_domain(session_source) is not null then net.reg_domain(session_source)
            else session_source
          end as session_source,
          session_campaign,
          session_device_type,
          session_country,
          session_browser_language,
        from shopping_stage_data_raw
        where event_name = 'begin_checkout'
        group by all
      ),

      add_payment_info as (
        select 
          event_date,
          client_id,
          session_id,
          session_channel_grouping,
          case
            when net.reg_domain(session_source) is not null then net.reg_domain(session_source)
            else session_source
          end as session_source,
          session_campaign,
          session_device_type,
          session_country,
          session_browser_language,
        from shopping_stage_data_raw
        where event_name = 'add_payment_info'
        group by all
      ),

      add_shipping_info as (
        select 
          event_date,
          client_id,
          session_id,
          session_channel_grouping,
          case
            when net.reg_domain(session_source) is not null then net.reg_domain(session_source)
            else session_source
          end as session_source,
          session_campaign,
          session_device_type,
          session_country,
          session_browser_language,
        from shopping_stage_data_raw
        where event_name = 'add_shipping_info'
        group by all
      ),

      purchase as (
        select 
          event_date,
          client_id,
          session_id,
          session_channel_grouping,
          case
            when net.reg_domain(session_source) is not null then net.reg_domain(session_source)
            else session_source
          end as session_source,
          session_campaign,
          session_device_type,
          session_country,
          session_browser_language,
        from shopping_stage_data_raw
        where event_name = 'purchase'
        group by all
      ),

      union_steps as (
        select 
          *,
          'All' as status,
        "0 - All" as step_name,
        0 as step_index
        from all_sessions

        union all

        select 
          *,
          case 
            when session_id not in (select session_id from all_sessions) then 'New funnel entries'
            else 'Continuing funnel entries'
          end as status,
          "1 - View item" as step_name,
          1 as step_index
        from view_item

        union all

        select 
          *,
          case 
            when session_id not in (select session_id from view_item) then 'New funnel entries'
            else 'Continuing funnel entries'
          end as status,
          "2 - Add to cart" as step_name,
          2 as step_index
        from add_to_cart

        union all

        select 
          *,
          case 
            when session_id not in (select session_id from add_to_cart) then 'New funnel entries'
            else 'Continuing funnel entries'
          end as status,
          "3 - Begin checkout" as step_name,
          3 as step_index
        from begin_checkout

        union all

        select 
          *,
          case 
            when session_id not in (select session_id from begin_checkout) then 'New funnel entries'
            else 'Continuing funnel entries'
          end as status,
          "4 - Add shipping info" as step_name,
          4 as step_index
        from add_shipping_info

        union all

        select 
          *,
          case 
            when session_id not in (select session_id from add_shipping_info) then 'New funnel entries'
            else 'Continuing funnel entries'
          end as status,
          "5 - Add payment info" as step_name,
          5 as step_index
        from add_payment_info
        
        union all

        select 
          *,
          case 
            when session_id not in (select session_id from add_payment_info) then 'New funnel entries'
            else 'Continuing funnel entries'
          end as status,
          "6 - Purchase" as step_name,
          6 as step_index
        from purchase
      ),
      
      union_steps_def as (
        select
          event_date,
          client_id,
          session_id,
          session_channel_grouping,
          session_source,
          session_campaign,
          session_device_type,
          session_country,
          session_browser_language,
          step_name,
          step_index,
          case 
            when step_name = '6 - Purchase' then null 
            else step_index + 1 
          end as step_index_next_step_real,
          
          lead(step_index, 1) over (
            partition by client_id, session_id, session_device_type, session_country, session_browser_language, session_channel_grouping, session_source, session_campaign
            order by event_date, client_id, session_id, session_device_type, session_country, session_browser_language, session_channel_grouping, session_source, session_campaign, step_name
          ) as step_index_next_step,
          status,
          lead(client_id, 1) over (
            partition by client_id, session_id, session_device_type, session_country, session_browser_language, session_channel_grouping, session_source, session_campaign
            order by event_date, client_id, session_id, session_device_type, session_country, session_browser_language, session_channel_grouping, session_source, session_campaign, step_name
          ) as client_id_next_step,
          lead(session_id, 1) over (
            partition by client_id, session_id, session_device_type, session_country, session_browser_language, session_channel_grouping, session_source, session_campaign
            order by event_date, client_id, session_id, session_device_type, session_country, session_browser_language, session_channel_grouping, session_source, session_campaign, step_name
          ) as session_id_next_step,
        from union_steps
      )

      select 
          event_date,
          client_id,
          session_id,
          session_channel_grouping,
          session_source,
          session_campaign,
          session_device_type,
          session_country,
          session_browser_language,
          step_name,
          step_index,
          step_index_next_step_real,
          step_index_next_step,
          status,
          case 
            when step_name = '6 - Purchase' then null 
            else case when step_index_next_step_real = step_index_next_step then client_id_next_step else null end 
          end as client_id_next_step,
          case 
            when step_name = '6 - Purchase' then null 
            else case when step_index_next_step_real = step_index_next_step then session_id_next_step else null end 
          end as session_id_next_step,
      from union_steps_def
    );
  """
, ec_shopping_stages_open_funnel_path, main_table_path);


# ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------

# Create  tables 

execute immediate main_table_sql;
execute immediate users_sql;
execute immediate sessions_sql;
execute immediate pages_sql;
execute immediate ec_transactions_sql;
execute immediate ec_products_sql;
execute immediate ec_shopping_stages_closed_funnel_sql;
execute immediate ec_shopping_stages_open_funnel_sql;
```
