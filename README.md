<picture>
  <source srcset="https://github.com/user-attachments/assets/6af1ff70-3abe-4890-a952-900a18589590" media="(prefers-color-scheme: dark)">
  <img src="https://github.com/user-attachments/assets/9d9a4e42-cd46-452e-9ea8-2c03e0289006">
</picture>

---

# Tables
The Nameless Analytics Tables is a set of tables in BigQuery where users, sessions and events data are stored.

For an overview of how Nameless Analytics works [start from here](https://github.com/tommasomoretti/nameless-analytics/).

Start from here:
- Tables
  - [Main table](#main-table)
  - [User and sessions](#user-and-sessions)
  - [Batch data loader logs](#data-loader-logs)
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

```sql
CREATE OR REPLACE TABLE FUNCTION `tom-moretti.nameless_analytics.users_raw_latest`(start_date DATE, end_date DATE, user_session_scope_param STRING) AS (
  with users_raw as (
    select
      document_name,
      document_id,
      FIRST_VALUE(timestamp) OVER (PARTITION BY document_name ORDER BY timestamp DESC) AS timestamp,
      FIRST_VALUE(event_id) OVER (PARTITION BY document_name ORDER BY timestamp DESC) AS event_id,
      FIRST_VALUE(operation) OVER (PARTITION BY document_name ORDER BY timestamp DESC) AS operation,
      FIRST_VALUE(data) OVER (PARTITION BY document_name ORDER BY timestamp DESC) AS data,
      FIRST_VALUE(old_data) OVER (PARTITION BY document_name ORDER BY timestamp DESC) AS old_data,
      FIRST_VALUE(operation) OVER (PARTITION BY document_name ORDER BY timestamp DESC) = "DELETE" AS is_deleted
    from `tom-moretti.nameless_analytics.users_raw_changelog`
  )

  select 
    # USER DATA
    date(JSON_VALUE(data, '$.user_date')) as user_date,
    JSON_VALUE(data, '$.user_id') as user_id,
    JSON_VALUE(data, '$.client_id') as client_id,
    timestamp_millis(cast(JSON_VALUE(data, '$.user_first_session_timestamp') as int64)) as user_first_session_timestamp,
    timestamp_millis(cast(JSON_VALUE(data, '$.user_last_session_timestamp') as int64)) as user_last_session_timestamp,
    datetime_diff(timestamp_micros(cast(JSON_VALUE(data, '$.user_last_session_timestamp') as int64)), timestamp_micros(cast(JSON_VALUE(data, '$.user_first_session_timestamp') as int64)), day) as days_from_first_to_last_visit,
    datetime_diff(current_timestamp(), timestamp_millis(cast(JSON_VALUE(data, '$.user_last_session_timestamp') as int64)), day) as days_from_first_visit,
    datetime_diff(current_timestamp(), timestamp_millis(cast(JSON_VALUE(data, '$.user_last_session_timestamp') as int64)), day) as days_from_last_visit,
    JSON_VALUE(data, '$.user_channel_grouping') as user_channel_grouping,
    JSON_VALUE(data, '$.user_source') as user_source,
    JSON_VALUE(data, '$.user_campaign') as user_campaign,
    JSON_VALUE(data, '$.user_campaign_id') as user_campaign_id,
    JSON_VALUE(data, '$.user_device_type') as user_device_type,
    JSON_VALUE(data, '$.user_country') as user_country,
    JSON_VALUE(data, '$.user_language') as user_language,
    # Add custom values here
    # JSON_VALUE(data, '$.custom_user_param') as custom_user_param,
  
    # SESSION DATA
    date(JSON_VALUE(session_data, '$.session_date')) as session_date,
    JSON_VALUE(session_data, '$.session_id') as session_id,
    cast(JSON_VALUE(session_data, '$.session_number') as int64) as session_number,
    JSON_VALUE(session_data, '$.cross_domain_session') as cross_domain_session,
    timestamp_millis(cast(JSON_VALUE(session_data, '$.session_start_timestamp') as int64)) as session_start_timestamp,
    timestamp_millis(cast(JSON_VALUE(session_data, '$.session_end_timestamp') as int64)) as session_end_timestamp, 
    datetime_diff(timestamp_millis(cast(JSON_VALUE(session_data, '$.session_end_timestamp') as int64)), timestamp_millis(cast(JSON_VALUE(session_data, '$.session_start_timestamp') as int64)), second) as session_duration_sec,
    JSON_VALUE(session_data, '$.session_channel_grouping') as session_channel_grouping,
    JSON_VALUE(session_data, '$.session_source') as session_source,
    JSON_VALUE(session_data, '$.session_campaign') as session_campaign,
    JSON_VALUE(session_data, '$.session_campaign_id') as session_campaign_id,
    JSON_VALUE(session_data, '$.session_device_type') as session_device_type,
    JSON_VALUE(session_data, '$.session_country') as session_country,
    JSON_VALUE(session_data, '$.session_language') as session_language,
    JSON_VALUE(session_data, '$.session_hostname') as session_hostname,
    JSON_VALUE(session_data, '$.session_browser_name') as session_browser_name,
    JSON_VALUE(session_data, '$.session_landing_page_category') as session_landing_page_category,
    JSON_VALUE(session_data, '$.session_landing_page_location') as session_landing_page_location,
    JSON_VALUE(session_data, '$.session_landing_page_title') as session_landing_page_title,
    JSON_VALUE(session_data, '$.session_exit_page_category') as session_exit_page_category,
    JSON_VALUE(session_data, '$.session_exit_page_location') as session_exit_page_location,
    JSON_VALUE(session_data, '$.session_exit_page_title') as session_exit_page_title,
    # Add custom values here
    # JSON_VALUE(session_data, '$.custom_session_param') as custom_session_param,
    
  from users_raw,
    unnest(JSON_EXTRACT_ARRAY(PARSE_JSON(data), '$.sessions')) AS session_data
  where true 
    and not is_deleted
    and case 
      when user_session_scope_param = 'user_level' then date(JSON_VALUE(data, '$.user_date'))
      else date(JSON_VALUE(session_data, '$.session_date'))
    end between start_date and end_date
  group by all
);
```


### Users
Lorem ipsum

```sql
CREATE OR REPLACE TABLE FUNCTION `tom-moretti.nameless_analytics.users`(start_date DATE, end_date DATE) AS (
  with user_data_raw as ( 
    select   
      -- USER DATA
      user_data.user_date,
      user_data.user_id,
      user_data.client_id,
      user_data.user_first_session_timestamp,
      user_data.user_last_session_timestamp,
      days_from_first_to_last_visit,
      days_from_first_visit,
      days_from_last_visit,
      user_data.user_channel_grouping,
      user_data.user_source,
      user_data.user_campaign,
      user_data.user_device_type,
      user_data.user_country,
      user_data.user_language,
      
      -- SESSION DATA
      user_data.session_date,
      user_data.session_id, 
      user_data.session_number,
      user_data.cross_domain_session,
      user_data.session_start_timestamp,
      user_data.session_end_timestamp,
      user_data.session_duration_sec,
      user_data.session_channel_grouping,
      user_data.session_source,
      user_data.session_campaign,
      user_data.session_hostname,
      user_data.session_device_type,
      user_data.session_country,
      user_data.session_language,
      user_data.session_browser_name,
      user_data.session_landing_page_category,
      user_data.session_landing_page_location,
      user_data.session_landing_page_title,
      user_data.session_exit_page_category,
      user_data.session_exit_page_location,
      user_data.session_exit_page_title,

      -- EVENT DATA
      event_data.event_date,
      event_data.event_name,
      event_data.event_timestamp,

      -- ECOMMERCE DATA
      if(event_data.event_name = 'purchase', event_data.event_timestamp, null) as purchase_timestamp,
      ecommerce as transaction_data,
      json_extract_array(ecommerce, '$.items') as items_data
          
    from `tom-moretti.nameless_analytics.users_raw_latest` ('2025-06-10', '2025-06-10', 'user_level') as user_data
      left join `tom-moretti.nameless_analytics.events` as event_data 
        on user_data.client_id = event_data.client_id
        and user_data.session_id = event_data.session_id
  ),

  user_data as (
    select
      user_date,
      client_id,
      user_id,
      user_first_session_timestamp,
      user_last_session_timestamp,
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
      user_channel_grouping,
      case
        when user_source = 'tagassistant.google.com' then user_source
        when net.reg_domain(user_source) is not null then net.reg_domain(user_source)
        else user_source
      end as user_source,
      user_campaign,
      user_country,
      user_language,

      # SESSION DATA
      session_date,
      session_id,
      session_duration_sec,
      
      # EVENT DATA
      event_date,
      event_name,
      timestamp_millis(event_timestamp) as event_timestamp,

      purchase_timestamp,
      timestamp_millis(min(purchase_timestamp) over (partition by client_id)) as first_purchase_timestamp,
      timestamp_millis(max(purchase_timestamp) over (partition by client_id)) as last_purchase_timestamp,

      case 
        when event_name = 'purchase' then json_value(transaction_data, '$.transaction_id')
        else null
      end as purchase_id,
      case 
        when event_name = 'refund' then json_value(transaction_data, '$.transaction_id')
        else null
      end as refund_id,
      sum(case 
        when event_name = 'purchase' then cast(json_value(items, '$.quantity') as int64)
        else 0
      end) as item_quantity_purchased,
      sum(case 
        when event_name ='refund' then cast(json_value(items, '$.quantity') as int64)
        else 0
      end) as item_quantity_refunded,
      sum(case 
        when event_name = 'purchase' then cast(json_value(items, '$.price') as float64) * cast(json_value(items, '$.quantity') as int64)
        else 0
      end) as item_revenue_purchased,
      sum(case 
        when event_name = 'refund' then -cast(json_value(items, '$.price') as float64) * cast(json_value(items, '$.quantity') as int64)
        else 0
      end) as item_revenue_refunded
    from user_data_raw
      left join unnest(items_data) as items
    group by all
  ),
    
  user_data_def as (
    select 
      # USER DATA
      user_date,
      client_id,
      user_id,
      user_channel_grouping,
      split(user_source, '.')[safe_offset(0)] as user_source,
      user_campaign,
      user_country,
      user_last_session_timestamp,
      user_first_session_timestamp,
      user_type,
      new_user,
      returning_user,
      
      # SESSION DATA
      session_date,
      session_id,
      session_duration_sec,

      # EVENT DATA
      event_date,
      event_name,
      countif(event_name = 'page_view') as page_view,
      countif(event_name = 'purchase') as purchase,
      countif(event_name = 'refund') as refund,
      first_purchase_timestamp,
      last_purchase_timestamp,
      sum(item_revenue_purchased) as item_revenue_purchased,
      sum(item_revenue_refunded) as item_revenue_refunded,
      sum(item_quantity_purchased) as item_quantity_purchased,
      sum(item_quantity_refunded) as item_quantity_refunded,
    from user_data
    group by all
  ),
  user_data_def_def as(
    select
      # USER DATA 
      user_date,
      client_id,
      user_id,
      user_channel_grouping,
      user_source,
      user_campaign,
      user_country,
      user_last_session_timestamp,
      user_first_session_timestamp,
      user_type,
      new_user,
      returning_user,
      date_diff(CURRENT_DATE(), DATE(user_first_session_timestamp), day) as days_from_first_visit,
      date_diff(CURRENT_DATE(), date(user_last_session_timestamp), day) as days_from_last_visit,

      # SESSION DATA
      session_date,
      session_id,
      count(distinct session_id) as sessions,

      case 
        when sum(page_view) >= 2 and (avg(session_duration_sec) >= 10 or countif(event_name = 'purchase') >= 1) then 1
        else 0
      end as engaged_session,
      sum(page_view) as page_view,
      date_diff(CURRENT_DATE(), DATE(first_purchase_timestamp), day) as days_from_first_purchase,
      date_diff(CURRENT_DATE(), date(last_purchase_timestamp), day) as days_from_last_purchase,
      sum(purchase) as purchase,
      sum(refund) as refund,
      sum(item_quantity_purchased) as item_quantity_purchased,
      sum(item_quantity_refunded) as item_quantity_refunded,
      sum(item_revenue_purchased) as purchase_revenue,
      sum(item_revenue_refunded) as refund_revenue,
      sum(item_revenue_purchased) + sum(item_revenue_refunded) as revenue_net_refund,
      ifnull(safe_divide(sum(item_revenue_purchased), countif(event_name = 'purchase')), 0) as avg_purchase_value,
      ifnull(safe_divide(sum(item_revenue_refunded), countif(event_name = 'refund')), 0) as avg_refund_value
    from user_data_def
    group by all
  ),

  clustering_prep as (
    select 
      # USER DATA 
      user_date,
      client_id,
      new_user as new_user_client_id,
      returning_user as returning_user_client_id,
      user_id,
      user_first_session_timestamp,
      user_last_session_timestamp,
      days_from_first_visit,
      days_from_last_visit,
      user_channel_grouping,
      user_source,
      user_campaign,
      user_country,
      case 
        when sum(purchase) = 0 then 'Not customer'
        when sum(purchase) > 0 then 'Customer'
      end as is_customer,
      case 
        when sum(purchase) = 1 then 'New customer'
        when sum(purchase) > 1 then 'Returning customer'
        else 'Not customer'
      end as customer_type,
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
      sum(sessions) as sessions,
      sum(page_view) as page_view,
      days_from_first_purchase,
      days_from_last_purchase,
      sum(purchase) as purchase,
      sum(refund) as refund,
      sum(item_quantity_purchased) as item_quantity_purchased,
      sum(item_quantity_refunded) as item_quantity_refunded,
      sum(purchase_revenue) as purchase_revenue,
      sum(refund_revenue) as refund_revenue,
      sum(revenue_net_refund) as revenue_net_refund,
      avg(avg_purchase_value) as avg_purchase_value,
      avg(avg_refund_value) as avg_refund_value
    from user_data_def_def
    group by all
  ),

  clustering as (
    select 
      *,
      CASE
        WHEN user_last_session_timestamp is null THEN 0
        ELSE NTILE(5) OVER (ORDER BY user_last_session_timestamp ASC)
      END AS recency_score,
      CASE
        WHEN purchase = 0 THEN 0
        ELSE NTILE(5) OVER (ORDER BY purchase DESC)
      END AS frequency_score,
      CASE
        WHEN purchase_revenue = 0 THEN 0
        ELSE NTILE(5) OVER (ORDER BY purchase_revenue DESC)
      END AS monetary_score,
    from clustering_prep
  )

  select 
    *,
    CONCAT(CAST(recency_score AS STRING), CAST(frequency_score AS STRING), CAST(monetary_score AS STRING)) AS rfm_segment,
    CASE 
      -- High Valuable: High scores in at least 2 dimensions (R, F, M)
      WHEN ((recency_score >= 4 AND frequency_score >= 4) OR (recency_score >= 4 AND monetary_score >= 4) OR (frequency_score >= 4 AND monetary_score >= 4)) THEN 'High Valuable'
      -- Mid Valuable: At least 1 dimension high or medium scores across all dimensions
      WHEN ((recency_score >= 3 OR frequency_score >= 3 OR monetary_score >= 3) AND (recency_score BETWEEN 2 AND 4 AND frequency_score BETWEEN 2 AND 4 AND monetary_score BETWEEN 2 AND 4)) THEN 'Mid Valuable'
      -- Low Valuable: Low scores in at least 2 dimensions
      WHEN ((recency_score <= 2 AND frequency_score <= 2) OR (recency_score <= 2 AND monetary_score <= 2) OR (frequency_score <= 2 AND monetary_score <= 2)) THEN 'Low Valuable'
      -- Others: Catch-all for any remaining segments
      ELSE 'Others'
    END AS rfm_cluster
  from clustering
);
```


### Sessions
Lorem ipsum

```sql
CREATE OR REPLACE TABLE FUNCTION `tom-moretti.nameless_analytics.sessions`(start_date DATE, end_date DATE) AS (
  with session_data_raw as ( 
    select
      -- USER DATA
      user_data.user_date,
      user_data.user_id,
      user_data.client_id,
      user_data.user_first_session_timestamp,
      user_data.user_last_session_timestamp,
      days_from_first_to_last_visit,
      days_from_first_visit,
      days_from_last_visit,
      user_data.user_channel_grouping,
      user_data.user_source,
      user_data.user_campaign,
      user_data.user_device_type,
      user_data.user_country,
      user_data.user_language,
      
      -- SESSION DATA
      user_data.session_date,
      user_data.session_id, 
      user_data.session_number,
      user_data.cross_domain_session,
      user_data.session_start_timestamp,
      user_data.session_end_timestamp,
      user_data.session_duration_sec,
      user_data.session_channel_grouping,
      user_data.session_source,
      user_data.session_campaign,
      user_data.session_hostname,
      user_data.session_device_type,
      user_data.session_country,
      user_data.session_language,
      user_data.session_browser_name,
      user_data.session_landing_page_category,
      user_data.session_landing_page_location,
      user_data.session_landing_page_title,
      user_data.session_exit_page_category,
      user_data.session_exit_page_location,
      user_data.session_exit_page_title,

      -- EVENT DATA
      event_date,
      event_name,
      timestamp_millis(event_timestamp) as event_timestamp,

      -- ECOMMERCE DATA
      ecommerce as transaction_data,
      -- json_extract_array(ecommerce, '$.items') as items_data

      -- CONSENT DATA
      (select value.string from unnest(consent_data) where name = 'consent_type') as consent_type,
      (select value.string from unnest(consent_data) where name = 'ad_user_data') as ad_user_data,
      (select value.string from unnest(consent_data) where name = 'ad_personalization') as ad_personalization,
      (select value.string from unnest(consent_data) where name = 'ad_storage') as ad_storage,
      (select value.string from unnest(consent_data) where name = 'analytics_storage') as analytics_storage,
      (select value.string from unnest(consent_data) where name = 'functionality_storage') as functionality_storage,
      (select value.string from unnest(consent_data) where name = 'personalization_storage') as personalization_storage,
      (select value.string from unnest(consent_data) where name = 'security_storage') as security_storage

    from `tom-moretti.nameless_analytics.users_raw_latest`(start_date, end_date, 'session_level') as user_data
      left join `tom-moretti.nameless_analytics.events` as event_data 
        on user_data.client_id = event_data.client_id
        and user_data.session_id = event_data.session_id  
  ),

  session_data as (
    select 
      -- USER DATA
      user_date,
      client_id,
      user_id,
      user_channel_grouping,
      case
        when user_source = 'tagassistant.google.com' then user_source
        when net.reg_domain(user_source) is not null then net.reg_domain(user_source)
        else user_source
      end as original_user_source,
      user_campaign,
      user_device_type,
      user_country,
      user_language,
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

      -- SESSION DATA
      session_date,
      session_number,
      session_id,
      session_start_timestamp,
      session_end_timestamp,
      session_duration_sec,
      session_channel_grouping,
      case
        when session_source = 'tagassistant.google.com' then session_source
        when net.reg_domain(session_source) is not null then net.reg_domain(session_source)
        else session_source
      end as original_session_source,
      session_campaign,
      cross_domain_session,  
      session_landing_page_category,
      session_landing_page_location,
      session_landing_page_title,
      session_exit_page_category,
      session_exit_page_location,
      session_exit_page_title,
      session_hostname,
      session_device_type,
      session_country,
      session_language,
      session_browser_name,

      -- EVENT DATA
      event_date,
      event_name,
      event_timestamp,

      -- ECOMMERCE DATA
      case
        when event_name = 'purchase' then ifnull(cast(json_value(transaction_data.value) as float64), 0.0)
        else null
      end as transaction_value,
      case
        when event_name = 'purchase' then ifnull(cast(json_value(transaction_data.shipping) as float64), 0.0)
        else null
      end as transaction_shipping,
      case
        when event_name = 'purchase' then ifnull(cast(json_value(transaction_data.tax) as float64), 0.0)
        else null
      end as transaction_tax,
      case
        when event_name = 'refund' then - ifnull(cast(json_value(transaction_data.value) as float64), 0.0)
        else null
      end as refund_value,
      case
        when event_name = 'refund' then - ifnull(cast(json_value(transaction_data.shipping) as float64), 0.0)
        else null
      end as refund_shipping,
      case
        when event_name = 'refund' then - ifnull(cast(json_value(transaction_data.tax) as float64), 0.0)
        else null
      end as refund_tax,

      -- CONSENT DATA
      consent_type,
      ad_user_data,
      ad_personalization,
      ad_storage,
      analytics_storage,
      functionality_storage,
      personalization_storage,
      security_storage,
      case 
        when countif(consent_type = 'Update') over (partition by session_id) > 0 then first_value(case when consent_type = 'Update' then ad_user_data end ignore nulls) over (partition by session_id order by event_timestamp asc rows between unbounded preceding and unbounded following)
        else first_value(ad_user_data) over (partition by session_id order by event_timestamp asc rows between unbounded preceding and unbounded following)
      end as session_ad_user_data,
      case 
        when countif(consent_type = 'Update') over (partition by session_id) > 0 then first_value(case when consent_type = 'Update' then ad_personalization end ignore nulls) over (partition by session_id order by event_timestamp asc rows between unbounded preceding and unbounded following)
        else first_value(ad_personalization) over (partition by session_id order by event_timestamp asc rows between unbounded preceding and unbounded following)
      end as session_ad_personalization,
      case 
        when countif(consent_type = 'Update') over (partition by session_id) > 0 then first_value(case when consent_type = 'Update' then ad_storage end ignore nulls) over (partition by session_id order by event_timestamp asc rows between unbounded preceding and unbounded following)
        else first_value(ad_storage) over (partition by session_id order by event_timestamp asc rows between unbounded preceding and unbounded following)
      end as session_ad_storage,
      case 
        when countif(consent_type = 'Update') over (partition by session_id) > 0 then first_value(case when consent_type = 'Update' then analytics_storage end ignore nulls) over (partition by session_id order by event_timestamp asc rows between unbounded preceding and unbounded following)
        else first_value(analytics_storage) over (partition by session_id order by event_timestamp asc rows between unbounded preceding and unbounded following)
      end as session_analytics_storage,
      case 
        when countif(consent_type = 'Update') over (partition by session_id) > 0 then first_value(case when consent_type = 'Update' then functionality_storage end ignore nulls) over (partition by session_id order by event_timestamp asc rows between unbounded preceding and unbounded following)
        else first_value(functionality_storage) over (partition by session_id order by event_timestamp asc rows between unbounded preceding and unbounded following)
      end as session_functionality_storage,
      case 
        when countif(consent_type = 'Update') over (partition by session_id) > 0 then first_value(case when consent_type = 'Update' then personalization_storage end ignore nulls) over (partition by session_id order by event_timestamp asc rows between unbounded preceding and unbounded following)
        else first_value(personalization_storage) over (partition by session_id order by event_timestamp asc rows between unbounded preceding and unbounded following)
      end as session_personalization_storage,
      case 
        when countif(consent_type = 'Update') over (partition by session_id) > 0 then first_value(case when consent_type = 'Update' then security_storage end ignore nulls) over (partition by session_id order by event_timestamp asc rows between unbounded preceding and unbounded following)
        else first_value(security_storage) over (partition by session_id order by event_timestamp asc rows between unbounded preceding and unbounded following)
      end as session_security_storage,
      case 
        when countif(consent_type = 'Update') over (partition by session_id) > 0 then first_value(case when consent_type = 'Update' then event_timestamp end ignore nulls) over (partition by session_id order by event_timestamp asc rows between unbounded preceding and unbounded following)
        else first_value(event_timestamp) over (partition by session_id order by event_timestamp asc rows between unbounded preceding and unbounded following)
      end as consent_timestamp,
      case 
          when countif(consent_type = 'Update') over (partition by session_id) > 0 then 'Yes'
          else 'No'
      end as consent_expressed

    from session_data_raw
    group by all
  ),

  session_data_def as (
    select
      -- USER DATA
      user_date,
      client_id,
      user_id,
      user_channel_grouping,
      split(original_user_source, '.')[safe_offset(0)] as user_source,
      original_user_source,
      user_campaign,
      user_device_type,
      user_country,
      user_language,
      user_type,
      new_user,
      returning_user,

      -- SESSION DATA
      session_date,
      session_number,
      session_id,
      session_start_timestamp as session_start,
      session_duration_sec,
      session_channel_grouping,
      split(original_session_source, '.')[safe_offset(0)] as session_source,
      original_session_source,
      session_campaign,
      session_device_type,
      session_country,
      session_browser_name,
      session_language,
      cross_domain_session,
      session_landing_page_category,
      session_landing_page_location,
      session_landing_page_title,
      session_exit_page_category,
      session_exit_page_location,
      session_exit_page_title,
      session_hostname,

      -- ECOMMERCE DATA
      countif(event_name = 'page_view') as page_view,
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
      ifnull(sum(transaction_value), 0) as purchase_revenue,
      ifnull(sum(transaction_shipping), 0) as purchase_shipping,
      ifnull(sum(transaction_tax), 0) as purchase_tax,
      ifnull(sum(refund_value), 0) as refund_revenue,
      ifnull(sum(refund_shipping), 0) as refund_shipping,
      ifnull(sum(refund_tax), 0) as refund_tax,

      -- CONSENT DATA
      case when session_ad_user_data = 'Granted' then 1 else 0 end as session_ad_user_data,
      case when session_ad_personalization = 'Granted' then 1 else 0 end as session_ad_personalization,
      case when session_ad_storage = 'Granted' then 1 else 0 end as session_ad_storage,
      case when session_analytics_storage = 'Granted' then 1 else 0 end as session_analytics_storage,
      case when session_functionality_storage = 'Granted' then 1 else 0 end as session_functionality_storage,
      case when session_personalization_storage = 'Granted' then 1 else 0 end as session_personalization_storage,
      case when session_security_storage = 'Granted' then 1 else 0 end as session_security_storage,
      consent_timestamp,
      consent_expressed

    from session_data
    group by all
  )

  select 
    -- USER DATA
    user_date,
    client_id,
    user_id,
    user_channel_grouping,
    user_source,
    original_user_source,
    user_campaign,
    user_device_type,
    user_country,
    user_language,
    user_type,
    new_user,
    returning_user,

    -- SESSION DATA
    session_date,
    session_number,
    session_id,
    session_start,
    avg(session_duration_sec) as session_duration_sec,
    case 
      when session_number = 1 then 1
      else 0
    end as first_session,
    case 
      when sum(page_view) >= 2 and (avg(session_duration_sec) >= 10 or sum(purchase) >= 1) then 1
      else 0
    end as engaged_session,
    session_channel_grouping,
    session_source, 
    original_session_source,
    session_campaign,
    session_device_type,
    session_country,
    session_browser_name,
    session_language,
    cross_domain_session,
    session_landing_page_category,
    session_landing_page_location,
    session_landing_page_title,
    session_exit_page_category,
    session_exit_page_location,
    session_exit_page_title,
    session_hostname,

    -- ECOMMERCE DATA
    sum(page_view) as page_view,
    sum(view_item_list) as view_item_list,
    sum(select_item) as select_item,
    sum(view_item) as view_item,
    sum(add_to_wishlist) as add_to_wishlist,
    sum(add_to_cart) as add_to_cart,
    sum(remove_from_cart) as remove_from_cart,
    sum(view_cart) as view_cart,
    sum(begin_checkout) as begin_checkout,
    sum(add_shipping_info) as add_shipping_info,
    sum(add_payment_info) as add_payment_info,
    sum(purchase) as purchase,
    sum(refund) as refund,
    sum(purchase_revenue) as purchase_revenue,
    sum(purchase_shipping) as purchase_shipping,
    sum(purchase_tax) as purchase_tax,
    sum(refund_revenue) as refund_revenue,
    sum(refund_shipping) as refund_shipping,
    sum(refund_tax) as refund_tax,
    sum(purchase) - sum(refund) as purchase_net_refund,
    ifnull(sum(purchase_revenue), 0) - ifnull(sum(refund_revenue), 0) as revenue_net_refund,
    ifnull(sum(purchase_shipping), 0) + ifnull(sum(refund_shipping), 0) as shipping_net_refund,
    ifnull(sum(purchase_tax), 0) + ifnull(sum(refund_tax), 0) as tax_net_refund,

    -- CONSENT DATA
    session_ad_user_data,
    session_ad_personalization,
    session_ad_storage,
    session_analytics_storage,
    session_functionality_storage,
    session_personalization_storage,
    session_security_storage,
    consent_timestamp,
    consent_expressed
    
  from session_data_def
  group by all
);
```


### Pages
Lorem ipsum

```sql
CREATE OR REPLACE TABLE FUNCTION `tom-moretti.nameless_analytics.pages`(start_date DATE, end_date DATE) AS (
  with page_data_raw as ( 
    select
      -- USER DATA
      user_data.user_date,
      user_data.user_id,
      user_data.client_id,
      user_data.user_first_session_timestamp,
      user_data.user_last_session_timestamp,
      days_from_first_to_last_visit,
      days_from_first_visit,
      days_from_last_visit,
      user_data.user_channel_grouping,
      user_data.user_source,
      user_data.user_campaign,
      user_data.user_device_type,
      user_data.user_country,
      user_data.user_language,
        
      -- SESSION DATA
      user_data.session_date,
      user_data.session_id, 
      user_data.session_number,
      user_data.cross_domain_session,
      user_data.session_start_timestamp,
      user_data.session_end_timestamp,
      user_data.session_duration_sec,
      user_data.session_channel_grouping,
      user_data.session_source,
      user_data.session_campaign,
      user_data.session_hostname,
      user_data.session_device_type,
      user_data.session_country,
      user_data.session_language,
      user_data.session_browser_name,
      user_data.session_landing_page_category,
      user_data.session_landing_page_location,
      user_data.session_landing_page_title,
      user_data.session_exit_page_category,
      user_data.session_exit_page_location,
      user_data.session_exit_page_title,

      -- PAGE DATA
      (select value.string from unnest (event_data.event_data) where name = 'page_id') as page_id,
      first_value(event_timestamp) over (partition by (select value.string from unnest (event_data.event_data) where name = 'page_id') order by event_timestamp asc) as page_load_timestamp,
      first_value(event_timestamp) over (partition by (select value.string from unnest (event_data.event_data) where name = 'page_id') order by event_timestamp desc) as page_unload_timestamp,
      (select value.string from unnest (event_data.event_data) where name = 'page_category') as page_category,
      (select value.string from unnest (event_data.event_data) where name = 'page_location') as page_location,
      (select value.string from unnest (event_data.event_data) where name = 'page_title') as page_title,
      (select value.string from unnest (event_data.event_data) where name = 'page_hostname') as page_hostname,
      (select value.int from unnest (event_data.event_data) where name = 'time_to_dom_interactive') as time_to_dom_interactive,
      (select value.int from unnest (event_data.event_data) where name = 'page_render_time') as page_render_time,
      (select value.int from unnest (event_data.event_data) where name = 'time_to_dom_complete') as time_to_dom_complete,
      (select value.int from unnest (event_data.event_data) where name = 'total_page_load_time') as total_page_load_time,
      (select value.int from unnest (event_data.event_data) where name = 'page_status_code') as page_status_code,

      -- EVENT DATA
      event_date,
      event_name,
      timestamp_millis(event_timestamp) as event_timestamp,
      
    from `tom-moretti.nameless_analytics.users_raw_latest` (start_date, end_date, 'session_level') as user_data
      left join `tom-moretti.nameless_analytics.events` as event_data 
        on user_data.client_id = event_data.client_id
        and user_data.session_id = event_data.session_id
  ),

  page_data as(
    select 
      -- USER DATA
      user_date,
      client_id,
      user_id,
      user_channel_grouping,
      case
        when user_source = 'tagassistant.google.com' then user_source
        when net.reg_domain(user_source) is not null then net.reg_domain(user_source)
        else user_source
      end as original_user_source,
      user_campaign,
      user_device_type,
      user_country,
      user_language,
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

      -- SESSION DATA
      session_date,
      session_number,
      session_id,
      session_start_timestamp,
      session_channel_grouping,
      session_source,
      case
        when session_source = 'tagassistant.google.com' then session_source
        when net.reg_domain(session_source) is not null then net.reg_domain(session_source)
        else session_source
      end as original_session_source,
      session_campaign,
      session_landing_page_category,
      session_landing_page_location,
      session_landing_page_title,
      session_exit_page_category,
      session_exit_page_location,
      session_exit_page_title,
      session_hostname,
      session_device_type,
      session_country,
      session_language,
      session_browser_name,

      -- PAGE DATA
      dense_rank() over (partition by session_id order by page_load_timestamp desc) as page_view_number,
      page_id,
      page_location,
      page_hostname,
      page_title,
      page_category,
      page_load_timestamp,
      page_unload_timestamp,
      (page_unload_timestamp - page_load_timestamp) / 1000 as time_on_page_sec,
      time_to_dom_interactive,
      page_render_time,
      time_to_dom_complete,
      total_page_load_time,
      page_status_code,
      max(page_status_code) over (partition by page_id) as max_page_status_code,
      count(1) as total_events,
      countif(event_name = 'page_view') as page_view,

    from page_data_raw
    group by all
  )

  select 
    -- USER DATA
    user_date,
    client_id,
    user_id,
    user_type,
    new_user,
    returning_user,
    
    -- SESSION DATA
    session_date,
    session_number,
    session_id,
    session_start_timestamp,
    session_channel_grouping,
    session_source,
    original_session_source,
    session_campaign,
    session_landing_page_category,
    session_landing_page_location,
    session_landing_page_title,
    session_exit_page_category,
    session_exit_page_location,
    session_exit_page_title,
    session_hostname,
    session_device_type,
    session_country,
    session_language,
    session_browser_name,

    -- PAGE DATA
    page_view_number,
    page_id,
    page_location,
    page_hostname,
    page_title,
    page_category,
    page_load_timestamp,
    page_unload_timestamp,
    avg(ifnull(time_on_page_sec, 0)) as time_on_page_sec,
    avg(ifnull(time_to_dom_interactive, 0)) / 1000 as time_to_dom_interactive,
    avg(ifnull(page_render_time, 0)) / 1000 as page_render_time,
    avg(ifnull(time_to_dom_complete, 0)) / 1000 as time_to_dom_complete,
    avg(ifnull(total_page_load_time, 0)) / 1000 as page_load_time,
    max_page_status_code as page_status_code,
    sum(total_events) as total_events,
    sum(page_view) as page_view,

  from page_data
  group by all
);
```


### Transactions
Lorem ipsum

```sql
CREATE OR REPLACE TABLE FUNCTION `tom-moretti.nameless_analytics.ec_transactions`(start_date DATE, end_date DATE) AS (
  with transaction_data_raw as ( 
    select
      -- USER DATA
      user_data.user_date,
      user_data.user_id,
      user_data.client_id,
      user_data.user_first_session_timestamp,
      user_data.user_last_session_timestamp,
      days_from_first_to_last_visit,
      days_from_first_visit,
      days_from_last_visit,
      user_data.user_channel_grouping,
      user_data.user_source,
      user_data.user_campaign,
      user_data.user_device_type,
      user_data.user_country,
      user_data.user_language,
        
      -- SESSION DATA
      user_data.session_date,
      user_data.session_id, 
      user_data.session_number,
      user_data.cross_domain_session,
      user_data.session_start_timestamp,
      user_data.session_end_timestamp,
      user_data.session_duration_sec,
      user_data.session_channel_grouping,
      user_data.session_source,
      user_data.session_campaign,
      user_data.session_hostname,
      user_data.session_device_type,
      user_data.session_country,
      user_data.session_language,
      user_data.session_browser_name,
      user_data.session_landing_page_category,
      user_data.session_landing_page_location,
      user_data.session_landing_page_title,
      user_data.session_exit_page_category,
      user_data.session_exit_page_location,
      user_data.session_exit_page_title,

      -- EVENT DATA
      event_date,
      event_name,
      timestamp_millis(event_timestamp) as event_timestamp,

      -- ECOMMERCE DATA
      ecommerce as transaction_data,

    from `tom-moretti.nameless_analytics.users_raw_latest` (start_date, end_date, 'session_level') as user_data
      left join `tom-moretti.nameless_analytics.events` as event_data 
        on user_data.client_id = event_data.client_id
        and user_data.session_id = event_data.session_id
  ),

  transaction_data as (
    select 
      -- USER DATA
      user_date,
      client_id,
      user_id,
      user_channel_grouping,
      case
        when user_source = 'tagassistant.google.com' then user_source
        when net.reg_domain(user_source) is not null then net.reg_domain(user_source)
        else user_source
      end as original_user_source,
      user_campaign,
      user_device_type,
      user_country,
      user_language,
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

      -- SESSION DATA
      session_date,
      session_number,
      session_id,
      session_start_timestamp,
      session_end_timestamp,
      session_channel_grouping,
      case
        when session_source = 'tagassistant.google.com' then session_source
        when net.reg_domain(session_source) is not null then net.reg_domain(session_source)
        else session_source
      end as original_session_source,
      session_campaign,
      session_landing_page_category,
      session_landing_page_location,
      session_landing_page_title,
      session_exit_page_category,
      session_exit_page_location,
      session_exit_page_title,
      session_hostname,
      session_device_type,
      session_country,
      session_language,
      session_browser_name,

      -- EVENT DATA
      event_date,
      event_name,
      event_timestamp,

      -- ECOMMERCE DATA
      json_value(transaction_data.transaction_id) as transaction_id,
      json_value(transaction_data.currency) as transaction_currency,
      json_value(transaction_data.coupon) as transaction_coupon,
      case
        when event_name = 'purchase' then ifnull(cast(json_value(transaction_data.value) as float64), 0.0)
        else null
      end as purchase_revenue,
      case
        when event_name = 'purchase' then ifnull(cast(json_value(transaction_data.shipping) as float64), 0.0)
        else null
      end as purchase_shipping,
      case
        when event_name = 'purchase' then ifnull(cast(json_value(transaction_data.tax) as float64), 0.0)
        else null
      end as purchase_tax,
      case
        when event_name = 'refund' then ifnull(cast(json_value(transaction_data.value) as float64), 0.0)
        else null
      end as refund_revenue,
      case
        when event_name = 'refund' then ifnull(cast(json_value(transaction_data.shipping) as float64), 0.0)
        else null
      end as refund_shipping,
      case
        when event_name = 'refund' then ifnull(cast(json_value(transaction_data.tax) as float64), 0.0)
        else null
      end as refund_tax,
    from transaction_data_raw
  ),

  transaction_data_def as (
    select 
      -- USER DATA
      user_date,
      client_id,
      user_id,
      user_channel_grouping,
      split(original_user_source, '.')[safe_offset(0)] as user_source,
      original_user_source,
      user_campaign,
      user_device_type,
      user_country,
      user_language,
      user_type,
      new_user,
      returning_user,
      
      -- SESSION DATA
      session_number,
      session_id,
      session_start_timestamp,
      session_channel_grouping,
      split(original_session_source, '.')[safe_offset(0)] as session_source,
      original_session_source,
      session_campaign,
      session_device_type,
      session_country,
      session_browser_name,
      session_language,
      
      -- EVENT DATA
      event_date,
      event_name,
      event_timestamp,

      -- ECOMMERCE DATA
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
    from transaction_data
    where true
      and regexp_contains(event_name, 'purchase|refund')
    group by all
  )

  select 
    -- USER DATA
    user_date,
    client_id,
    user_id,
    user_channel_grouping,
    user_source,
    original_user_source,
    user_campaign,
    user_device_type,
    user_country,
    user_language,
    user_type,
    new_user,
    returning_user,

    -- SESSION DATA
    session_number,
    session_id,
    session_start_timestamp,
    session_channel_grouping,
    session_source,
    original_session_source,
    session_campaign,
    session_device_type,
    session_country,
    session_browser_name,
    session_language,
    
    -- EVENT DATA
    event_date,
    event_name,
    event_timestamp,

    -- ECOMMERCE DATA
    transaction_id, 
    purchase,
    refund,
    transaction_currency,
    transaction_coupon,
    purchase_revenue,
    purchase_shipping,
    purchase_tax,
    refund_revenue,
    refund_shipping,
    refund_tax,
    purchase - refund as purchase_net_refund,
    ifnull(purchase_revenue, 0) - ifnull(refund_revenue, 0) as revenue_net_refund,
    ifnull(purchase_shipping, 0) + ifnull(refund_shipping, 0) as shipping_net_refund,
    ifnull(purchase_tax, 0) + ifnull(refund_tax, 0) as tax_net_refund,
  from transaction_data_def
  where true
    and event_date between start_date and end_date
);
```


### Products
Lorem ipsum

```sql
CREATE OR REPLACE TABLE FUNCTION `tom-moretti.nameless_analytics.ec_products`(start_date DATE, end_date DATE) AS (
  with product_data_raw as ( 
    select
      -- USER DATA
      user_data.user_date,
      user_data.user_id,
      user_data.client_id,
      user_data.user_first_session_timestamp,
      user_data.user_last_session_timestamp,
      days_from_first_to_last_visit,
      days_from_first_visit,
      days_from_last_visit,
      user_data.user_channel_grouping,
      user_data.user_source,
      user_data.user_campaign,
      user_data.user_device_type,
      user_data.user_country,
      user_data.user_language,
        
      -- SESSION DATA
      user_data.session_date,
      user_data.session_id, 
      user_data.session_number,
      user_data.cross_domain_session,
      user_data.session_start_timestamp,
      user_data.session_end_timestamp,
      user_data.session_duration_sec,
      user_data.session_channel_grouping,
      user_data.session_source,
      user_data.session_campaign,
      user_data.session_hostname,
      user_data.session_device_type,
      user_data.session_country,
      user_data.session_language,
      user_data.session_browser_name,
      user_data.session_landing_page_category,
      user_data.session_landing_page_location,
      user_data.session_landing_page_title,
      user_data.session_exit_page_category,
      user_data.session_exit_page_location,
      user_data.session_exit_page_title,

      -- EVENT DATA
      event_date,
      event_name,
      timestamp_millis(event_timestamp) as event_timestamp,

      -- ECOMMERCE DATA
      ecommerce as transaction_data,
      json_extract_array(ecommerce, '$.items') as items_data
      
    from `tom-moretti.nameless_analytics.users_raw_latest` (start_date, end_date, 'session_level') as user_data
      left join `tom-moretti.nameless_analytics.events` as event_data 
        on user_data.client_id = event_data.client_id
        and user_data.session_id = event_data.session_id
  ),

  product_data as (
    select 
      -- USER DATA
      user_date,
      client_id,
      user_id,
      user_channel_grouping,
      case
        when user_source = 'tagassistant.google.com' then user_source
        when net.reg_domain(user_source) is not null then net.reg_domain(user_source)
        else user_source
      end as original_user_source,
      user_campaign,
      user_device_type,
      user_country,
      user_language,
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

      -- SESSION DATA
      session_date,
      session_number,
      session_id,
      session_start_timestamp,
      session_end_timestamp,
      session_channel_grouping,
      case
        when session_source = 'tagassistant.google.com' then session_source
        when net.reg_domain(session_source) is not null then net.reg_domain(session_source)
        else session_source
      end as original_session_source,
      session_campaign,
      session_landing_page_category,
      session_landing_page_location,
      session_landing_page_title,
      session_exit_page_category,
      session_exit_page_location,
      session_exit_page_title,
      session_hostname,
      session_device_type,
      session_country,
      session_language,
      session_browser_name,

      -- EVENT DATA
      event_date,
      event_name,
      event_timestamp,

      -- ECOMMERCE DATA
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
        else null
      end as item_quantity_purchased,
      case when 
        event_name ='refund' then cast(json_value(items, '$.quantity') as int64)
        else null
      end as item_quantity_refunded,
      case when 
        event_name = 'add_to_cart' then cast(json_value(items, '$.quantity') as int64)
        else null
      end as item_quantity_added_to_cart,
      case when 
        event_name = 'remove_from_cart' then cast(json_value(items, '$.quantity') as int64)
        else null
      end as item_quantity_removed_from_cart,
      case 
        when event_name = 'purchase' then cast(json_value(items, '$.price') as float64) * cast(json_value(items, '$.quantity') as int64)
        else null
      end as item_revenue_purchased,
      case 
        when event_name = 'refund' then -cast(json_value(items, '$.price') as float64) * cast(json_value(items, '$.quantity') as int64)
        else null
      end as item_revenue_refunded,
      case 
        when event_name = 'purchase' then count(distinct json_value(items, '$.item_name'))
        else null
      end as item_unique_purchases
    from product_data_raw
      left join unnest(items_data) as items
    group by all
  ),

  product_data_def as (
    select 
      -- USER DATA
      user_date,
      client_id,
      user_id,
      user_channel_grouping,
      split(original_user_source, '.')[safe_offset(0)] as user_source,
      original_user_source,
      user_campaign,
      user_device_type,
      user_country,
      user_language,
      user_type,
      new_user,
      returning_user,
      
      -- SESSION DATA
      session_number,
      session_id,
      session_start_timestamp,
      session_channel_grouping,
      split(original_session_source, '.')[safe_offset(0)] as session_source,
      original_session_source,
      session_campaign,
      session_device_type,
      session_country,
      session_browser_name,
      session_language,

      -- EVENT DATA
      event_date,
      event_name,
      event_timestamp,

      -- ECOMMERCE DATA
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
      sum(item_price) as item_price,
      sum(item_quantity_purchased) as item_quantity_purchased,
      sum(item_quantity_refunded) as item_quantity_refunded,
      sum(item_quantity_added_to_cart) as item_quantity_added_to_cart,
      sum(item_quantity_removed_from_cart) as item_quantity_removed_from_cart,
      sum(item_revenue_purchased) as item_purchase_revenue,
      sum(item_revenue_refunded) as item_refund_revenue,
      sum(item_unique_purchases) as item_unique_purchases
    from product_data
    where true
      and regexp_contains(event_name, 'view_promotion|select_promotion|view_item_list|select_item|view_item|add_to_wishlist|add_to_cart|remove_from_cart|view_cart|begin_checkout|add_shipping_info|add_payment_info|purchase|refund')
      group by all
  )

  select 
    -- USER DATA
    user_date,
    client_id,
    user_id,
    user_channel_grouping,
    user_source,
    original_user_source,
    user_campaign,
    user_device_type,
    user_country,
    user_language,
    user_type,
    new_user,
    returning_user,

    -- SESSION DATA
    session_number,
    session_id,
    session_start_timestamp,
    session_channel_grouping,
    session_source,
    original_session_source,
    session_campaign,
    session_device_type,
    session_country,
    session_browser_name,
    session_language,
    
    -- EVENT DATA
    event_date,
    event_name,
    event_timestamp,

    -- ECOMMERCE DATA
    transaction_id, 
    list_name,
    item_list_id,
    item_list_name,
    item_affiliation,
    item_coupon,
    item_discount,
    creative_name,
    creative_slot,
    promotion_id,
    promotion_name,
    item_brand,
    item_id,
    item_name,
    item_variant,
    item_category,
    item_category_2,
    item_category_3,
    item_category_4,
    item_category_5,
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
    item_quantity_purchased,
    item_quantity_refunded,
    item_quantity_added_to_cart,
    item_quantity_removed_from_cart,
    item_purchase_revenue,
    item_refund_revenue,
    item_unique_purchases,
    ifnull(item_purchase_revenue, 0) + ifnull(item_refund_revenue, 0) as item_revenue_net_refund
  from product_data_def
  where true
);
```


### Shopping stages open funnel
Lorem ipsum

```sql
CREATE OR REPLACE TABLE FUNCTION `tom-moretti.nameless_analytics.ec_shopping_stages_open_funnel`(start_date DATE, end_date DATE) AS (
  with shopping_stage_data_raw as ( 
        select
          -- USER DATA
          client_id,
          first_value(user_id) over (partition by session_id order by event_timestamp asc) as user_id,

          -- SESSION DATA
          session_id, 
          first_value(event_timestamp) over (partition by session_id order by event_timestamp asc) as min_session_timestamp,
          first_value((select value.string from unnest (event_data) where name = 'channel_grouping')) over (partition by session_id order by event_timestamp) as session_channel_grouping,
          first_value((select value.string from unnest (event_data) where name = 'source')) over (partition by session_id order by event_timestamp) as session_source,
          first_value((select value.string from unnest (event_data) where name = 'campaign')) over (partition by session_id order by event_timestamp) as session_campaign,
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
        from `tom-moretti.nameless_analytics.events`
        where true 
          and (client_id != 'Redacted')  
          and (session_id != 'Redacted_Redacted')
      ),

      all_sessions as (
        select 
          event_date,
          client_id,
          user_id,
          session_id,
          session_channel_grouping,
          case
            when session_source = 'tagassistant.google.com' then session_source
            when net.reg_domain(session_source) is not null then net.reg_domain(session_source)
            else session_source
          end as original_session_source,
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
          user_id,
          session_id,
          session_channel_grouping,
          case
            when net.reg_domain(session_source) is not null then net.reg_domain(session_source)
            else session_source
          end as original_session_source,
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
          user_id,
          session_id,
          session_channel_grouping,
          case
            when net.reg_domain(session_source) is not null then net.reg_domain(session_source)
            else session_source
          end as original_session_source,
          session_campaign,
          session_device_type,
          session_country,
          session_browser_language,
        from shopping_stage_data_raw
        where event_name = 'add_to_cart'
        group by all
      ),

      view_cart as (
        select 
          event_date,
          client_id,
          user_id,
          session_id,
          session_channel_grouping,
          case
            when net.reg_domain(session_source) is not null then net.reg_domain(session_source)
            else session_source
          end as original_session_source,
          session_campaign,
          session_device_type,
          session_country,
          session_browser_language,
        from shopping_stage_data_raw
        where event_name = 'view_cart'
        group by all
      ),

      begin_checkout as (
        select 
          event_date,
          client_id,
          user_id,
          session_id,
          session_channel_grouping,
          case
            when net.reg_domain(session_source) is not null then net.reg_domain(session_source)
            else session_source
          end as original_session_source,
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
          user_id,
          session_id,
          session_channel_grouping,
          case
            when net.reg_domain(session_source) is not null then net.reg_domain(session_source)
            else session_source
          end as original_session_source,
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
          user_id,
          session_id,
          session_channel_grouping,
          case
            when net.reg_domain(session_source) is not null then net.reg_domain(session_source)
            else session_source
          end as original_session_source,
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
          user_id,
          session_id,
          session_channel_grouping,
          case
            when net.reg_domain(session_source) is not null then net.reg_domain(session_source)
            else session_source
          end as original_session_source,
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
          user_id,
          session_id,
          session_channel_grouping,
          original_session_source,
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
            partition by client_id, user_id, session_id, session_device_type, session_country, session_browser_language, session_channel_grouping, original_session_source, session_campaign
            order by event_date, client_id, user_id, session_id, session_device_type, session_country, session_browser_language, session_channel_grouping, original_session_source, session_campaign, step_name
          ) as step_index_next_step,
          status,
          lead(client_id, 1) over (
            partition by client_id, user_id, session_id, session_device_type, session_country, session_browser_language, session_channel_grouping, original_session_source, session_campaign
            order by event_date, client_id, user_id, session_id, session_device_type, session_country, session_browser_language, session_channel_grouping, original_session_source, session_campaign, step_name
          ) as client_id_next_step,
          lead(session_id, 1) over (
            partition by client_id, user_id, session_id, session_device_type, session_country, session_browser_language, session_channel_grouping, original_session_source, session_campaign
            order by event_date, client_id, user_id, session_id, session_device_type, session_country, session_browser_language, session_channel_grouping, original_session_source, session_campaign, step_name
          ) as session_id_next_step,
        from union_steps
      )

      select 
          event_date,
          client_id,
          user_id,
          session_id,
          session_channel_grouping,
          original_session_source,
          split(original_session_source, '.')[safe_offset(0)] as session_source,
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
      where true 
        and event_date between start_date and end_date
);
```


### Shopping stages closed funnel
Lorem ipsum

```sql
CREATE OR REPLACE TABLE FUNCTION `tom-moretti.nameless_analytics.ec_shopping_stages_closed_funnel`(start_date DATE, end_date DATE) AS (
  with shopping_stage_data_raw as ( 
    select
      -- USER DATA
      user_data.user_date,
      user_data.user_id,
      user_data.client_id,
      user_data.user_first_session_timestamp,
      user_data.user_last_session_timestamp,
      days_from_first_to_last_visit,
      days_from_first_visit,
      days_from_last_visit,
      user_data.user_channel_grouping,
      user_data.user_source,
      user_data.user_campaign,
      user_data.user_device_type,
      user_data.user_country,
      user_data.user_language,
      
      -- SESSION DATA
      user_data.session_date,
      user_data.session_id, 
      user_data.session_number,
      user_data.cross_domain_session,
      user_data.session_start_timestamp,
      user_data.session_end_timestamp,
      user_data.session_duration_sec,
      user_data.session_channel_grouping,
      user_data.session_source,
      user_data.session_campaign,
      user_data.session_hostname,
      user_data.session_device_type,
      user_data.session_country,
      user_data.session_language,
      user_data.session_browser_name,
      user_data.session_landing_page_category,
      user_data.session_landing_page_location,
      user_data.session_landing_page_title,
      user_data.session_exit_page_category,
      user_data.session_exit_page_location,
      user_data.session_exit_page_title,

      -- EVENT DATA
      event_name,
      event_date,
      -- event_timestamp,

      -- ECOMMERCE DATA
      -- (select value.json from unnest(event_data) where name = 'ecommerce') as transaction_data,

    from `tom-moretti.nameless_analytics.users_raw_latest` (start_date, end_date, 'session_level') as user_data
      left join `tom-moretti.nameless_analytics.events` as event_data 
        on user_data.client_id = event_data.client_id
        and user_data.session_id = event_data.session_id
  ),

  all_sessions as (
    select 
      event_date,
      client_id,
      user_id,
      session_id,
      session_channel_grouping,
      case
        when session_source = 'tagassistant.google.com' then session_source
        when net.reg_domain(session_source) is not null then net.reg_domain(session_source)
        else session_source
      end as original_session_source,
      session_campaign,
      session_device_type,
      session_country,
      session_language,
    from shopping_stage_data_raw
    group by all
  ),

      view_item as (
        select 
          event_date,
          client_id,
          user_id,
          session_id,
          session_channel_grouping,
          case
            when net.reg_domain(session_source) is not null then net.reg_domain(session_source)
            else session_source
          end as original_session_source,
          session_campaign,
          session_device_type,
          session_country,
          session_language,
        from shopping_stage_data_raw
        where event_name = 'view_item'
        group by all
      ),

      add_to_cart as (
        select 
          event_date,
          client_id,
          user_id,
          session_id,
          session_channel_grouping,
          case
            when net.reg_domain(session_source) is not null then net.reg_domain(session_source)
            else session_source
          end as original_session_source,
          session_campaign,
          session_device_type,
          session_country,
          session_language,
        from shopping_stage_data_raw
        where event_name = 'add_to_cart'
        group by all
      ),

      vieew_cart as (
        select 
          event_date,
          client_id,
          user_id,
          session_id,
          session_channel_grouping,
          case
            when net.reg_domain(session_source) is not null then net.reg_domain(session_source)
            else session_source
          end as original_session_source,
          session_campaign,
          session_device_type,
          session_country,
          session_language,
        from shopping_stage_data_raw
        where event_name = 'vieew_cart'
        group by all
      ),

      begin_checkout as (
        select 
          event_date,
          client_id,
          user_id,
          session_id,
          session_channel_grouping,
          case
            when net.reg_domain(session_source) is not null then net.reg_domain(session_source)
            else session_source
          end as original_session_source,
          session_campaign,
          session_device_type,
          session_country,
          session_language,
        from shopping_stage_data_raw
        where event_name = 'begin_checkout'
        group by all
      ),

      add_payment_info as (
        select 
          event_date,
          client_id,
          user_id,
          session_id,
          session_channel_grouping,
          case
            when net.reg_domain(session_source) is not null then net.reg_domain(session_source)
            else session_source
          end as original_session_source,
          session_campaign,
          session_device_type,
          session_country,
          session_language,
        from shopping_stage_data_raw
        where event_name = 'add_payment_info'
        group by all
      ),

      add_shipping_info as (
        select 
          event_date,
          client_id,
          user_id,
          session_id,
          session_channel_grouping,
          case
            when net.reg_domain(session_source) is not null then net.reg_domain(session_source)
            else session_source
          end as original_session_source,
          session_campaign,
          session_device_type,
          session_country,
          session_language,
        from shopping_stage_data_raw
        where event_name = 'add_shipping_info'
        group by all
      ),

      purchase as (
        select 
          event_date,
          client_id,
          user_id,
          session_id,
          session_channel_grouping,
          case
            when net.reg_domain(session_source) is not null then net.reg_domain(session_source)
            else session_source
          end as original_session_source,
          session_campaign,
          session_device_type,
          session_country,
          session_language,
        from shopping_stage_data_raw
        where event_name = 'purchase'
        group by all
      ),

      join_steps as (
        select 
          all_sessions.event_date,
          all_sessions.client_id,
          all_sessions.user_id,
          all_sessions.session_id,
          all_sessions.session_channel_grouping,
          all_sessions.original_session_source,
          split(all_sessions.original_session_source, '.')[safe_offset(0)] as session_source,
          all_sessions.session_campaign,
          all_sessions.session_device_type,
          all_sessions.session_country,
          all_sessions.session_language,

          all_sessions.client_id as all_sessions_client_id,
          view_item.client_id as view_item_client_id,
          add_to_cart.client_id as add_to_cart_client_id,
          begin_checkout.client_id as begin_checkout_client_id,
          add_shipping_info.client_id as add_shipping_info_client_id,
          add_payment_info.client_id as add_payment_info_client_id,
          purchase.client_id as purchase_client_id,

          all_sessions.user_id as all_sessions_user_id,
          view_item.user_id as view_item_user_id,
          add_to_cart.user_id as add_to_cart_user_id,
          begin_checkout.user_id as begin_checkout_user_id,
          add_shipping_info.user_id as add_shipping_info_user_id,
          add_payment_info.user_id as add_payment_info_user_id,
          purchase.user_id as purchase_user_id,

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
          unpivot((client_id, user_id, session_id) for step_name in (
            (all_sessions_client_id, all_sessions_user_id, all_sessions_sessions) as "0 - All",
            (view_item_client_id, view_item_user_id, view_item_sessions) as "1 - View item",
            (add_to_cart_client_id, add_to_cart_user_id, add_to_cart_sessions) as "2 - Add to cart",
            (begin_checkout_client_id, begin_checkout_user_id, begin_checkout_sessions) as "3 - Begin checkout",
            (add_shipping_info_client_id, add_shipping_info_user_id, add_shipping_info_sessions) as "4 - Add shipping info",
            (add_payment_info_client_id, add_payment_info_user_id, add_payment_info_sessions) as "5 - Add payment info",
            (purchase_client_id, purchase_user_id, purchase_sessions) as "6 - Purchase"
          ))
      )

      select
        event_date,
        client_id,
        user_id,
        session_id,
        session_channel_grouping,
        original_session_source,
        session_source,
        session_campaign,
        session_device_type,
        session_country,
        session_language,
        step_name,
        lead(client_id, 1) over (
          partition by client_id, user_id, session_id, session_device_type, session_country, session_language, session_channel_grouping, original_session_source, session_source, session_campaign
          order by event_date, client_id, session_id, session_device_type, session_country, session_language, session_channel_grouping, original_session_source,session_source, session_campaign, step_name
        ) as client_id_next_step,
        lead(user_id, 1) over (
          partition by client_id, user_id, session_id, session_device_type, session_country, session_language, session_channel_grouping, original_session_source, session_source, session_campaign
          order by event_date, client_id, session_id, session_device_type, session_country, session_language, session_channel_grouping, original_session_source, session_source, session_campaign, step_name
        ) as user_id_next_step,
        lead(session_id, 1) over (
          partition by client_id, user_id, session_id, session_device_type, session_country, session_language, session_channel_grouping, original_session_source, session_source, session_campaign
          order by event_date, client_id, session_id, session_device_type, session_country, session_language, session_channel_grouping, original_session_source, session_source, session_campaign, step_name
        ) as session_id_next_step
      from steps_pivot
      where true 
        and event_date between start_date and end_date
      group by all
);
```


### GTM performances
Lorem ipsum

```sql
CREATE OR REPLACE TABLE FUNCTION `tom-moretti.nameless_analytics.gtm_performances`(start_date DATE, end_date DATE) AS (
  SELECT
    -- USER DATA
    user_data.user_date,
    user_data.user_id,
    user_data.client_id,
    user_data.user_first_session_timestamp,
    user_data.user_last_session_timestamp,
    days_from_first_to_last_visit,
    days_from_first_visit,
    days_from_last_visit,
    user_data.user_channel_grouping,
    user_data.user_source,
    user_data.user_campaign,
    user_data.user_device_type,
    user_data.user_country,
    user_data.user_language,
    
    -- SESSION DATA
    user_data.session_date,
    user_data.session_id, 
    user_data.session_number,
    user_data.cross_domain_session,
    user_data.session_start_timestamp,
    user_data.session_end_timestamp,
    user_data.session_duration_sec,
    user_data.session_channel_grouping,
    user_data.session_source,
    user_data.session_campaign,
    user_data.session_hostname,
    user_data.session_device_type,
    user_data.session_country,
    user_data.session_language,
    user_data.session_browser_name,
    user_data.session_landing_page_category,
    user_data.session_landing_page_location,
    user_data.session_landing_page_title,
    user_data.session_exit_page_category,
    user_data.session_exit_page_location,
    user_data.session_exit_page_title,

    -- EVENT DATA
    events.event_date,
    events.event_datetime,
    events.event_timestamp,
    events.processing_event_timestamp,
    events.processing_event_timestamp - events.event_timestamp AS delay_in_milliseconds,
    (events.processing_event_timestamp - events.event_timestamp) / 1000 AS delay_in_seconds,
    events.event_origin,
    events.content_length,
    (SELECT value.string FROM UNNEST(events.event_data) WHERE name = 'page_hostname') AS cs_hostname,
    (SELECT value.string FROM UNNEST(events.event_data) WHERE name = 'ss_hostname') AS ss_hostname,
    (SELECT value.string FROM UNNEST(events.event_data) WHERE name = 'cs_container_id') AS cs_container_id,
    (SELECT value.string FROM UNNEST(events.event_data) WHERE name = 'ss_container_id') AS ss_container_id,
    ROW_NUMBER() OVER (PARTITION BY user_data.client_id, user_data.session_id ORDER BY events.event_timestamp ASC) AS hit_number,
    events.event_name,
    events.event_id,
    ARRAY_AGG(
        STRUCT(
            event_data.name,
            event_data.value.string as string_value,
            event_data.value.int as int_value,
            event_data.value.float as float_value,
            TO_JSON_STRING(event_data.value.json) AS json_value
        )
    ) AS event_data,
    TO_JSON_STRING(ecommerce) as ecommerce,
    TO_JSON_STRING(dataLayer) as dataLayer
  FROM `tom-moretti.nameless_analytics.users_raw_latest`(start_date, end_date, 'session_level') AS user_data
  LEFT JOIN `tom-moretti.nameless_analytics.events` AS events
    ON user_data.client_id = events.client_id AND user_data.session_id = events.session_id
    CROSS JOIN UNNEST(events.event_data) AS event_data
  GROUP BY all
);
```


### Consents
Lorem ipsum

```sql
CREATE OR REPLACE TABLE FUNCTION `tom-moretti.nameless_analytics.consents`(start_date DATE, end_date DATE) AS (
  with consent_data as (
    SELECT
      session_date,
      session_id,
      session_channel_grouping, 
      original_session_source,
      session_source, 
      session_campaign, 
      session_device_type, 
      session_country, 
      session_language,
      case 
        when consent_expressed = 'Yes' then 'Consent expressed'
        else'Consent not expressed'
      end as consent_state,
      consent AS consent_name,
      sum(value) AS consent_value_int_accepted
    FROM `tom-moretti.nameless_analytics.sessions`(start_date, end_date)
    UNPIVOT (
      value FOR consent IN (session_ad_user_data, session_ad_personalization, session_ad_storage, session_analytics_storage, session_functionality_storage, session_personalization_storage, session_security_storage)
    )
    group by all
  )

  select 
      session_date,
      session_id,
      session_channel_grouping, 
      original_session_source,
      session_source, 
      session_campaign, 
      session_device_type, 
      session_country, 
      session_language,
      consent_state,
      case 
        when consent_state = 'Consent expressed' then session_id
        else null
      end as session_id_consent_expressed,
      case 
        when consent_state = 'Consent not expressed' then session_id
        else null
      end as session_id_consent_not_expressed,
      consent_name,
      case 
        when consent_state = 'Consent expressed' and consent_value_int_accepted = 1 then 'Granted'
        when consent_state = 'Consent expressed' and consent_value_int_accepted = 0 then 'Denied'
        -- when consent_state = 'Consent not expressed' then ''
      end as consent_value_string,
      consent_value_int_accepted,
      case 
        when consent_state = 'Consent expressed' and consent_name = "session_ad_user_data" and consent_value_int_accepted = 0 then 1
        when consent_state = 'Consent expressed' and consent_name = "session_ad_personalization" and consent_value_int_accepted = 0 then 1
        when consent_state = 'Consent expressed' and consent_name = "session_ad_storage" and consent_value_int_accepted = 0 then 1
        when consent_state = 'Consent expressed' and consent_name = "session_analytics_storage" and consent_value_int_accepted = 0 then 1
        when consent_state = 'Consent expressed' and consent_name = "session_functionality_storage" and consent_value_int_accepted = 0 then 1
        when consent_state = 'Consent expressed' and consent_name = "session_personalization_storage" and consent_value_int_accepted = 0 then 1
        when consent_state = 'Consent expressed' and consent_name = "session_security_storage" and consent_value_int_accepted = 0 then 1
        else 0
      end as consent_value_int_denied,
  from consent_data
  group by all
);
```



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
