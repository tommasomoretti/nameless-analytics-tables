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
