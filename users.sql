CREATE OR REPLACE TABLE FUNCTION `tom-moretti.nameless_analytics.users`(start_date DATE, end_date DATE) AS (
with user_data_raw as ( 
        select   
          -- USER DATA
          client_id,
          first_value(user_id) over (partition by session_id order by event_timestamp asc) as user_id,

          first_value(event_timestamp) over (partition by client_id order by event_timestamp asc) as min_user_timestamp,
          first_value(event_timestamp) over (partition by client_id order by event_timestamp desc) as max_user_timestamp,
          first_value((select value.string from unnest (event_data) where name = 'channel_grouping')) over (partition by client_id order by event_timestamp asc) as user_channel_grouping,
          first_value((select value.string from unnest (event_data) where name = 'source')) over (partition by client_id order by event_timestamp asc) as user_source,
          first_value((select value.string from unnest (event_data) where name = 'campaign')) over (partition by client_id order by event_timestamp asc) as user_campaign,
          first_value((select value.string from unnest (event_data) where name = 'country')) over (partition by client_id order by event_timestamp) as user_country,

          -- SESSION DATA
          session_id, 
          first_value(event_timestamp) over (partition by session_id order by event_timestamp asc) as min_session_timestamp,
          first_value(event_timestamp) over (partition by session_id order by event_timestamp desc) as max_session_timestamp,

          -- EVENT DATA
          event_name,
          event_date,
          event_timestamp,

          -- ECOMMERCE DATA
          if(event_name = 'purchase', event_timestamp, null) as purchase_timestamp,
          (select value.json from unnest(event_data) where name = 'ecommerce') as transaction_data,
          json_extract_array((select value.json from unnest(event_data) where name = 'ecommerce'), '$.items') as items_data
        from `tom-moretti.nameless_analytics.events`
        where true 
          and (client_id != 'Redacted')  
          and (session_id != 'Redacted_Redacted')
      ),

      user_data_event_def as (
        select
          event_date,
          client_id,
          user_id,
          user_channel_grouping,
          case
            when user_source = 'tagassistant.google.com' then user_source
            when net.reg_domain(user_source) is not null then net.reg_domain(user_source)
            else user_source
          end as original_user_source,
          user_campaign,
          user_country,
          timestamp_millis(min_user_timestamp) as min_user_timestamp,
          timestamp_millis(max_user_timestamp) as max_user_timestamp,
          session_id,
          timestamp_millis(min_session_timestamp) as min_session_timestamp,
          timestamp_millis(max_session_timestamp) as max_session_timestamp,
          event_name,
          event_timestamp,
          purchase_timestamp,
          timestamp_millis(min(purchase_timestamp) over (partition by client_id)) as min_purchase_timestamp,
          timestamp_millis(max(purchase_timestamp) over (partition by client_id)) as max_purchase_timestamp,
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
      
      user_data_session_def as (
        select 
          event_date,
          client_id,
          user_id,
          user_channel_grouping,
          original_user_source,
          split(original_user_source, '.')[safe_offset(0)] as user_source,
          user_campaign,
          user_country,
          max_user_timestamp,
          min_user_timestamp,
          session_id,
          dense_rank() over (partition by client_id order by min_session_timestamp asc) as session_number,
          min_session_timestamp,
          max_session_timestamp,
          event_name,
          timestamp_diff(max_session_timestamp, min_session_timestamp, MILLISECOND) / 1000 as session_duration_sec,
          countif(event_name = 'page_view') as page_view,
          countif(event_name = 'purchase') as purchase,
          countif(event_name = 'refund') as refund,
          min_purchase_timestamp,
          max_purchase_timestamp,
          sum(item_revenue_purchased) as item_revenue_purchased,
          sum(item_revenue_refunded) as item_revenue_refunded,
          sum(item_quantity_purchased) as item_quantity_purchased,
          sum(item_quantity_refunded) as item_quantity_refunded,
        from user_data_event_def
        group by all
      ),

      user_data_def as(
        select 
          event_date,    
          client_id,
          case 
            when session_number = 1 then client_id
            else null
          end as new_user,
          case 
            when session_number > 1 then client_id
            else null
          end as returning_user,
          user_id,
          user_channel_grouping,
          original_user_source,
          user_source,
          user_campaign,
          user_country,
          min_user_timestamp,
          max_user_timestamp,
          date_diff(CURRENT_DATE(), DATE(min_user_timestamp), day) as days_since_first_visit,
          date_diff(CURRENT_DATE(), date(max_user_timestamp), day) as days_from_last_visit,
          session_id,
          session_number,
          case 
            when sum(page_view) >= 2 and (avg(session_duration_sec) >= 10 or countif(event_name = 'purchase') >= 1) then 1
            else 0
          end as engaged_session,
          sum(page_view) as page_view,
          date_diff(CURRENT_DATE(), DATE(min_purchase_timestamp), day) as days_since_first_purchase,
          date_diff(CURRENT_DATE(), date(max_purchase_timestamp), day) as days_from_last_purchase,
          purchase,
          refund,
          sum(item_quantity_purchased) as item_quantity_purchased,
          sum(item_quantity_refunded) as item_quantity_refunded,
          sum(item_revenue_purchased) as purchase_revenue,
          sum(item_revenue_refunded) as refund_revenue,
          sum(item_revenue_purchased) + sum(item_revenue_refunded) as revenue_net_refund,
          ifnull(safe_divide(sum(item_revenue_purchased), countif(event_name = 'purchase')), 0) as avg_purchase_value,
          ifnull(safe_divide(sum(item_revenue_refunded), countif(event_name = 'refund')), 0) as avg_refund_value
        from user_data_session_def
        group by all
      ),

      def as (
        select 
          event_date, 
          client_id,
          new_user,
          max(new_user) over (partition by client_id) as new_user_client_id,
          returning_user,
          max(returning_user) over (partition by client_id) as returning_user_client_id,
          user_id,
          user_channel_grouping,
          original_user_source,
          user_source,
          user_campaign,
          user_country,
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
          days_since_first_purchase,
          max(days_since_first_purchase) over (partition by client_id) as max_days_since_first_purchase,
          days_from_last_purchase,
          max(days_from_last_purchase) over (partition by client_id) as max_days_from_last_purchase,
          sum(purchase) as purchase,
          sum(refund) as refund,
          sum(item_quantity_purchased) as item_quantity_purchased,
          sum(item_quantity_refunded) as item_quantity_refunded,
          sum(purchase_revenue) as purchase_revenue,
          sum(refund_revenue) as refund_revenue,
          sum(revenue_net_refund) as revenue_net_refund,
          avg(avg_purchase_value) as avg_purchase_value,
          avg(avg_refund_value) as avg_refund_value
        from user_data_def
        group by all
      ),

      clustering_prep as (
        select 
          CAST(FORMAT_DATETIME('%Y-%m-%d', min_user_timestamp) as date) as user_acquisition_date,
          client_id,
          max(new_user_client_id) as new_user_client_id,
          max(returning_user_client_id) as returning_user_client_id,
          user_id,
          min_user_timestamp,
          max_user_timestamp,
          max(max_days_since_first_visit) as days_since_first_visit,
          max(max_days_from_last_visit) as days_from_last_visit,
          user_channel_grouping,
          original_user_source,
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
          max(max_days_since_first_purchase) as days_since_first_purchase,
          max(max_days_from_last_purchase) as days_from_last_purchase,
          sum(purchase) as purchase,
          sum(refund) as refund,
          sum(item_quantity_purchased) as item_quantity_purchased,
          sum(item_quantity_refunded) as item_quantity_refunded,
          sum(purchase_revenue) as purchase_revenue,
          sum(refund_revenue) as refund_revenue,
          sum(revenue_net_refund) as revenue_net_refund,
          avg(avg_purchase_value) as avg_purchase_value,
          avg(avg_refund_value) as avg_refund_value
        from def
        group by all
      ),

      clustering as (
        select 
          *,
          CASE
            WHEN max_user_timestamp is null THEN 0
            ELSE NTILE(5) OVER (ORDER BY max_user_timestamp ASC)
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
          WHEN (
            (recency_score >= 4 AND frequency_score >= 4) OR
            (recency_score >= 4 AND monetary_score >= 4) OR
            (frequency_score >= 4 AND monetary_score >= 4)
          ) THEN 'High Valuable'

          -- Mid Valuable: At least 1 dimension high or medium scores across all dimensions
          WHEN (
            (recency_score >= 3 OR frequency_score >= 3 OR monetary_score >= 3)
            AND (recency_score BETWEEN 2 AND 4 AND frequency_score BETWEEN 2 AND 4 AND monetary_score BETWEEN 2 AND 4)
          ) THEN 'Mid Valuable'

          -- Low Valuable: Low scores in at least 2 dimensions
          WHEN (
            (recency_score <= 2 AND frequency_score <= 2) OR
            (recency_score <= 2 AND monetary_score <= 2) OR
            (frequency_score <= 2 AND monetary_score <= 2)
          ) THEN 'Low Valuable'

          -- Others: Catch-all for any remaining segments
          ELSE 'Others'
        END AS rfm_cluster
      from clustering
      where user_acquisition_date between start_date and end_date
);
