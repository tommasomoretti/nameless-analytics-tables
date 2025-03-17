CREATE OR REPLACE TABLE FUNCTION `tom-moretti.nameless_analytics.ec_transactions`(start_date DATE, end_date DATE) AS (
with ecommerce_data_raw as ( 
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
          event_timestamp,
          event_origin,

          -- ECOMMERCE DATA
          (select value.json from unnest(event_data) where name = 'ecommerce') as transaction_data,
        from `tom-moretti.nameless_analytics.events`
        where true 
          and (client_id != 'Redacted')  
          and (session_id != 'Redacted_Redacted')
      ),

      ecommerce_data_def as (
        select 
          event_date,
          client_id, 
          user_id,
          dense_rank() over (partition by client_id order by min_session_timestamp asc) as session_number,
          session_id,
          min_session_timestamp,
          session_channel_grouping,
          case
            when session_source = 'tagassistant.google.com' then session_source
            when net.reg_domain(session_source) is not null then net.reg_domain(session_source)
            else session_source
          end as original_session_source,
          session_campaign,
          session_device_type,
          session_country,
          session_browser_name,
          session_browser_language,
          event_name,
          event_timestamp,
          event_origin,
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
        from ecommerce_data_raw
      ),

      ecommerce_data as (
        select 
          event_date,
          client_id, 
          user_id,
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
          original_session_source,
          split(original_session_source, '.')[safe_offset(0)] as session_source,
          session_campaign,
          session_device_type,
          session_country,
          session_browser_name,
          session_browser_language,
          event_name,
          event_timestamp,
          event_origin,
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
        user_id,
        user_type,
        new_user,
        returning_user,
        session_number,
        session_id,
        min_session_timestamp,
        session_channel_grouping,
        original_session_source,
        session_source,
        session_campaign,
        session_device_type,
        session_country,
        session_browser_name,
        session_browser_language,
        event_origin,
        event_name,
        event_timestamp,
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
      from ecommerce_data
      where true
        and event_date between start_date and end_date
);
