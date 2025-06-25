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
