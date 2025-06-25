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
