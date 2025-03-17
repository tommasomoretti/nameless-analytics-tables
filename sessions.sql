CREATE OR REPLACE TABLE FUNCTION `tom-moretti.nameless_analytics.sessions`(start_date DATE, end_date DATE) AS (
with session_data_raw as ( 
        select
          -- USER DATA
          client_id,
          first_value(user_id) over (partition by session_id order by event_timestamp asc) as user_id,
          first_value((select value.string from unnest (event_data) where name = 'channel_grouping')) over (partition by client_id order by event_timestamp) as user_channel_grouping,
          first_value((select value.string from unnest (event_data) where name = 'source')) over (partition by client_id order by event_timestamp) as user_source,
          first_value((select value.string from unnest (event_data) where name = 'campaign')) over (partition by client_id order by event_timestamp) as user_campaign,
          first_value((select value.string from unnest (event_data) where name = 'device_type')) over (partition by client_id order by event_timestamp) as user_device_type,
          first_value((select value.string from unnest (event_data) where name = 'country')) over (partition by client_id order by event_timestamp) as user_country,
          first_value((select value.string from unnest (event_data) where name = 'browser_language')) over (partition by session_id order by event_timestamp) as user_browser_language,

          -- SESSION DATA
          session_id, 
          first_value(event_timestamp) over (partition by session_id order by event_timestamp asc) as min_session_timestamp,
          first_value(event_timestamp) over (partition by session_id order by event_timestamp desc) as max_session_timestamp,
          first_value((select value.string from unnest (event_data) where name = 'channel_grouping')) over (partition by session_id order by event_timestamp) as session_channel_grouping,
          first_value((select value.string from unnest (event_data) where name = 'source')) over (partition by session_id order by event_timestamp) as session_source,
          first_value((select value.string from unnest (event_data) where name = 'campaign')) over (partition by session_id order by event_timestamp) as session_campaign,
          first_value((select value.string from unnest (event_data) where name = 'content_group')) over (partition by session_id order by event_timestamp) as session_landing_page_content_group,
          first_value((select value.string from unnest (event_data) where name = 'page_location')) over (partition by session_id order by event_timestamp) as session_landing_page_location,
          first_value((select value.string from unnest (event_data) where name = 'page_title')) over (partition by session_id order by event_timestamp) as session_landing_page_title,
          first_value((select value.string from unnest (event_data) where name = 'content_group')) over (partition by session_id order by event_timestamp desc) as session_exit_page_content_group,
          first_value((select value.string from unnest (event_data) where name = 'page_location')) over (partition by session_id order by event_timestamp desc) as session_exit_page_location,
          first_value((select value.string from unnest (event_data) where name = 'page_title')) over (partition by session_id order by event_timestamp desc) as session_exit_page_title,
          first_value((select value.string from unnest (event_data) where name = 'page_hostname')) over (partition by session_id order by event_timestamp) as session_hostname,
          first_value((select value.string from unnest (event_data) where name = 'device_type')) over (partition by session_id order by event_timestamp) as session_device_type,
          first_value((select value.string from unnest (event_data) where name = 'country')) over (partition by session_id order by event_timestamp) as session_country,
          first_value((select value.string from unnest (event_data) where name = 'browser_name')) over (partition by session_id order by event_timestamp) as session_browser_name,
          first_value((select value.string from unnest (event_data) where name = 'browser_language')) over (partition by session_id order by event_timestamp) as session_browser_language,

          -- EVENT DATA
          event_name,
          event_date,
          event_timestamp,
          (select value.string from unnest (event_data) where name = 'page_query') as page_query,

          -- ECOMMERCE DATA
          (select value.json from unnest(event_data) where name = 'ecommerce') as transaction_data,

          -- CONSENT DATA
          (select value.string from unnest(consent_data) where name = 'respect_consent_mode') as respect_consent_mode,
          (select value.string from unnest(consent_data) where name = 'tracking_anonimization') as tracking_anonimization,
          (select value.string from unnest(consent_data) where name = 'consent_type') as consent_type,
          (select value.string from unnest(consent_data) where name = 'ad_user_data') as ad_user_data,
          (select value.string from unnest(consent_data) where name = 'ad_personalization') as ad_personalization,
          (select value.string from unnest(consent_data) where name = 'ad_storage') as ad_storage,
          (select value.string from unnest(consent_data) where name = 'analytics_storage') as analytics_storage,
          (select value.string from unnest(consent_data) where name = 'functionality_storage') as functionality_storage,
          (select value.string from unnest(consent_data) where name = 'personalization_storage') as personalization_storage,
          (select value.string from unnest(consent_data) where name = 'security_storage') as security_storage
        from `tom-moretti.nameless_analytics.events`
        where true 
          and (client_id != 'Redacted')  
          and (session_id != 'Redacted_Redacted')
      ),

      session_data_def as (
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
          user_device_type,
          user_country,
          user_browser_language,
          dense_rank() over (partition by client_id order by min_session_timestamp asc) as session_number,
          session_id,
          min_session_timestamp,
          max_session_timestamp,
          session_channel_grouping,
          case
            when session_source = 'tagassistant.google.com' then session_source
            when net.reg_domain(session_source) is not null then net.reg_domain(session_source)
            else session_source
          end as original_session_source,
          session_campaign,
          case 
            when regexp_contains(page_query, 'na_id=') then 'Yes'
          end as is_cross_domain_session,  
          session_landing_page_location,
          session_landing_page_title,
          session_landing_page_content_group,
          session_exit_page_location,
          session_exit_page_title,
          session_exit_page_content_group,
          session_hostname,
          session_device_type,
          session_country,
          session_browser_name,
          session_browser_language,
          event_name,
          event_timestamp,
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
          respect_consent_mode,
          tracking_anonimization,
          consent_type,
          ad_user_data,
          ad_personalization,
          ad_storage,
          analytics_storage,
          functionality_storage,
          personalization_storage,
          security_storage,
          case 
            when countif(consent_type = 'Update') over (partition by session_id) > 0 then first_value(case when consent_type = 'Update' then respect_consent_mode end ignore nulls) over (partition by session_id order by event_timestamp asc rows between unbounded preceding and unbounded following)
            ELSE first_value(respect_consent_mode) over (partition by session_id order by event_timestamp asc rows between unbounded preceding and unbounded following)
          end AS     session_respect_consent_mode,

          case 
            when countif(consent_type = 'Update') over (partition by session_id) > 0 then first_value(case when consent_type = 'Update' then tracking_anonimization end ignore nulls) over (partition by session_id order by event_timestamp asc rows between unbounded preceding and unbounded following)
            ELSE first_value(tracking_anonimization) over (partition by session_id order by event_timestamp asc rows between unbounded preceding and unbounded following)
          end AS session_tracking_anonimization,

          case 
            when countif(consent_type = 'Update') over (partition by session_id) > 0 then first_value(case when consent_type = 'Update' then ad_user_data end ignore nulls) over (partition by session_id order by event_timestamp asc rows between unbounded preceding and unbounded following)
            ELSE first_value(ad_user_data) over (partition by session_id order by event_timestamp asc rows between unbounded preceding and unbounded following)
          end AS session_ad_user_data,

          case 
            when countif(consent_type = 'Update') over (partition by session_id) > 0 then first_value(case when consent_type = 'Update' then ad_personalization end ignore nulls) over (partition by session_id order by event_timestamp asc rows between unbounded preceding and unbounded following)
            ELSE first_value(ad_personalization) over (partition by session_id order by event_timestamp asc rows between unbounded preceding and unbounded following)
          end AS session_ad_personalization,

          case 
            when countif(consent_type = 'Update') over (partition by session_id) > 0 then first_value(case when consent_type = 'Update' then ad_storage end ignore nulls) over (partition by session_id order by event_timestamp asc rows between unbounded preceding and unbounded following)
            ELSE first_value(ad_storage) over (partition by session_id order by event_timestamp asc rows between unbounded preceding and unbounded following)
          end AS session_ad_storage,

          case 
            when countif(consent_type = 'Update') over (partition by session_id) > 0 then first_value(case when consent_type = 'Update' then analytics_storage end ignore nulls) over (partition by session_id order by event_timestamp asc rows between unbounded preceding and unbounded following)
            ELSE first_value(analytics_storage) over (partition by session_id order by event_timestamp asc rows between unbounded preceding and unbounded following)
          end AS session_analytics_storage,

          case 
            when countif(consent_type = 'Update') over (partition by session_id) > 0 then first_value(case when consent_type = 'Update' then functionality_storage end ignore nulls) over (partition by session_id order by event_timestamp asc rows between unbounded preceding and unbounded following)
            ELSE first_value(functionality_storage) over (partition by session_id order by event_timestamp asc rows between unbounded preceding and unbounded following)
          end AS session_functionality_storage,

          case 
            when countif(consent_type = 'Update') over (partition by session_id) > 0 then first_value(case when consent_type = 'Update' then personalization_storage end ignore nulls) over (partition by session_id order by event_timestamp asc rows between unbounded preceding and unbounded following)
            ELSE first_value(personalization_storage) over (partition by session_id order by event_timestamp asc rows between unbounded preceding and unbounded following)
          end AS session_personalization_storage,

          case 
            when countif(consent_type = 'Update') over (partition by session_id) > 0 then first_value(case when consent_type = 'Update' then security_storage end ignore nulls) over (partition by session_id order by event_timestamp asc rows between unbounded preceding and unbounded following)
            ELSE first_value(security_storage) over (partition by session_id order by event_timestamp asc rows between unbounded preceding and unbounded following)
          end AS session_security_storage,

          case 
            when countif(consent_type = 'Update') over (partition by session_id) > 0 then first_value(case when consent_type = 'Update' then event_timestamp end ignore nulls) over (partition by session_id order by event_timestamp asc rows between unbounded preceding and unbounded following)
            ELSE first_value(event_timestamp) over (partition by session_id order by event_timestamp asc rows between unbounded preceding and unbounded following)
          end AS consent_timestamp,

          case 
              when countif(consent_type = 'Update') over (partition by session_id) > 0 then 'Yes'
              else 'No'
          end as consent_expressed

        from session_data_raw
        group by all
      ),

      session_data as (
        select
          event_date,
          client_id,
          user_id,
          user_channel_grouping,
          original_user_source,
          split(original_user_source, '.')[safe_offset(0)] as user_source,
          user_campaign,
          user_device_type,
          user_country,
          user_browser_language,
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
          is_cross_domain_session,
          max(is_cross_domain_session) over (partition by session_id) as max_is_cross_domain_session,
          session_landing_page_location,
          session_landing_page_title,
          session_landing_page_content_group,
          session_exit_page_location,
          session_exit_page_title,
          session_exit_page_content_group,
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
          session_respect_consent_mode,
          session_tracking_anonimization,
          case when session_ad_user_data = 'Granted' then 1 else 0 end as session_ad_user_data,
          case when session_ad_personalization = 'Granted' then 1 else 0 end as session_ad_personalization,
          case when session_ad_storage = 'Granted' then 1 else 0 end as session_ad_storage,
          case when session_analytics_storage = 'Granted' then 1 else 0 end as session_analytics_storage,
          case when session_functionality_storage = 'Granted' then 1 else 0 end as session_functionality_storage,
          case when session_personalization_storage = 'Granted' then 1 else 0 end as session_personalization_storage,
          case when session_security_storage = 'Granted' then 1 else 0 end as session_security_storage,
          consent_timestamp,
          timestamp_millis(consent_timestamp) as consent_datetime,
          consent_expressed
        from session_data_def
        group by all
      )
      select 
        event_date,
        client_id,
        user_id,
        user_channel_grouping,
        user_source,
        user_campaign,
        user_device_type,
        user_country,
        user_browser_language,
        user_type,
        new_user,
        returning_user,
        session_number,
        session_id,
        min_session_timestamp as session_start,
        case 
          when session_number = 1 then 1
          else 0
        end as first_session,
        case 
          when sum(page_view) >= 2 and (avg(session_duration_sec) >= 10 or sum(purchase) >= 1) then 1
          else 0
        end as engaged_session,
        session_channel_grouping,
        original_session_source,
        session_source, 
        session_campaign,
        session_device_type,
        session_country,
        session_browser_name,
        session_browser_language,
        case 
          when max_is_cross_domain_session is null then 'No'
          else max_is_cross_domain_session
        end as is_cross_domain_session,
        session_landing_page_location,
        session_landing_page_title,
        session_landing_page_content_group,
        session_exit_page_location,
        session_exit_page_title,
        session_exit_page_content_group,
        session_hostname,
        avg(session_duration_sec) as session_duration_sec,
        sum(page_view) as page_view,
        sum(click_contact_button) as click_contact_button, 
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
        session_respect_consent_mode,
        session_tracking_anonimization,
        session_ad_user_data,
        session_ad_personalization,
        session_ad_storage,
        session_analytics_storage,
        session_functionality_storage,
        session_personalization_storage,
        session_security_storage,
        consent_timestamp,
        consent_datetime,
        consent_expressed
      from session_data
      where event_date between start_date and end_date
      group by all
);
