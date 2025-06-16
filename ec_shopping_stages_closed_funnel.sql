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
