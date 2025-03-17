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
