CREATE OR REPLACE TABLE FUNCTION `tom-moretti.nameless_analytics.pages`(start_date DATE, end_date DATE) AS (
with page_data_raw as ( 
        select
          -- USER DATA
          client_id,
          first_value(user_id) over (partition by session_id order by event_timestamp asc) as user_id,

          -- SESSION DATA
          session_id, 
          first_value(event_timestamp) over (partition by session_id order by event_timestamp asc) as min_session_timestamp,
          first_value(event_timestamp) over (partition by session_id order by event_timestamp desc) as max_session_timestamp,
          first_value((select value.string from unnest (event_data) where name = 'channel_grouping')) over (partition by session_id order by event_timestamp) as session_channel_grouping,
          first_value((select value.string from unnest (event_data) where name = 'source')) over (partition by session_id order by event_timestamp) as session_source,
          first_value((select value.string from unnest (event_data) where name = 'campaign')) over (partition by session_id order by event_timestamp) as session_campaign,
          first_value((select value.string from unnest (event_data) where name = 'page_hostname')) over (partition by session_id order by event_timestamp) as session_hostname,
          first_value((select value.string from unnest (event_data) where name = 'device_type')) over (partition by session_id order by event_timestamp) as session_device_type,
          first_value((select value.string from unnest (event_data) where name = 'country')) over (partition by session_id order by event_timestamp) as session_country,
          first_value((select value.string from unnest (event_data) where name = 'browser_name')) over (partition by session_id order by event_timestamp) as session_browser_name,
          first_value((select value.string from unnest (event_data) where name = 'browser_language')) over (partition by session_id order by event_timestamp) as session_browser_language,

          -- EVENT DATA
          event_name,
          (select value.string from unnest (event_data) where name = 'event_type') as event_type,
          event_date,
          event_timestamp,
          event_origin,
          (select value.string from unnest (event_data) where name = 'page_id') as page_id,
          first_value(event_timestamp) over (partition by (select value.string from unnest (event_data) where name = 'page_id') order by event_timestamp asc) as min_page_timestamp,
          first_value(event_timestamp) over (partition by (select value.string from unnest (event_data) where name = 'page_id') order by event_timestamp desc) as max_page_timestamp,
          (select value.string from unnest (event_data) where name = 'page_location') as page_location,
          (select value.string from unnest (event_data) where name = 'page_hostname') as page_hostname,
          (select value.string from unnest (event_data) where name = 'page_title') as page_title,
          (select value.string from unnest (event_data) where name = 'content_group') as content_group,
          (select value.int from unnest (event_data) where name = 'time_to_dom_interactive') as time_to_dom_interactive,
          (select value.int from unnest (event_data) where name = 'page_render_time') as page_render_time,
          (select value.int from unnest (event_data) where name = 'time_to_dom_complete') as time_to_dom_complete,
          (select value.int from unnest (event_data) where name = 'total_page_load_time') as total_page_load_time
        from `tom-moretti.nameless_analytics.events`
        where true 
          and (client_id != 'Redacted')  
          and (session_id != 'Redacted_Redacted')
      ),

      page_data as(
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
          session_hostname,
          session_device_type,
          session_country,
          session_browser_name,
          session_browser_language,
          event_name,
          event_type,
          event_timestamp,
          event_origin,
          page_id,
          page_location,
          page_hostname,
          page_title,
          content_group,
          min_page_timestamp,
          max_page_timestamp,
          time_to_dom_interactive,
          page_render_time,
          time_to_dom_complete,
          total_page_load_time,
          countif(event_name = 'page_view') as page_view,
          countif(event_name = 'page_load_time') as page_load_time,
          countif(event_name = 'click_contact_button') as click_contact_button,
          countif(event_name = 'purchase') as purchase
        from page_data_raw
        group by all
      ),

      page_data_def as (
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
          session_hostname,
          dense_rank() over (partition by session_id order by event_timestamp asc) as page_view_number,
          event_name,
          event_type,
          event_timestamp,
          event_origin,
          page_id,
          page_location,
          page_hostname,
          page_title,
          content_group,
          (max_page_timestamp - min_page_timestamp) / 1000 as time_on_page_sec,
          time_to_dom_interactive,
          max(time_to_dom_interactive) over (partition by page_id) as max_time_to_dom_interactive,
          page_render_time,
          max(page_render_time) over (partition by page_id) as max_page_render_time,
          time_to_dom_complete,
          max(time_to_dom_complete) over (partition by page_id) as max_time_to_dom_complete,
          total_page_load_time,
          max(total_page_load_time) over (partition by page_id) as max_total_page_load_time,
          page_view,
          max(page_view) over (partition by page_id) as max_page_view,
          page_load_time,
          max(page_load_time) over(partition by page_id) as max_page_load_time,
          purchase,
          max(purchase) over (partition by page_id) as max_purchase,
          click_contact_button,
          max(click_contact_button) over (partition by page_id) as max_click_contact_button
        from page_data
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
        session_hostname,
        page_view_number,
        event_type,
        event_timestamp,
        event_origin,
        page_id,
        page_location,
        page_hostname,
        page_title,
        content_group,
        time_on_page_sec,
        max_time_to_dom_interactive / 1000 as time_to_dom_interactive_sec,
        max_page_render_time / 1000 as page_render_time_sec,
        max_time_to_dom_complete / 1000 as time_to_dom_complete_sec,
        max_total_page_load_time / 1000 as total_page_load_time_sec,
        max_page_view as page_view,
        max_page_load_time as page_load_time,
        max_purchase as purchase,
        max_click_contact_button as click_contact_button,
      from page_data_def 
      where true
      and event_date between start_date and end_date 
      and event_name = 'page_view'
);
