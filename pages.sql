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
