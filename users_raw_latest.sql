CREATE OR REPLACE TABLE FUNCTION `tom-moretti.nameless_analytics.users_raw_latest`(start_date DATE, end_date DATE, user_session_scope_param STRING) AS (
  with users_raw as (
    select
      document_name,
      document_id,
      FIRST_VALUE(timestamp) OVER (PARTITION BY document_name ORDER BY timestamp DESC) AS timestamp,
      FIRST_VALUE(event_id) OVER (PARTITION BY document_name ORDER BY timestamp DESC) AS event_id,
      FIRST_VALUE(operation) OVER (PARTITION BY document_name ORDER BY timestamp DESC) AS operation,
      FIRST_VALUE(data) OVER (PARTITION BY document_name ORDER BY timestamp DESC) AS data,
      FIRST_VALUE(old_data) OVER (PARTITION BY document_name ORDER BY timestamp DESC) AS old_data,
      FIRST_VALUE(operation) OVER (PARTITION BY document_name ORDER BY timestamp DESC) = "DELETE" AS is_deleted
    from `tom-moretti.nameless_analytics.users_raw_changelog`
  )

  select 
    # USER DATA
    date(JSON_VALUE(data, '$.user_date')) as user_date,
    JSON_VALUE(data, '$.user_id') as user_id,
    JSON_VALUE(data, '$.client_id') as client_id,
    timestamp_millis(cast(JSON_VALUE(data, '$.user_first_session_timestamp') as int64)) as user_first_session_timestamp,
    timestamp_millis(cast(JSON_VALUE(data, '$.user_last_session_timestamp') as int64)) as user_last_session_timestamp,
    datetime_diff(timestamp_micros(cast(JSON_VALUE(data, '$.user_last_session_timestamp') as int64)), timestamp_micros(cast(JSON_VALUE(data, '$.user_first_session_timestamp') as int64)), day) as days_from_first_to_last_visit,
    datetime_diff(current_timestamp(), timestamp_millis(cast(JSON_VALUE(data, '$.user_last_session_timestamp') as int64)), day) as days_from_first_visit,
    datetime_diff(current_timestamp(), timestamp_millis(cast(JSON_VALUE(data, '$.user_last_session_timestamp') as int64)), day) as days_from_last_visit,
    JSON_VALUE(data, '$.user_channel_grouping') as user_channel_grouping,
    JSON_VALUE(data, '$.user_source') as user_source,
    JSON_VALUE(data, '$.user_campaign') as user_campaign,
    JSON_VALUE(data, '$.user_campaign_id') as user_campaign_id,
    JSON_VALUE(data, '$.user_device_type') as user_device_type,
    JSON_VALUE(data, '$.user_country') as user_country,
    JSON_VALUE(data, '$.user_language') as user_language,
    # Add custom values here
    # JSON_VALUE(data, '$.custom_user_param') as custom_user_param,
  
    # SESSION DATA
    date(JSON_VALUE(session_data, '$.session_date')) as session_date,
    JSON_VALUE(session_data, '$.session_id') as session_id,
    cast(JSON_VALUE(session_data, '$.session_number') as int64) as session_number,
    JSON_VALUE(session_data, '$.cross_domain_session') as cross_domain_session,
    timestamp_millis(cast(JSON_VALUE(session_data, '$.session_start_timestamp') as int64)) as session_start_timestamp,
    timestamp_millis(cast(JSON_VALUE(session_data, '$.session_end_timestamp') as int64)) as session_end_timestamp, 
    datetime_diff(timestamp_millis(cast(JSON_VALUE(session_data, '$.session_end_timestamp') as int64)), timestamp_millis(cast(JSON_VALUE(session_data, '$.session_start_timestamp') as int64)), second) as session_duration_sec,
    JSON_VALUE(session_data, '$.session_channel_grouping') as session_channel_grouping,
    JSON_VALUE(session_data, '$.session_source') as session_source,
    JSON_VALUE(session_data, '$.session_campaign') as session_campaign,
    JSON_VALUE(session_data, '$.session_campaign_id') as session_campaign_id,
    JSON_VALUE(session_data, '$.session_device_type') as session_device_type,
    JSON_VALUE(session_data, '$.session_country') as session_country,
    JSON_VALUE(session_data, '$.session_language') as session_language,
    JSON_VALUE(session_data, '$.session_hostname') as session_hostname,
    JSON_VALUE(session_data, '$.session_browser_name') as session_browser_name,
    JSON_VALUE(session_data, '$.session_landing_page_category') as session_landing_page_category,
    JSON_VALUE(session_data, '$.session_landing_page_location') as session_landing_page_location,
    JSON_VALUE(session_data, '$.session_landing_page_title') as session_landing_page_title,
    JSON_VALUE(session_data, '$.session_exit_page_category') as session_exit_page_category,
    JSON_VALUE(session_data, '$.session_exit_page_location') as session_exit_page_location,
    JSON_VALUE(session_data, '$.session_exit_page_title') as session_exit_page_title,
    # Add custom values here
    # JSON_VALUE(session_data, '$.custom_session_param') as custom_session_param,
    
  from users_raw,
    unnest(JSON_EXTRACT_ARRAY(PARSE_JSON(data), '$.sessions')) AS session_data
  where true 
    and not is_deleted
    and case 
      when user_session_scope_param = 'user_level' then date(JSON_VALUE(data, '$.user_date'))
      else date(JSON_VALUE(session_data, '$.session_date'))
    end between start_date and end_date
  group by all
);
