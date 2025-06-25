CREATE OR REPLACE TABLE FUNCTION `tom-moretti.nameless_analytics.gtm_performances`(start_date DATE, end_date DATE) AS (
  SELECT
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
    events.event_date,
    events.event_datetime,
    events.event_timestamp,
    events.processing_event_timestamp,
    events.processing_event_timestamp - events.event_timestamp AS delay_in_milliseconds,
    (events.processing_event_timestamp - events.event_timestamp) / 1000 AS delay_in_seconds,
    events.event_origin,
    events.content_length,
    (SELECT value.string FROM UNNEST(events.event_data) WHERE name = 'page_hostname') AS cs_hostname,
    (SELECT value.string FROM UNNEST(events.event_data) WHERE name = 'ss_hostname') AS ss_hostname,
    (SELECT value.string FROM UNNEST(events.event_data) WHERE name = 'cs_container_id') AS cs_container_id,
    (SELECT value.string FROM UNNEST(events.event_data) WHERE name = 'ss_container_id') AS ss_container_id,
    ROW_NUMBER() OVER (PARTITION BY user_data.client_id, user_data.session_id ORDER BY events.event_timestamp ASC) AS hit_number,
    events.event_name,
    events.event_id,
    ARRAY_AGG(
        STRUCT(
            event_data.name,
            event_data.value.string as string_value,
            event_data.value.int as int_value,
            event_data.value.float as float_value,
            TO_JSON_STRING(event_data.value.json) AS json_value
        )
    ) AS event_data,
    TO_JSON_STRING(ecommerce) as ecommerce,
    TO_JSON_STRING(dataLayer) as dataLayer
  FROM `tom-moretti.nameless_analytics.users_raw_latest`(start_date, end_date, 'session_level') AS user_data
  LEFT JOIN `tom-moretti.nameless_analytics.events` AS events
    ON user_data.client_id = events.client_id AND user_data.session_id = events.session_id
    CROSS JOIN UNNEST(events.event_data) AS event_data
  GROUP BY all
);
