CREATE OR REPLACE TABLE FUNCTION `tom-moretti.nameless_analytics.gtm_performances`(start_date DATE, end_date DATE) AS (
with db as (
        SELECT 
          event_date,
          event_datetime,
          event_timestamp,
          processing_event_timestamp,
          processing_event_timestamp - event_timestamp AS delay_in_milliseconds,
          event_origin,
          content_length,
          client_id,
          user_id,
          session_id,
          (SELECT value.string FROM UNNEST(event_data) WHERE name = 'page_hostname') as hostname,
          (SELECT value.string FROM UNNEST(event_data) WHERE name = 'ss_hostname') as ss_hostname,
          (SELECT value.string FROM UNNEST(event_data) WHERE name = 'cs_container_id') as cs_container_id,
          (SELECT value.string FROM UNNEST(event_data) WHERE name = 'ss_container_id') as ss_container_id,
          event_name,
          (SELECT value.string FROM UNNEST(event_data) WHERE name = 'event_id') as event_id,
          name,
          value.string as string_value,
          value.int as int_value,
          value.float as float_value,
          if(TO_JSON_STRING(value.json) != 'null', TO_JSON_STRING(value.json), null) as json_value,
        from `tom-moretti.nameless_analytics.events`
          cross join unnest(event_data)
        where true 
          and event_date between start_date and end_date
      )

      select 
        event_date,
        event_datetime,
        event_timestamp,
        processing_event_timestamp,
        delay_in_milliseconds,
        delay_in_milliseconds / 1000 as delay_in_seconds,
        event_origin,
        content_length,
        client_id,
        user_id,
        session_id,
        hostname,
        ss_hostname,
        cs_container_id,
        ss_container_id,
        case 
          when (client_id != 'Redacted' and client_id is not null) then rank() over (partition by client_id, session_id order by event_timestamp asc)
          else null
        end 
        as hit_number,
        event_name,
        event_id,
        array_agg(
          struct(
            name,
            string_value,
            int_value,
            float_value,
            json_value
          )
        ) as event_data
      from db
      where true 
      and event_date between start_date and end_date
      group by all
);
