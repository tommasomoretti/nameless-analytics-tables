CREATE OR REPLACE TABLE FUNCTION `tom-moretti.nameless_analytics.consents`(start_date DATE, end_date DATE) AS (
with consent_data as (
    SELECT
      event_date,
      session_id,
      session_channel_grouping, 
      original_session_source,
      session_source, 
      session_campaign, 
      session_device_type, 
      session_country, 
      session_browser_language,
      case 
        when consent_expressed = 'Yes' then 'Consent expressed'
        else'Consent not expressed'
      end as consent_expressed,
      consent AS consent_name,
      sum(value) AS consent_value_int_accepted
    FROM `tom-moretti.nameless_analytics.sessions`(start_date, end_date)
    UNPIVOT (
      value FOR consent IN (session_ad_user_data, session_ad_personalization, session_ad_storage, session_analytics_storage, session_functionality_storage, session_personalization_storage, session_security_storage)
    )
    group by all
  )

  select 
      event_date,
      session_id,
      session_channel_grouping, 
      original_session_source,
      session_source, 
      session_campaign, 
      session_device_type, 
      session_country, 
      session_browser_language,
      consent_expressed,
      case 
        when consent_expressed = 'Consent expressed' then session_id
        else null
      end as session_id_consent_expressed_value_int,
      case 
        when consent_expressed = 'Consent not expressed' then session_id
        else null
      end as session_id_consent_not_expressed_value_int,
      consent_name,
      case 
        when consent_expressed = 'Consent expressed' and consent_value_int_accepted = 1 then 'Granted'
        when consent_expressed = 'Consent expressed' and consent_value_int_accepted = 0 then 'Denied'
        -- when consent_expressed = 'Consent not expressed' then ''
      end as consent_value_string,
      consent_value_int_accepted,
      case 
        when consent_expressed = 'Consent expressed' and consent_name = "session_ad_user_data" and consent_value_int_accepted = 0 then 1
        when consent_expressed = 'Consent expressed' and consent_name = "session_ad_personalization" and consent_value_int_accepted = 0 then 1
        when consent_expressed = 'Consent expressed' and consent_name = "session_ad_storage" and consent_value_int_accepted = 0 then 1
        when consent_expressed = 'Consent expressed' and consent_name = "session_analytics_storage" and consent_value_int_accepted = 0 then 1
        when consent_expressed = 'Consent expressed' and consent_name = "session_functionality_storage" and consent_value_int_accepted = 0 then 1
        when consent_expressed = 'Consent expressed' and consent_name = "session_personalization_storage" and consent_value_int_accepted = 0 then 1
        when consent_expressed = 'Consent expressed' and consent_name = "session_security_storage" and consent_value_int_accepted = 0 then 1
        else 0
      end as consent_value_int_denied,
  from consent_data
  group by all
);
