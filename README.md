# nameless-analytics-reporting-queries

``` sql
CREATE TABLE IF NOT EXISTS `project.dataset.table_name` ( # Insert your path here
  event_date DATE OPTIONS (description = 'The date of the request'),
  client_id STRING OPTIONS (description = 'The client id of the user'),
  session_id STRING OPTIONS (description = 'The session id of the user'),
  event_name STRING OPTIONS (description = 'The event name of the request'),
  event_timestamp INT64 OPTIONS (description = 'The insert timestamp of the inserted request in BigQuery'),
  event_data ARRAY <
    STRUCT < 
      name STRING OPTIONS (description = 'Event data parameter name'),
      value STRUCT < 
        string STRING OPTIONS (description = 'Event data parameter string value'),
        int INT64 OPTIONS (description = 'Event data parameter int number value'),
        float FLOAT64 OPTIONS (description = 'Event data parameter float number value'),
        json JSON OPTIONS (description = 'Event data parameter JSON value')
      > OPTIONS (description = 'Event data parameter value name')
    >
  > OPTIONS (description = 'Event data'),
  consent_data ARRAY <
    STRUCT < 
      name STRING OPTIONS (description = 'Consent data parameter name'),
      value BOOL OPTIONS (description = 'Consent data parameter boolean value')
    >
  > OPTIONS (description = 'Consent data')
) 

PARTITION BY event_date
CLUSTER BY client_id, session_id 
OPTIONS (
  description = 'BQ Analytics'
)
```
