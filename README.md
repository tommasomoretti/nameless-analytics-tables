# nameless-analytics-reporting-queries

## The main table

This is the schema of the main table. 

<img width="1412" alt="Screenshot 2024-06-02 alle 20 03 35" src="https://github.com/tommasomoretti/nameless-analytics-reporting-queries/assets/29273232/6c803b6b-cfdc-4573-8782-b8f14abec92f">

The tags does not create any table in Google Big Query so you have to create it manually. 

``` sql
CREATE TABLE IF NOT EXISTS tom-moretti.nameless_analytics.hits (
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
OPTIONS (description = 'Nameless Analytics | Main table')
```

## Query examples
Here some query examples for make [user](https://github.com/tommasomoretti/nameless-analytics-reporting-queries?tab=readme-ov-file#users), [session](https://github.com/tommasomoretti/nameless-analytics-reporting-queries?tab=readme-ov-file#sessions), [pages](https://github.com/tommasomoretti/nameless-analytics-reporting-queries?tab=readme-ov-file#pages), [transactions](https://github.com/tommasomoretti/nameless-analytics-reporting-queries?tab=readme-ov-file#ecommerce-transactions) and [products](https://github.com/tommasomoretti/nameless-analytics-reporting-queries?tab=readme-ov-file#ecommerce-products) reports. 

### Users

``` sql
with user_data_raw as ( 
  select
    -- USER DATA
    client_id,
    --- (select value.string from unnest (user_data) where name = 'user_id') as user_id,
    first_value(event_timestamp) over (partition by client_id order by event_timestamp asc) as min_user_timestamp,
    first_value(event_timestamp) over (partition by client_id order by event_timestamp desc) as max_user_timestamp,
    first_value((select value.string from unnest (event_data) where name = 'channel_grouping')) over (partition by client_id order by event_timestamp asc) as user_channel_grouping,
    first_value((select value.string from unnest (event_data) where name = 'source')) over (partition by client_id order by event_timestamp asc) as user_source,
    first_value((select value.string from unnest (event_data) where name = 'campaign')) over (partition by client_id order by event_timestamp asc) as user_campaign,

    -- SESSION DATA
    session_id, 
    first_value(event_timestamp) over (partition by session_id order by event_timestamp asc) as min_session_timestamp,
    first_value(event_timestamp) over (partition by session_id order by event_timestamp desc) as max_session_timestamp,

    -- EVENT DATA
    event_name,
    event_timestamp,

    -- ECOMMERCE DATA
    (select value.json from unnest(event_data) where name = 'ecommerce') as transaction_data,
    json_extract_array((select value.json from unnest(event_data) where name = 'ecommerce'), '$.items') as items_data
  from `tom-moretti.nameless_analytics.hits`
),

user_data_product_def as (
  select
    client_id,
    -- user_id,
    user_channel_grouping,
    ifnull(NET.REG_DOMAIN(user_source), 'direct') as user_source,
    user_campaign,
    timestamp_millis(min_user_timestamp) as min_user_timestamp,
    timestamp_millis(max_user_timestamp) as max_user_timestamp,
    session_id,
    timestamp_millis(min_session_timestamp) as min_session_timestamp,
    timestamp_millis(max_session_timestamp) as max_session_timestamp,
    event_name,
    event_timestamp,
    case
      when event_name = 'purchase' then json_value(transaction_data.transaction_id)
      else null
    end as purchase,
    case
      when event_name = 'refund' then json_value(transaction_data.transaction_id)
      else null
    end as refund,
    case 
      when event_name = 'purchase' then cast(json_value(items, '$.price') as float64) * cast(json_value(items, '$.quantity') as int64)
      else 0
    end as item_revenue_purchased,
    case 
      when event_name = 'refund' then -cast(json_value(items, '$.price') as float64) * cast(json_value(items, '$.quantity') as int64)
      else 0
    end as item_revenue_refunded,
    case when 
      event_name = 'purchase' then cast(json_value(items, '$.quantity') as int64)
      else 0
    end as item_quantity_purchased,
    case when 
      event_name ='refund' then cast(json_value(items, '$.quantity') as int64)
      else 0
    end as item_quantity_refunded,
  from user_data_raw
    left join unnest(items_data) as items
),
 
user_data_session_def as (
  select 
    date(min_session_timestamp) as user_acquisition_date,
    client_id,
    -- user_id,
    user_channel_grouping,
    user_source,
    user_campaign,
    max_user_timestamp,
    min_user_timestamp,
    session_id,
    dense_rank() over (partition by client_id order by min_session_timestamp asc) as session_number,
    min_session_timestamp,
    max_session_timestamp,
    TIMESTAMP_DIFF(max_session_timestamp, min_session_timestamp, MILLISECOND) / 1000 as session_duration_sec,
    countif(event_name = 'page_view') as page_view,
    count(distinct purchase) as purchase,
    count(distinct refund) as refund,
    sum(item_revenue_purchased) as item_revenue_purchased,
    sum(item_revenue_refunded) as item_revenue_refunded,
    sum(item_quantity_purchased) as item_quantity_purchased,
    sum(item_quantity_refunded) as item_quantity_refunded,
  from user_data_product_def
  group by all
),

user_data_def as(
  select 
    user_acquisition_date,
    client_id,
    -- user_id,
    case 
      when session_number = 1 then client_id
      else null
    end as new_user,
    case 
      when session_number > 1 then client_id
      else null
    end as returning_user,
    
    -- case 
    --   when purchase = 0 then client_id
    --   else null
    -- end as not_a_customer,
    -- case 
    --   when purchase >= 1 then client_id
    --   else null
    -- end as customer,

    user_channel_grouping,
    user_source,
    user_campaign,
    date_diff(DATE(max_user_timestamp), DATE(min_user_timestamp), day) + 1 as days_since_first_visit,
    count(session_id) as sessions,
    session_id,
    session_number,
    case 
      when sum(page_view) >= 2 and (avg(session_duration_sec) >= 10 or sum(purchase) >= 1) then 1
      else 0
    end as engaged_session,
    case 
    when sum(page_view) >= 2 and (avg(session_duration_sec) >= 10 or sum(purchase) >= 1) then 1
    else 0
    end as engaged_session,
    sum(page_view) as page_view,
    sum(purchase) as purchase,
    sum(refund) as refund,
    sum(item_quantity_purchased) as item_quantity_purchased,
    sum(item_quantity_refunded) as item_quantity_refunded,
    sum(item_revenue_purchased) as purchase_revenue,
    sum(item_revenue_refunded) as refund_revenue,
    sum(item_revenue_purchased) + sum(item_revenue_refunded) as total_revenue_net_refund,
  from user_data_session_def
  group by all
),

user_data as (
  select 
    user_acquisition_date,
    client_id,
    user_channel_grouping,
    user_source,
    user_campaign,
    new_user,
    returning_user,
    max(new_user) over (partition by client_id) as new_user_id,
    max(returning_user) over (partition by client_id) as returning_user_id,
    -- not_a_customer,
    -- customer,
    -- max(customer) over (partition by client_id) as customer_id,
    -- max(not_a_customer) over (partition by client_id) as not_customer_id,
    -- case 
    --   when sum(purchase) >= 1 then 'Customer'
    --   else 'Not a customer'
    -- end as customer_type,
    days_since_first_visit,
    max(days_since_first_visit) over (partition by client_id) as max_days_since_first_visit,
    sum(sessions) as sessions,
    sum(page_view) as page_view,
    sum(purchase) as purchase,
    sum(refund) as refund,
    sum(item_quantity_purchased) as item_quantity_purchased,
    sum(item_quantity_refunded) as item_quantity_refunded,
    sum(purchase_revenue) as purchase_revenue,
    sum(refund_revenue) as refund_revenue,
    sum(total_revenue_net_refund) as total_revenue_net_refund
  from user_data_def
  group by all
)

select  
  user_acquisition_date,
  client_id,
  new_user_id,
  returning_user_id,
  -- not_customer_id,
  -- customer_id,
  case 
    when sum(purchase) >= 1 then 'Customer'
    else 'Not a customer'
  end as customer_type,
  case 
    when sum(purchase) >= 1 then 1
    else 0
  end as customer,
  case 
    when sum(purchase) >= 1 then 1
    else 0
  end as not_a_customer,
  user_channel_grouping,
  user_source,
  user_campaign,
  max_days_since_first_visit,
  sum(sessions) as sessions,
  sum(purchase) / sum(sessions) as conversion_rate,
  sum(page_view) as page_view,
  sum(purchase) as purchase,
  sum(refund) as refund,
  sum(item_quantity_purchased) as item_quantity_purchased,
  sum(item_quantity_refunded) as item_quantity_refunded,
  sum(purchase_revenue) as purchase_revenue,
  sum(refund_revenue) as refund_revenue,
  sum(total_revenue_net_refund) as revenue_net_refund
from user_data
group by all
```

## Sessions
``` sql
with session_data_raw as ( 
  select
    -- USER DATA
    client_id,   
    --- (select value.string from unnest (event_data) where name = 'user_id') as user_id,

    -- SESSION DATA
    session_id, 
    first_value(event_timestamp) over (partition by session_id order by event_timestamp asc) as min_session_timestamp,
    first_value(event_timestamp) over (partition by session_id order by event_timestamp desc) as max_session_timestamp,
    first_value((select value.string from unnest (event_data) where name = 'channel_grouping')) over (partition by session_id order by event_timestamp) as session_channel_grouping,
    first_value((select value.string from unnest (event_data) where name = 'source')) over (partition by session_id order by event_timestamp) as session_source,
    first_value((select value.string from unnest (event_data) where name = 'campaign')) over (partition by session_id order by event_timestamp) as session_campaign,
    first_value((select value.string from unnest (event_data) where name = 'page_location')) over (partition by session_id order by event_timestamp) as session_landing_page_location,
    first_value((select value.string from unnest (event_data) where name = 'device_type')) over (partition by session_id order by event_timestamp) as session_device_type,
    first_value((select value.string from unnest (event_data) where name = 'country')) over (partition by session_id order by event_timestamp) as session_country,
    first_value((select value.string from unnest (event_data) where name = 'browser_name')) over (partition by session_id order by event_timestamp) as session_browser_name,
    first_value((select value.string from unnest (event_data) where name = 'browser_language')) over (partition by session_id order by event_timestamp) as session_browser_language,

    -- EVENT DATA
    event_name,
    event_date,
    event_timestamp,

    -- ECOMMERCE DATA
    (select value.json from unnest(event_data) where name = 'ecommerce') as transaction_data,
  from `tom-moretti.nameless_analytics.hits`
),

session_data_def as (
  select 
    event_date,
    client_id,
    -- user_id,
    dense_rank() over (partition by client_id order by min_session_timestamp asc) as session_number,
    session_id,
    min_session_timestamp,
    max_session_timestamp,
    session_channel_grouping,
    ifnull(NET.REG_DOMAIN(session_source), 'direct') as session_source,
    session_campaign,
    session_landing_page_location,
    session_device_type,
    session_country,
    session_browser_name,
    session_browser_language,
    event_name,
    event_timestamp,
    case
      when event_name = 'purchase' then ifnull(cast(json_value(transaction_data.value) as float64), 0.0)
      else 0.0
    end as transaction_value,
    case
      when event_name = 'purchase' then ifnull(cast(json_value(transaction_data.shipping) as float64), 0.0)
      else 0.0
    end as transaction_shipping,
    case
      when event_name = 'purchase' then ifnull(cast(json_value(transaction_data.tax) as float64), 0.0)
      else 0.0
    end as transaction_tax,
    case
      when event_name = 'refund' then -ifnull(cast(json_value(transaction_data.value) as float64), 0.0)
      else 0.0
    end as refund_value,
    case
      when event_name = 'refund' then -ifnull(cast(json_value(transaction_data.shipping) as float64), 0.0)
      else 0.0
    end as refund_shipping,
    case
      when event_name = 'refund' then -ifnull(cast(json_value(transaction_data.tax) as float64), 0.0)
      else 0.0
    end as refund_tax,
  from session_data_raw
),

session_data as (
  select
    event_date,
    client_id,
    -- user_id,
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
    session_source,
    session_campaign,
    session_device_type,
    session_country,
    session_browser_name,
    session_browser_language,
    session_landing_page_location,
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
  from session_data_def
  group by all
)

select 
  event_date,
  client_id,
  -- user_id,
  user_type,
  new_user,
  returning_user,
  session_number,
  session_id,
  min_session_timestamp,
  case 
    when session_number = 1 then 1
    else 0
  end as new_session,
  case 
    when page_view >= 2 and (session_duration_sec >= 10 or purchase >= 1) then 1
    else 0
  end as engaged_session,
  session_channel_grouping,
  session_source,
  session_campaign,
  session_device_type,
  session_country,
  session_browser_name,
  session_browser_language,
  session_landing_page_location,
  session_duration_sec,
  page_view,
  click_contact_button, 
  view_item_list,
  select_item,
  view_item,
  add_to_wishlist,
  add_to_cart,
  remove_from_cart,
  view_cart,
  begin_checkout,
  add_shipping_info,
  add_payment_info,
  purchase,
  refund,
  purchase_revenue,
  purchase_shipping,
  purchase_tax,
  refund_revenue,
  refund_shipping,
  refund_tax,
  purchase_revenue + refund_revenue as revenue_net_refund,
  purchase_shipping + refund_shipping as shipping_net_refund,
  purchase_tax + refund_tax as tax_net_refund,
from session_data
```

## Pages
```sql
with page_data_raw as ( 
  select
    -- USER DATA
    client_id,
    --- (select value.string from unnest (user_data) where name = 'user_id') as user_id,

    -- SESSION DATA
    session_id, 
    first_value(event_timestamp) over (partition by session_id order by event_timestamp asc) as min_session_timestamp,
    first_value(event_timestamp) over (partition by session_id order by event_timestamp desc) as max_session_timestamp,
    first_value((select value.string from unnest (event_data) where name = 'channel_grouping')) over (partition by session_id order by event_timestamp) as session_channel_grouping,
    first_value((select value.string from unnest (event_data) where name = 'source')) over (partition by session_id order by event_timestamp) as session_source,
    first_value((select value.string from unnest (event_data) where name = 'campaign')) over (partition by session_id order by event_timestamp) as session_campaign,
    first_value((select value.string from unnest (event_data) where name = 'page_location')) over (partition by session_id order by event_timestamp) as session_landing_page_location,
    first_value((select value.string from unnest (event_data) where name = 'device_type')) over (partition by session_id order by event_timestamp) as session_device_type,
    first_value((select value.string from unnest (event_data) where name = 'country')) over (partition by session_id order by event_timestamp) as session_country,
    first_value((select value.string from unnest (event_data) where name = 'browser_name')) over (partition by session_id order by event_timestamp) as session_browser_name,
    first_value((select value.string from unnest (event_data) where name = 'browser_language')) over (partition by session_id order by event_timestamp) as session_browser_language,

    -- EVENT DATA
    event_name,
    event_date,
    event_timestamp,
    first_value(event_timestamp) over (partition by (select value.string from unnest (event_data) where name = 'page_id') order by event_timestamp asc) as min_page_timestamp,
    first_value(event_timestamp) over (partition by (select value.string from unnest (event_data) where name = 'page_id') order by event_timestamp desc) as max_page_timestamp,
    (select value.string from unnest (event_data) where name = 'page_id') as page_id,
    (select value.string from unnest (event_data) where name = 'page_location') as page_location,
    (select value.string from unnest (event_data) where name = 'page_title') as page_title,
    (select value.string from unnest (event_data) where name = 'content_group') as content_group,
  from `tom-moretti.nameless_analytics.hits` 
),

page_data_def as(
  select 
    event_date,
    client_id,
    -- user_id,
    dense_rank() over (partition by client_id order by min_session_timestamp asc) as session_number,
    session_id,
    min_session_timestamp,
    session_channel_grouping,
    ifnull(NET.REG_DOMAIN(session_source), 'direct') as session_source,
    session_campaign,
    session_landing_page_location,
    session_device_type,
    session_country,
    session_browser_name,
    session_browser_language,
    event_name,
    event_timestamp,
    page_id,
    page_location,
    page_title,
    content_group,
    min_page_timestamp,
    max_page_timestamp,
  from page_data_raw
)

select 
  event_date,
  client_id,
  -- user_id,
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
  session_source,  
  session_campaign,
  session_device_type,
  session_country,
  session_browser_name,
  session_browser_language,
  session_landing_page_location,
  page_id,
  page_location,
  page_title,
  content_group,
  (max_page_timestamp - min_page_timestamp) / 1000 as time_on_page_sec,
  countif(event_name = 'page_view') as page_view
from page_data_def
group by all
```

## Ecommerce: Transactions
```sql
with ecommerce_data_raw as ( 
  select
    -- USER DATA
    client_id,
    --- (select value.string from unnest (user_data) where name = 'user_id') as user_id,

    -- SESSION DATA
    session_id, 
    first_value(event_timestamp) over (partition by session_id order by event_timestamp asc) as min_session_timestamp,
    first_value((select value.string from unnest (event_data) where name = 'channel_grouping')) over (partition by session_id order by event_timestamp) as session_channel_grouping,
    first_value((select value.string from unnest (event_data) where name = 'source')) over (partition by session_id order by event_timestamp) as session_source,
    first_value((select value.string from unnest (event_data) where name = 'campaign')) over (partition by session_id order by event_timestamp) as session_campaign,
    first_value((select value.string from unnest (event_data) where name = 'page_location')) over (partition by session_id order by event_timestamp) as session_landing_page_location,
    first_value((select value.string from unnest (event_data) where name = 'device_type')) over (partition by session_id order by event_timestamp) as session_device_type,
    first_value((select value.string from unnest (event_data) where name = 'country')) over (partition by session_id order by event_timestamp) as session_country,
    first_value((select value.string from unnest (event_data) where name = 'browser_name')) over (partition by session_id order by event_timestamp) as session_browser_name,
    first_value((select value.string from unnest (event_data) where name = 'browser_language')) over (partition by session_id order by event_timestamp) as session_browser_language,

    -- EVENT DATA
    event_name,
    event_date,
    event_timestamp,

    -- ECOMMERCE DATA
    (select value.json from unnest(event_data) where name = 'ecommerce') as transaction_data,
    json_extract_array((select value.json from unnest(event_data) where name = 'ecommerce'), '$.items') as items_data
  from `tom-moretti.nameless_analytics.hits` 
),

ecommerce_data_def as (
  select 
    event_date,
    client_id, 
    -- user_id,
    dense_rank() over (partition by client_id order by min_session_timestamp asc) as session_number,
    session_id,
    min_session_timestamp,
    session_channel_grouping,
    ifnull(NET.REG_DOMAIN(session_source), 'direct') as session_source,
    session_campaign,
    session_landing_page_location,
    session_device_type,
    session_country,
    session_browser_name,
    session_browser_language,
    event_name,
    event_timestamp,
    json_value(transaction_data.transaction_id) as transaction_id,
    json_value(transaction_data.item_list_id) as list_id,
    json_value(transaction_data.item_list_name) as list_name,
    json_value(transaction_data.creative_name) as creative_name,
    json_value(transaction_data.creative_slot) as creative_slot,
    json_value(transaction_data.promotion_id) as promotion_id,
    json_value(transaction_data.promotion_name) as promotion_name,
    json_value(items, '$.item_list_id') as item_list_id,
    json_value(items, '$.item_list_name') as item_list_name,
    json_value(items, '$.affiliation') as item_affiliation,
    json_value(items, '$.coupon') as item_coupon,
    cast(json_value(items, '$.discount') as float64) as item_discount,
    json_value(items, '$.item_brand') as item_brand,
    json_value(items, '$.item_id') as item_id,
    json_value(items, '$.item_name') as item_name,
    json_value(items, '$.item_variant') as item_variant,
    json_value(items, '$.item_category') as item_category,
    json_value(items, '$.item_category2') as item_category_2,
    json_value(items, '$.item_category3') as item_category_3,
    json_value(items, '$.item_category4') as item_category_4,
    json_value(items, '$.item_category5') as item_category_5,
    cast(json_value(items, '$.price') as float64) as item_price,
    case when 
      event_name = 'purchase' then cast(json_value(items, '$.quantity') as int64)
      else 0
    end as item_quantity_purchased,
    case when 
      event_name ='refund' then cast(json_value(items, '$.quantity') as int64)
      else 0
    end as item_quantity_refunded,
    case when 
      event_name = 'add_to_cart' then cast(json_value(items, '$.quantity') as int64)
      else 0
    end as item_quantity_added_to_cart,
    case when 
      event_name = 'remove_from_cart' then cast(json_value(items, '$.quantity') as int64)
      else 0
    end as item_quantity_removed_from_cart,
    case 
      when event_name = 'purchase' then cast(json_value(items, '$.price') as float64) * cast(json_value(items, '$.quantity') as int64)
      else 0
    end as item_revenue_purchased,
    case 
      when event_name = 'refund' then -cast(json_value(items, '$.price') as float64) * cast(json_value(items, '$.quantity') as int64)
      else 0
    end as item_revenue_refunded,
    case 
      when event_name = 'purchase' then count(distinct json_value(items, '$.item_name'))
      else 0 
    end as item_unique_purchases
  from ecommerce_data_raw
    left join unnest(items_data) as items
  group by all
),

ecommerce_data as (
select 
  event_date,
  client_id, 
  -- user_id,
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
  session_source,
  session_campaign,
  session_landing_page_location,
  session_device_type,
  session_country,
  session_browser_name,
  session_browser_language,
  event_timestamp,
  event_name,
  countif(event_name = "view_promotion") as view_promotion,
  countif(event_name = "select_promotion") as select_promotion,
  countif(event_name = "view_item_list") as view_item_list,
  countif(event_name = "select_item") as select_item,
  countif(event_name = "view_item") as view_item,
  countif(event_name = "add_to_wishlist") as add_to_wishlist,
  countif(event_name = "add_to_cart") as add_to_cart,
  countif(event_name = "remove_from_cart") as remove_from_cart,
  countif(event_name = "view_cart") as view_cart,
  countif(event_name = "begin_checkout") as begin_checkout,
  countif(event_name = "add_shipping_info") as add_shipping_info,
  countif(event_name = "add_payment_info") as add_payment_info,
  countif(event_name = 'purchase') as purchase,
  countif(event_name = 'refund') as refund,
  transaction_id,
  list_id,
  list_name,
  creative_name,
  creative_slot,
  promotion_id,
  promotion_name,
  item_list_id,
  item_list_name,
  item_affiliation,
  item_coupon,
  item_discount,
  item_brand,
  item_id,
  item_name,
  item_variant,
  item_category,
  item_category_2,
  item_category_3,
  item_category_4,
  item_category_5,
  sum(item_price) as item_price,
  sum(item_quantity_purchased) as item_quantity_purchased,
  sum(item_quantity_refunded) as item_quantity_refunded,
  sum(item_quantity_added_to_cart) as item_quantity_added_to_cart,
  sum(item_quantity_removed_from_cart) as item_quantity_removed_from_cart,
  sum(item_revenue_purchased) as item_purchase_revenue,
  sum(item_revenue_refunded) as item_refund_revenue,
  sum(item_unique_purchases) as item_unique_purchases
from ecommerce_data_def
where true
  and regexp_contains(event_name, 'view_promotion|select_promotion|view_item_list|select_item|view_item|add_to_wishlist|add_to_cart|remove_from_cart|view_cart|begin_checkout|add_shipping_info|add_payment_info|purchase|refund')
  group by all
)

select 
  event_date,
  client_id,
  -- user_id,
  user_type,
  new_user,
  returning_user,
  session_number,
  session_id,
  min_session_timestamp,
  session_channel_grouping,
  session_source,
  session_campaign,
  session_landing_page_location,
  session_device_type,
  session_country,
  session_browser_name,
  session_browser_language,
  purchase,
  refund,
  transaction_id, 
  view_promotion,
  select_promotion,
  view_item_list,
  select_item,
  view_item,
  add_to_wishlist,
  add_to_cart,
  remove_from_cart,
  view_cart,
  begin_checkout,
  add_shipping_info,
  add_payment_info,
  list_id,
  list_name,
  creative_name,
  creative_slot,
  promotion_id,
  promotion_name,
  item_list_id,
  item_list_name,
  item_affiliation,
  item_coupon,
  item_discount,
  item_brand,
  item_id,
  item_name,
  item_variant,
  item_category,
  item_category_2,
  item_category_3,
  item_category_4,
  item_category_5,
  item_price,
  item_quantity_purchased,
  item_quantity_refunded,
  item_quantity_added_to_cart,
  item_quantity_removed_from_cart,
  item_purchase_revenue,
  item_refund_revenue,
  item_unique_purchases,
  purchase - refund as purchase_net_refund,
  item_purchase_revenue + item_refund_revenue as item_revenue_net_refund,
from ecommerce_data
```


## Ecommerce: Products
```sql
with ecommerce_data_raw as ( 
  select
    -- USER DATA
    client_id,
    --- (select value.string from unnest (user_data) where name = 'user_id') as user_id,

    -- SESSION DATA
    session_id, 
    first_value(event_timestamp) over (partition by session_id order by event_timestamp asc) as min_session_timestamp,
    first_value((select value.string from unnest (event_data) where name = 'channel_grouping')) over (partition by session_id order by event_timestamp) as session_channel_grouping,
    first_value((select value.string from unnest (event_data) where name = 'source')) over (partition by session_id order by event_timestamp) as session_source,
    first_value((select value.string from unnest (event_data) where name = 'campaign')) over (partition by session_id order by event_timestamp) as session_campaign,
    first_value((select value.string from unnest (event_data) where name = 'page_location')) over (partition by session_id order by event_timestamp) as session_landing_page_location,
    first_value((select value.string from unnest (event_data) where name = 'device_type')) over (partition by session_id order by event_timestamp) as session_device_type,
    first_value((select value.string from unnest (event_data) where name = 'country')) over (partition by session_id order by event_timestamp) as session_country,
    first_value((select value.string from unnest (event_data) where name = 'browser_name')) over (partition by session_id order by event_timestamp) as session_browser_name,
    first_value((select value.string from unnest (event_data) where name = 'browser_language')) over (partition by session_id order by event_timestamp) as session_browser_language,

    -- EVENT DATA
    event_name,
    event_date,
    event_timestamp,

    -- ECOMMERCE DATA
    (select value.json from unnest(event_data) where name = 'ecommerce') as transaction_data,
  from `tom-moretti.nameless_analytics.hits`
),

ecommerce_data_def as (
  select 
    event_date,
    client_id, 
    -- user_id,
    dense_rank() over (partition by client_id order by min_session_timestamp asc) as session_number,
    session_id,
    min_session_timestamp,
    session_channel_grouping,
    ifnull(NET.REG_DOMAIN(session_source), 'direct') as session_source,
    session_campaign,
    session_landing_page_location,
    session_device_type,
    session_country,
    session_browser_name,
    session_browser_language,
    event_name,
    event_timestamp,
    json_value(transaction_data.transaction_id) as transaction_id,
    json_value(transaction_data.currency) as transaction_currency,
    json_value(transaction_data.coupon) as transaction_coupon,
    case
      when event_name = 'purchase' then ifnull(cast(json_value(transaction_data.value) as float64), 0.0)
      else 0.0
    end as purchase_revenue,
    case
      when event_name = 'purchase' then ifnull(cast(json_value(transaction_data.shipping) as float64), 0.0)
      else 0.0
    end as purchase_shipping,
    case
      when event_name = 'purchase' then ifnull(cast(json_value(transaction_data.tax) as float64), 0.0)
      else 0.0
    end as purchase_tax,
    case
      when event_name = 'refund' then -ifnull(cast(json_value(transaction_data.value) as float64), 0.0)
      else 0.0
    end as refund_revenue,
    case
      when event_name = 'refund' then -ifnull(cast(json_value(transaction_data.shipping) as float64), 0.0)
      else 0.0
    end as refund_shipping,
    case
      when event_name = 'refund' then -ifnull(cast(json_value(transaction_data.tax) as float64), 0.0)
      else 0.0
    end as refund_tax,
  from ecommerce_data_raw
),

ecommerce_data as (
  select 
    event_date,
    client_id, 
    -- user_id,
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
    session_source,
    session_campaign,
    session_landing_page_location,
    session_device_type,
    session_country,
    session_browser_name,
    session_browser_language,
    event_timestamp,
    event_name,
    countif(event_name = 'purchase') as purchase,
    countif(event_name = 'refund') as refund,
    transaction_id,
    transaction_currency,
    transaction_coupon,
    sum(purchase_revenue) as purchase_revenue,
    sum(purchase_shipping) as purchase_shipping,
    sum(purchase_tax) as purchase_tax,
    sum(refund_revenue) as refund_revenue,
    sum(refund_shipping) as refund_shipping,
    sum(refund_tax) as refund_tax,
  from ecommerce_data_def
  where true
    and regexp_contains(event_name, 'purchase|refund')
  group by all
)

select 
  event_date,
  client_id,
  -- user_id,
  user_type,
  new_user,
  returning_user,
  session_number,
  session_id,
  min_session_timestamp,
  session_channel_grouping,
  session_source,
  session_campaign,
  session_landing_page_location,
  session_device_type,
  session_country,
  session_browser_name,
  session_browser_language,
  purchase,
  refund,
  transaction_id, 
  transaction_currency,
  transaction_coupon,
  purchase_revenue,
  purchase_shipping,
  purchase_tax,
  refund_revenue,
  refund_shipping,
  refund_tax,
  purchase - refund as purchase_net_refund,
  purchase_revenue + refund_revenue as revenue_net_refund,
  purchase_shipping + refund_shipping as shipping_net_refund,
  purchase_tax + refund_tax as tax_net_refund,
from ecommerce_data
```

## Ecommerce: user behaviour funnel


