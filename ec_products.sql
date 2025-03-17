CREATE OR REPLACE TABLE FUNCTION `tom-moretti.nameless_analytics.ec_products`(start_date DATE, end_date DATE) AS (
with ecommerce_data_raw as ( 
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
          event_timestamp,
          event_origin,

          -- ECOMMERCE DATA
          (select value.json from unnest(event_data) where name = 'ecommerce') as transaction_data,
          json_extract_array((select value.json from unnest(event_data) where name = 'ecommerce'), '$.items') as items_data
        from `tom-moretti.nameless_analytics.events`
        where true
          and (client_id != 'Redacted')  
          and (session_id != 'Redacted_Redacted')
      ),

      ecommerce_data_def as (
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
          session_device_type,
          session_country,
          session_browser_name,
          session_browser_language,
          event_name,
          event_timestamp,
          event_origin,
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
            else null
          end as item_quantity_purchased,
          case when 
            event_name ='refund' then cast(json_value(items, '$.quantity') as int64)
            else null
          end as item_quantity_refunded,
          case when 
            event_name = 'add_to_cart' then cast(json_value(items, '$.quantity') as int64)
            else null
          end as item_quantity_added_to_cart,
          case when 
            event_name = 'remove_from_cart' then cast(json_value(items, '$.quantity') as int64)
            else null
          end as item_quantity_removed_from_cart,
          case 
            when event_name = 'purchase' then cast(json_value(items, '$.price') as float64) * cast(json_value(items, '$.quantity') as int64)
            else null
          end as item_revenue_purchased,
          case 
            when event_name = 'refund' then -cast(json_value(items, '$.price') as float64) * cast(json_value(items, '$.quantity') as int64)
            else null
          end as item_revenue_refunded,
          case 
            when event_name = 'purchase' then count(distinct json_value(items, '$.item_name'))
            else null
          end as item_unique_purchases
        from ecommerce_data_raw
          left join unnest(items_data) as items
        group by all
      ),

      ecommerce_data as (
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
        event_name,
        event_timestamp,
        event_origin,
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
        event_name,
        event_timestamp,
        event_origin,
        transaction_id, 
        list_name,
        item_list_id,
        item_list_name,
        item_affiliation,
        item_coupon,
        item_discount,
        creative_name,
        creative_slot,
        promotion_id,
        promotion_name,
        item_brand,
        item_id,
        item_name,
        item_variant,
        item_category,
        item_category_2,
        item_category_3,
        item_category_4,
        item_category_5,
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
        item_quantity_purchased,
        item_quantity_refunded,
        item_quantity_added_to_cart,
        item_quantity_removed_from_cart,
        item_purchase_revenue,
        item_refund_revenue,
        item_unique_purchases,
        ifnull(item_purchase_revenue, 0) + ifnull(item_refund_revenue, 0) as item_revenue_net_refund
      from ecommerce_data
      where true 
        and event_date between start_date and end_date
);
