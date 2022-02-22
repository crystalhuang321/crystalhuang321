--How to use optimizely.public.opendoor_decisions and variation_ids 
--to do manual calculations for experiment metrics

--------------------------------
--Optimizely experiment link: https://app.optimizely.com/v2/projects/15794150625/experiments/21082930044/api_names

/* Various Notes */
--Metric being calculated: register (at onboarding flow only) --> browse at least 1 PDP, conversion rate
--Cleaning: Filter for only PDP views after registration

set experiment_start_date = '2022-01-13';
set experiment_end_date = '2022-01-21';

with treatment_and_control_group as (
--Originally from Goal Price Mode dash
    select distinct o.visitor_id as optimizely_visitor_id
        , case when variation_id = '21062230119' then 'Treatment: /overview mentions buying' 
               when variation_id = '21061800416' then 'Control: /overview no mention buying'
               else 'error' end as control_or_treatment
        , asi.initial_customer_id
        , si.created_at::date as si_created_at
    from optimizely.public.opendoor_decisions o
    inner join reception.seller_inputs si on si.experiment_entity_id = o.visitor_id
    left join dw.ax_seller_inputs asi on asi.seller_input_id = si.uuid
    where o.experiment_id = '21082930044'
        and si.created_at > $experiment_start_date
), 
--If need registration timestamp, data_mart_buyer.users is used by Buyer team
raw_pdp_views as ( --Modified from Buy Sell Hub mode report. app.mode.com/opendoor/reports/13fedeb13783/details/queries/733723777b13
    select u.opendoor_customer_id as customer_id,
           -- lv.ola_property_id as property_id,'mobile' as platform, 
           lv.timestamp
    from ios_production.listing_page_viewed lv
    join openlistings_ola.users u
        on u.id = lv.context_traits_ola_user_id

    union all
    --PDP views on android devices
    select u.opendoor_customer_id as customer_id,
           -- lv.ola_property_id as property_id,'mobile' as platform, 
           lv.timestamp
    from android_production.listing_page_viewed lv
    join openlistings_ola.users u
        on u.id = lv.context_traits_ola_user_id

    union all 
    --Older data table for website PDP views
    select u.opendoor_customer_id as customer_id,
           -- p.property_id, 'web' as platform, 
           timestamp
    from buying_experience_web_prod.pages p
    join buying_experience_web_prod.users bxu
        on bxu.id = p.user_id
    join openlistings_ola.users u
        on u.id = bxu.ola_user_id
    where p.name = 'Property Detail'
    
    union all 
    --Newer data table for website PDP views
    --Less than 1% are emails
    select substr(p.user_id, 10) as customer_id,
           -- p.property_id, 'web' as platform, p.user_id,
           timestamp
    from ola_js_prod.pages p
    where p.name = 'Property Detail'
        and p.user_id ilike '%customer%'
),
first_pdp_view as ( --first PDP view *after* experiment started
  select customer_id, min(timestamp) as first_pdp_view_at
  from (select * from raw_pdp_views where timestamp > $experiment_start_date
       )
  group by 1
),
combined_users as ( --Modified from Consumer Growth OKR dash app.mode.com/editor/opendoor/reports/e63b0b090e8d/queries/35730a535842
    select u.opendoor_customer_id
    , a.control_or_treatment
    , u.created_at::date as registered_at
    , a.si_created_at
    , pdp.first_pdp_view_at
    , datediff(day, registered_at, first_pdp_view_at) as days_between_reg_and_browse
    --Within conversion window, count in numerator of reg --> browse
    , case when days_between_reg_and_browse between 0 and 14 then 1 else 0 end as converted_indicator
    --this user table is more backfill resistant, new and improved
    from treatment_and_control_group a
    join data_mart_buyer.users u on u.opendoor_customer_id = a.initial_customer_id
    left join first_pdp_view pdp on pdp.customer_id = a.initial_customer_id::string
    WHERE True
      and u.channel = 'seller'
      and u.created_at > $experiment_start_date
      and registered_at > $experiment_start_date
),
agg_by_day as (
  select registered_at, control_or_treatment
         , count(distinct opendoor_customer_id) as num_custs_registered --denom
         , avg(converted_indicator)::number(5,3)  as reg_to_browse_conversion
  from combined_users
  group by 1, 2
),
agg_all_days as (
    select control_or_treatment
         , count(distinct opendoor_customer_id) as num_custs_registered --denom
         , sum(converted_indicator) as num_browsed_within_SLA --numerator
         , avg(converted_indicator)::number(5,4) as reg_to_browse_conversion
    from combined_users 
    group by 1
)
-- select * from agg_by_day 
select * from agg_all_days

;

/* Notes Graveyard */
--Angeline uses Google Analytics, may have list of webpages where buyers came from. May know if most sellers register after browsing PDPs
--However, it's simpler to use data_mart_buyer.users which has a channel column (seller, organic, visitor)

--Question for self: What's our most popular webpage? Onboarding address entry pages, or PDPs, or something else? I had assumed most users find us via AE pages
--Since PDP views are from ola.users table, these folks are already registered, no need to inner join on ax_customers to filter for registered users only
--Consider excluding seller's own PDP, using address_token
