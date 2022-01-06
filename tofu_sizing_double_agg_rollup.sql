-- When using count() or sum() in conjunction with the rollup table, your numbers will be multiplied and way too large
-- For example, when summing daily emails collected and joining on the rollup table using T7 as the period in the same subquery, 
-- your emails collected will the sum of the past seven days, and be about 7 times the daily number. Too large.

-- To avoid this, do the count() or sum() in an earlier subquery, then use avg() when joining on the rollup table in a later subquery
-- This way, you'll get an accurate T7 or T28 rolling average

-- Below code taken from: https://app.snowflake.com/us-east-1/opendoor/w39EFW2vzecS#query

--------------------------------------------------------------------------------------------------------
--Question 1 of 2: # of people who start the onboarding flow in BwOD markets from homepage and from /buy-and-sell
with get_seller_inputs as (
    select si.uuid as seller_input_id
       , si.created_at::date as created_at_date
       , '/buy-and-sell' as webpage
    from reception.seller_inputs si
    left join dwh.dw.ax_seller_inputs asi on si.uuid = asi.seller_input_id
    left join dwh.dw.ax_markets m on asi.market_identifier = m.identifier
    left join web_js_production.address_input_typing ait
        on ait.anonymous_id = si.experiment_entity_id
    where ait.value = 'buy_and_sell_hero_cta' --get addresses entered ONLY on the /buy-and-sell page
        and si.created_at >= '2021-01-01'
        and m.bwod_active is not null --address entered is in BwOD market

    union all

    select si.uuid as seller_input_id
       , si.created_at::date as created_at_date
       , 'homepage' as webpage
    from reception.seller_inputs si
    join dwh.dw.ax_seller_inputs asi on si.uuid = asi.seller_input_id
    join dwh.dw.ax_markets m on asi.market_identifier = m.identifier
    left join web_js_production.address_input_typing ait
        on ait.anonymous_id = si.experiment_entity_id
    where ait.value = 'new_homepage' --get addresses entered ONLY on homepage
        and si.created_at >= '2021-01-01'
        and m.bwod_active is not null --address entered is in BwOD market
),
count_AE_starts as (
    select created_at_date
       , count(distinct case when webpage in ('/buy-and-sell') then seller_input_id else null end) as num_buyandsell_starts
       , count(distinct case when webpage in ('homepage') then seller_input_id else null end) as num_homepage_starts
       --, count(distinct case when webpage in ('all AE pages') then seller_input_id else null end) as num_allAEpage_starts
       --, num_homepage_starts / num_allAEpage_starts as pct_starts_thatAre_homepage
       , num_homepage_starts + num_buyandsell_starts as num_HP_buysell_starts
    from get_seller_inputs
    group by 1 order by 1 desc
),
get_rolling_T7_average as (
    select rp.date as rolled_up_date --created_at_date
     , avg(num_buyandsell_starts)::number(6,0)
     , avg(num_homepage_starts)::number(6,0)
     , avg(num_HP_buysell_starts)::number(6,0)
    from count_AE_starts
    left join DW.AX_DIM_ROLLUP_PERIODS as rp
        on created_at_date >= rp.begin_period
        and created_at_date < rp.end_period
        and rp.period_id = 'T7'
    where rp.DATE < current_date
    group by 1
)
select * 
from get_rolling_t7_average
-- from count_ae_starts
order by rolled_up_date desc
-- order by created_at_date desc
;
