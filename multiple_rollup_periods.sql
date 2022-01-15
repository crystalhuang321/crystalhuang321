--Taken from onboarding dashboard
--https://app.mode.com/editor/opendoor/reports/3391bf72c648/queries/41a23eb46bf1

with nontransposed_address_inputs as (
    select year(rp.date) as rp_date_year
       --, created_at::date as created_at_date
       , rp.date as rolled_up_date
       , rp.period_id
       , date_from_parts(2000, date_part(month, rolled_up_date), date_part(day,rolled_up_date)) as month_day_only --use for graphing
       , count(distinct seller_input_id) as num_address_inputs_raw
       , case when period_id = 'T7' then num_address_inputs_raw / 7 --same as taking the average later
              else num_address_inputs_raw end as num_address_inputs_corrected
    from dw.ax_seller_inputs
    join DWH.DW.AX_DIM_ROLLUP_PERIODS rp
        on created_at >= rp.begin_period
        and created_at < rp.end_period

    where rp.date >= '2018-01-01' --T7 is looking 6 days ago
        and rp.period_id in ('D1','T7')
    group by 1,2,3,4
    order by rp.date asc
)
-- select * from nontransposed_address_inputs order by rolled_up_date desc

select rp_date_year
      , rolled_up_date
      , month_day_only
      --the "sum" doesn't really sum up anything here, it's only for transposing, since the above subquery already has one row per day
      , sum(case when period_id = 'T7' then num_address_inputs_corrected else 0 end)::number(8,0) as num_address_inputs_corrected_T7 
      , sum(case when period_id = 'D1' then num_address_inputs_corrected else 0 end) as num_address_inputs_D1
from nontransposed_address_inputs
where rolled_up_date <= dateadd(week, -1, current_date)
group by 1,2,3
order by 2 desc
