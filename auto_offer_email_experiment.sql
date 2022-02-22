--Reference google doc: https://docs.google.com/document/d/182EpsPYHPCtMCOmql_bywEGv8eklQLXmZktT9_5zXGw/edit#
--Primary metric: Unique user repeat offer rate (defined by ‘viewed_offer`)
--Secondary metric: ORVA scheduled, but instead use VA held. Can use engagement_events table

/*
select campaign_id::string
    -- , case when campaign_id in (2922616,2715137,2922618,2715136,2922617,2715135) then 'control'
    --      when campaign_id in (2985574,2985576,2985691,2985749,2985803,2985804) then 'test'
    --      end as treatement_group
    , count(*) as num_emails_sent
    , min(created_at)::date as earliest_email_sent
    -- , count(distinct _fivetran_id) as num_distinct_emails_sent
from fivetran.iterable.event
where campaign_id in (2922616,2715137,2922618,2715136,2922617,2715135,2985574,2985576,2985691,2985749,2985803,2985804)
    and event_name in ('emailSend')
    and created_at >= '2021-10-06' --when experiment started    
group by 1
;
--union all ------------------------------

select case when campaign_id in (2922616,2715137,2922618,2715136,2922617,2715135) then 'control campaigns'
            when campaign_id in (2985574,2985576,2985691,2985749,2985803,2985804) then 'test campaigns'
            end as campaign_id  
       , min(created_at)::date as earliest_email_sent
       , count(*) as num_emails_sent
from fivetran.iterable.event
where campaign_id in (2922616,2715137,2922618,2715136,2922617,2715135,2985574,2985576,2985691,2985749,2985803,2985804)
    and event_name in ('emailSend')
    --and created_at >= '2021-10-06' --when experiment started, but not 50/50, perhaps because experiment started on 2pm that day 
    and created_at >= '2021-10-07' --one day after experiment starts, shows 50/50
    --and created_at >= dateadd(week, -1, current_date) --emails from last week
group by 1

order by num_emails_sent
;

--Scratch work: How do I join seller_inputs with ax_leads? Hannah said seller_input_id
select count(asi.seller_input_id) as num_seller_input_ids
       , sum(case when l.id is not null then 1 else 0 end) as num_correct_joins
       , num_correct_joins / num_seller_input_ids as pct_correct_join
from dw.ax_seller_inputs asi
left join dw.ax_leads l --join on seller_input_uuid
    on l.seller_input_id = asi.seller_input_id --99% join success rate when using "qualified_at is not null"
where asi.qualified_at is not null
;
*/


--For calculating primary metric (Unique user repeat offer rate (defined by ‘viewed_offer`) and secondary metric, ORVA held
--Modified from Aleshia's query
--github.com/opendoor-labs/dwh/blob/master/scripts/axi_dag/sflk/data_mart_wbr.email_subscriber_base.sql

with get_flips_and_ORVAs as (
    --Grain: Seller_input_id
    select ax_si.email, ax_si.seller_input_id, ax_si.address_token
           , ax_si.created_at
           , count(distinct case when coalesce(va.orva_completed_at, va.self_serve_completed_at) is not null 
                                 then ax_si.seller_input_id else null end) as va_held_ind --de-duping. Force any seller_input_id, aka flip, to have at most one VA held
    
    from dw.ax_seller_inputs ax_si
    --Question: Don't use ax_seller_inputs.qualified_at, right? Because that timestamp isn't used for Seller Direct specifically
    join reception.product_offerings 
        on product_offerings.seller_input_uuid = ax_si.seller_input_id
        and product_offerings.product = 'OPENDOOR_DIRECT' --only get sellers who qualified for OPENDOOR_DIRECT, aka Sell Direct, the product we care about
    -- left join dw.ax_engagement_events ae
    --     on ae.address_token = ax_si.address_token and ae.event_type = 'iva_completed'
    --     and ae.source_event_at > ax_si.created_at --data cleaning and CYA: VA must have occurred after seller input started
    left join dw.ax_leads l --join on seller_input_uuid. Lead_id and seller_input_id is 1:1 as long as seller_input_id is qualified
        on l.seller_input_id = ax_si.seller_input_id
    left join web.virtual_assessments va --Katrina says to not use ax_engagement_events, this VA table is on the flip grain, which is better, not the address grain
        on va.flip_id = l.flip_id
    --Question: Don't need "channel = 'default'" because all customers will receive auto offer emails, regardless if they're an agent or working with homebuilder?
    
    where ax_si.email is not null
    group by 1,2,3,4
    --order by va_held_ind desc --ax_si.seller_input_id desc, created_at desc
)
, get_emailClick_offerView as (
    select es.created_at::date as email_sent_date
        , case when es.campaign_id in (2922616,2715137,2922618,2715136,2922617,2715135) then 'control campaigns'
               when es.campaign_id in (2985574,2985576,2985691,2985749,2985803,2985804) then 'test campaigns'
               end as treatment_group
        , count(distinct es.message_id) as emails_sent
        , count(distinct ec.message_id) as emails_clicked
        , (emails_clicked / emails_sent)::number(5,4) as email_CTR --proxy for offer view rate
        , count(distinct case when o.viewed_at is not null then flips.email end) as offers_viewed_dontUse --result is way too large because folks who didn't get an email can still view offers
        , sum(flips.va_held_ind) as num_ORVAs_held --Question: do I need to join on date or anything??
        , count(distinct flips.email) as num_distinct_emails
        , (num_ORVAs_held / num_distinct_emails) as VA_held_rate
        --To make VA held column more accurate, add timestamp filtering on iterable table join

    from get_flips_and_ORVAs flips
    
    left join fivetran.iterable.event es --email send. One to many join, one email address has many sent email events
        on flips.email = es.email
        and es.event_name = 'emailSend' --Question for Alex: At what point do we stop sending people emails? After they have VA? After sign contract?
        --and orva_held_at > email_sent_at
    left join fivetran.iterable.event ec
        on ec.message_id = es.message_id --one to many join
        and ec.event_name = 'emailClick'
    
    left join dw.ax_leads l --join on seller_input_uuid. Lead_id and seller_input_id is 1:1 as long as seller_input_id is qualified
        on l.seller_input_id = flips.seller_input_id
    left join dw.ax_offers o
        on o.lead_id = l.id
        --Question: Could join on offer view timestamp = email send timestamp
        --and o.indicator_initiated_by_od = True --Question: Since this whole experiment is about auto offer emails, would we ONLY want auto-generated offers. When uncomment this offers_viewed goes to ZERO
        --and o.viewed_at > flips.created_at --offer must have been viewed after seller_input_id created
        and o.viewed_at >= es.created_at --offer must have been viewed after email sent
        --Possible improvement to better tie offers to emails sent: find closest or most recent offer created to email sent time. Order of events should be: offer gen, email sent.
        --and o.viewed_at > es.created_at --offer must have been viewed after email sent
    -- left join data_mart_seller.cr_true_sellers ts on ts.lead_id = l.id
    
    where --campaign IDs for Auto offer email experiment, pt.2
        es.campaign_id in (2922616,2715137,2922618,2715136,2922617,2715135 
                          ,2985574,2985576,2985691,2985749,2985803,2985804)
        and es.created_at >= '2021-10-07' --one day after experiment starts, shows 50/50    
    
    group by email_sent_date, treatment_group
)
, agg_all_days as (
    select treatment_group
           , sum(emails_sent) as emails_sent_agg, sum(emails_clicked) as emails_clicked_agg
           , (emails_clicked_agg / emails_sent_agg)::number(5,4) as email_CTR--"Email CTR, proxy for offer view rate" --proxy for offer view rate
           , sum(num_orvas_held) as num_VAs_held_agg
           , sum(num_distinct_emails) as num_distinct_emails_agg
           , (num_VAs_held_agg / num_distinct_emails_agg)::number(5,4) as VA_held_rate 
    from get_emailClick_offerView
    group by 1
)
select * 
-- from get_emailClick_offerView --to get CI in databricks, cannot have aggregated data 
from agg_all_days
