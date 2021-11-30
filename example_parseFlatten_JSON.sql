--Taken from https://app.mode.com/editor/opendoor/reports/8d9afffe807e/queries/58e1a1ffcc89

SELECT 
o.id as offer_id 
,o.created_at 
,o.in_review_at
,o.canceled_at  
,o.cancelation_reason
,o.cancelation_description
,o.price
,o.SUBMITTED_PRICE
,o.property_id 
,o.obo_property_flip_token
,s.value
,TO_TIMESTAMP_NTZ(s.value:created_at) as buyer_agent_history_created_at
,s.value:_id as buying_agent_history_id
,s.value:buyer_agent_id as buyer_agent_id
,s.value:canceled as canceled


FROM
    (SELECT * from  openlistings_ola.offers 
   where ID = '61983a0ad535cf70f8e072a2'  -- '5fe8edc9ec8d831377095984' 
    --where created_at >= current_date() - interval '7 days'
    --and lower(cancelation_reason) = 'other'
    order by created_at desc 
    ) o
    , Lateral FLATTEN (INPUT => parse_json(o.buyer_agent_history_snapshots), outer => true ) s
