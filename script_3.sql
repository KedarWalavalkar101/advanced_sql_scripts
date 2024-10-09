with formatted_data as (
select 
date_format(cast(substring_index(flight_details,'/',1) as date),'%d/%m/%Y') as `date`
,substring_index(substring_index(flight_details,'/',3),'/',-1) as flight_no
,substring_index(substring_index(flight_details,'-',3),'/',-1) as `from`
,substring_index(substring_index(flight_details,'-',-1),'/',1) as `to`
,substring_index(substring_index(flight_details,'/',-3),'/',1) as class
,round(substring_index(flight_details,'/',-1),1) as price
,case when has_flow_card = 1 then 'Yes' else 'No' end as has_flow_card
,bags_checked
,meal_type
from w1_tbl
)
,flow_card_holders as (
select * from formatted_data
where has_flow_card = 'Yes'
)
,non_flow_card_holders as (
select * from formatted_data
where has_flow_card = 'No'
)
,combined_data as (
select *
from non_flow_card_holders
union all
select *
from flow_card_holders
)
, tbl_to_agg as (
select *
,concat('Q',quarter(str_to_date(`date`,'%d/%m/%Y'))) as dt_quarter 
,cast(price as signed) as price_col 
from combined_data
)
,min_max as (
select 
dt_quarter, class, has_flow_card
, min(price_col) as minval, max(price_col) as maxval
from tbl_to_agg
group by dt_quarter, class, has_flow_card
)
,tbl as (
select *
, case when rnasc > rndesc then rnasc - rndesc else rndesc- rnasc end as diff 
from ( 
select dt_quarter, class, has_flow_card, price_col
, row_number() over(partition by dt_quarter, class, has_flow_card order by price_col asc) as rnasc
, row_number() over(partition by dt_quarter, class, has_flow_card order by price_col desc) as rndesc
from tbl_to_agg
) x
)
,median_val as (
select dt_quarter, class, has_flow_card,
round(avg(price_col),0) as median_val
from tbl
where diff <= 1
group by dt_quarter, class, has_flow_card
)
,agg_tbl as (
select median.*, mm.minval, mm.maxval
from median_val median
join min_max mm on median.dt_quarter = mm.dt_quarter and median.class = mm.class and median.has_flow_card = mm.has_flow_card
)
, pre_final_tbl as (
select has_flow_card, dt_quarter, class
,'median_val' AS agg_val, median_val AS price 
from agg_tbl
union
select has_flow_card, dt_quarter, class
,'minval' AS agg_val, minval AS price 
from agg_tbl
union
select has_flow_card, dt_quarter, class
,'maxval' AS agg_val, maxval AS price 
from agg_tbl
)
,finaltbl as (
select has_flow_card, dt_quarter, agg_val
,sum(case when class = 'Business Class' then price else 0 end) as `business_class`
,sum(case when class = 'Economy' then price else 0 end) as 	`economy`
,sum(case when class = 'First Class' then price else 0 end) as `first_class`
,sum(case when class = 'Premium Economy' then price else 0 end) as `premium_economy`
from pre_final_tbl
group by has_flow_card, dt_quarter, agg_val
)
select has_flow_card
,has_flow_card, dt_quarter, agg_val, 
 `first_class` as `Economy`,
 `business_class` as `premium_economy`,
 `premium_economy` as `business_class`,
`economy` as `first_class`
from finaltbl;



