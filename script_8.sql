
-- 2023 week 6 solution


with unpivoted_tbl as (
select customer_id,
'mobile' as type, 'ease_of_use' as reason, mbl_ease_of_use as 'value'
from `2023-w6`
union all
select customer_id,
'online' as type, 'ease_of_use' as reason, on_ease_of_use as 'value'
from `2023-w6`
union all
select customer_id,
'mobile' as type, 'ease_of_access' as reason, mbl_ease_of_access as 'value'
from `2023-w6`
union all
select customer_id,
'online' as type,  'ease_of_access' as reason, on_ease_of_access as 'value'
from `2023-w6`
union all
select customer_id,
'mobile' as type, 'navigation' as reason,  mbl_navigation as 'value'
from `2023-w6`
union all
select customer_id,
'online' as type, 'navigation' as reason, on_navigation as 'value'
from `2023-w6`
union all
select customer_id,
'mobile' as type, 'likelihood_to_recommend' as  reason, mbl_likelihood_to_recommend as 'value'
from `2023-w6`
union all
select customer_id,
'online' as type, 'likelihood_to_recommend' as  reason, on_likelihood_to_recommend as 'value'
from `2023-w6`
union all
select customer_id,
'mobile' as type, 'overall' as reason, mbl_overall as 'value'
from `2023-w6`
union all
select customer_id,
'online' as type, 'overall', on_overall as 'value'
from `2023-w6`
)
,prepared_data as (
select customer_id, reason
,sum(case when type = 'mobile' then value else null end) as mobile
,sum(case when type = 'online' then value else null end) as `online`
from unpivoted_tbl
group by customer_id, reason
)
,customer_platform_avg as (
select *
,avg(mobile) over(partition by customer_id) as mobile_avg
,avg(`online`) over(partition by customer_id) as online_avg
,abs(avg(mobile) over(partition by customer_id) - avg(`online`) over(partition by customer_id)) as diff
from prepared_data
where reason <> 'overall'
)
,cust_categorized as (
select distinct
customer_id, mobile_avg, online_avg, diff
,case when diff >= 2  and mobile_avg > online_avg then 'Mobile App Superfans'
	when diff >= 1 and mobile_avg > online_avg then 'Mobile App Fans'
    when diff  >= 2  and mobile_avg < online_avg then 'Online Interface Superfans'
	when diff  >= 1  and mobile_avg < online_avg then 'Online Interface Fans'
    when (diff  >= 0 and diff <= 1) then 'Neutral' end as category
from customer_platform_avg
)
select category
,round(count(category)*1.0 / (select count(customer_id) from cust_categorized)*100,1) as category_perc
from cust_categorized
group by category;