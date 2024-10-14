
-- 2024 Week 6 Solution



with cte as ( 
select *  
,row_number() over(partition by staff_id) as rn 
,count(staff_id) over(partition by staff_id) as total_cnt 
from w6_tbl 
) 
,latest_records as ( 
select staff_id
,round((`1` + `2` + `3` + `4` + `5` + `6` + `7` + `8` + `9` + `10` + `11` + `12`),0) as annual_sal 
from cte
where rn = total_cnt 
)
,tax_rates as (
select 0 as lower_bound, 12570 as upper_bound, 0 as tax_perc
union
select 12571 as lower_bound, 50270 as upper_bound, 20 as tax_perc
union
select 50271 as lower_bound, 125140 as upper_bound, 40 as tax_perc
union
select 125141 as lower_bound, null as upper_bound, 45 as tax_perc
)
,tbl as (
select lr.*, taxr.lower_bound, taxr.upper_bound, taxr.tax_perc
,case when (annual_sal - lower_bound) > 1 then 1 else 0 end as flg
,(annual_sal - lower_bound) as diff 
,row_number() over(partition by staff_id order by case when (annual_sal - lower_bound) > 1 then 1 else 0 end  desc, (annual_sal - lower_bound) asc) as rn
from latest_records lr
cross join tax_rates taxr
)
, staff_max_rate as (
select staff_id, annual_sal, diff, tax_perc as max_tax_rate
from tbl 
where rn = 1
)
,pre_final_tbl as (
select *
,case when max_tax_rate = tax_perc then round((annual_sal - lower_bound)*max_tax_rate*1.0/100,0)
else round((upper_bound - lower_bound)*tax_perc*1.0/100,0)
end as tax
from staff_max_rate
cross join tax_rates
)
,final_tbl as (
select 
staff_id
,annual_sal
,max_tax_rate
,sum(case when tax_perc = 20 then tax else null end) as `20% tax`
,sum(case when tax_perc = 40 then tax else null end) as `40% tax`
,sum(case when tax_perc = 45 then tax else null end) as `45% tax`
from pre_final_tbl
group by staff_id
,annual_sal
,max_tax_rate
)
select staff_id, annual_sal, max_tax_rate
,(`20% tax` + `40% tax` + coalesce(`45% tax`,0)) as total_tax_paid
,`20% tax`, `40% tax`, `45% tax`
from final_tbl;
