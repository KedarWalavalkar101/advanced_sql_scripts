

-- 2023 week 29 solution



WITH RECURSIVE dates_cte AS (
    SELECT DATE('2020-01-01') AS first_date
    UNION ALL
    SELECT DATE_ADD(first_date, INTERVAL 1 MONTH)
    FROM dates_cte
    WHERE first_date < '2023-06-01'
)
,dates AS (
 SELECT DATE_FORMAT(first_date, '%Y-%m-%d') AS date
FROM dates_cte
)
,cte as (
select *
,str_to_date(date,'%d/%m/%Y') as dt
from `2023_w29`
)
,data_tbl as (
select 
concat(year(dt),'-',case when length(month(dt)) = 2 then month(dt) else concat(0,month(dt)) end,'-01') as trunc_dt
,store
,bike_type
,sum(sales) as total_sales
,sum(profit) as total_profit
,min(concat(year(dt),'-',case when length(month(dt)) = 2 then month(dt) else concat(0,month(dt)) end,'-01')) 
	over(partition by store, bike_type rows between unbounded preceding and unbounded following) as min_dt
,max(concat(year(dt),'-',case when length(month(dt)) = 2 then month(dt) else concat(0,month(dt)) end,'-01'))
	over(partition by store, bike_type rows between unbounded preceding and unbounded following) as max_dt
from cte
where dt >= '2020-01-01' and dt < '2023-07-01'
group by 
concat(year(dt),'-',case when length(month(dt)) = 2 then month(dt) else concat(0,month(dt)) end,'-01')
,store
,bike_type
)
,dt_tbl as (
SELECT a.date, b.store, b.bike_type, min_dt,max_dt
FROM dates a
cross join data_tbl b
where a.date >= min_dt 
)
,prefinal_tbl as (
select distinct t2.date, t2.store, t2.bike_type, coalesce(t1.total_sales,0) as sales, coalesce(t1.total_profit,0) as profit
from data_tbl t1
right join dt_tbl t2
on t1.trunc_dt = t2.`date` and t1.store = t2.store and t1.bike_type = t2.bike_type
)
,final_tbl as (
select *,
avg(profit) over(partition by store, bike_type order by date rows between 2 preceding and current row) as 3_month_moving_avg_profit
,lag(sales,2) over(partition by store, bike_type order by date) as flg
from prefinal_tbl
)
select date, store, bike_type, sales, profit,
case when flg is null then '' else `3_month_moving_avg_profit` end as `3_month_moving_avg_profit`
from final_tbl;